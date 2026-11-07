# Deploying to one VM behind Caddy

This guide puts the object store at `https://objectstore.ayomideo.dev` on a single ARM VM (Ubuntu 24.04, arm64, 4 cores, 23 GiB) that already runs other Compose projects and a Caddy container serving them over HTTPS.

**Layout:**
- **Caddy** runs in its own Compose project with host networking. It terminates HTTPS, asks for a password (basic auth, bcrypt at cost 10, the hash in Caddy's environment), and proxies to the two gateways on `127.0.0.1:9081` and `127.0.0.1:9082`. The site block is [`deploy/objectstore.caddy`](deploy/objectstore.caddy).
- **The object store** runs from this repo as the Compose project `objectstore`, with [`docker-compose.prod.yml`](docker-compose.prod.yml) layered on the tested `docker-compose.yml`, which is unchanged. The VM's `.env` selects it ([`.env.prod.example`](.env.prod.example)). It sets:
  - only the gateways' ports published, on 127.0.0.1; Postgres, the coordinator and the nodes are reachable only on the project's own network;
  - a required Postgres password;
  - GC at the code defaults (15 min read grace, 24 h pending timeout);
  - `restart: unless-stopped` and rotated logs.
- **Backups:** [`scripts/backup-db.sh`](scripts/backup-db.sh) takes a daily `pg_dump` of the metadata, kept 14 days. It covers the metadata only: the chunk files and the backups share the VM's one disk.

**Checks:**
- **[`scripts/caddy-check.sh`](scripts/caddy-check.sh)** verifies, locally, with the VM's Caddy version and that exact block, that Caddy keeps the gateway's guarantees:
  - `%2F` stays encoded;
  - uploads stream without buffering or a timeout;
  - a broken download fails at the client over HTTP/1.1, HTTP/2 and HTTP/3;
  - Host and Origin pass through;
  - a wrong password costs under 100 ms.
- **[`scripts/deploy-smoke.sh`](scripts/deploy-smoke.sh)** checks the live deployment from your own machine.

The reasons for each choice are in DECISIONS.md, in the entries marked "(deployment)". Deploying somewhere other than this VM? See [Deploying anywhere else](#deploying-anywhere-else).

The commands under "On the VM" run over SSH; those under "On your Mac" run in this repo on your machine. Nothing here runs tests, Testcontainers or mutation scripts on the VM.

## 0. Before you start (on your Mac)

1. **Make the password,** and store it in your password manager as the `objectstore` login for `https://objectstore.ayomideo.dev`:
   ```sh
   openssl rand -base64 48 | tr -d '/+=\n' | head -c 32; echo
   ```
   You'll paste it into one command on the VM (step 4). It's never stored there, only its hash.
2. **If the VM's Caddy isn't v2.11.7,** rerun the local check against its version first. Edit `CADDY_IMAGE` at the top of `scripts/caddy-check.sh`, then:
   ```sh
   export OBJECTSTORE_DB_PASSWORD=local-check
   docker compose down -v
   docker compose -p objectstore -f docker-compose.yml -f docker-compose.prod.yml up -d --build --wait
   scripts/caddy-check.sh                  # ends: PASS
   docker compose -p objectstore -f docker-compose.yml -f docker-compose.prod.yml down -v
   ```
   v2.11.7 is the version it last passed with. What it checks is in the script's header.

## 1. Preflight (on the VM)

Find Caddy's container and its project directory; the rest of this guide calls them `$CADDY` and `$CADDY_DIR`:
```sh
docker ps --format '{{.Names}}\t{{.Image}}' | grep -i caddy
CADDY=<name from above>
CADDY_DIR="$(docker inspect -f '{{index .Config.Labels "com.docker.compose.project.working_dir"}}' "$CADDY")"; echo "$CADDY_DIR"
```

Check each of these; the expected result is in the comment:
```sh
uname -m                                  # aarch64
docker compose version                    # v2.24.4 or later (for !reset); v5.6.0 is fine
docker exec "$CADDY" caddy version        # v2.11.7 (else see step 0.2)
docker inspect -f '{{.HostConfig.NetworkMode}}' "$CADDY"   # host
dig +short objectstore.ayomideo.dev       # 129.146.48.152
free -h                                   # "available" well over 5 GiB: the stack's limits total about 4.25 GiB
df -h /var/lib/docker                     # 10 GiB or more free: images about 3 GiB, plus your data

# The two ports the deployment publishes must be free. Both print nothing:
sudo ss -ltnp | grep -E ':(9081|9082)\b'
docker ps --format '{{.Names}}\t{{.Ports}}' | grep -E ':(9081|9082)->'

# The names it will use must be free. Both print nothing:
docker compose ls --all | grep -w objectstore
docker volume ls --format '{{.Name}}' | grep '^objectstore_'
```

The other ports the stack uses (9090, 9101–9104, 5432) are not published in production, so they can't clash with the other projects. To see who holds them anyway:
```sh
sudo ss -ltnp | grep -E ':(9090|910[1-4]|5432)\b'
```

If anything above isn't as expected, stop there.

## 2. Clone and configure (on the VM)

```sh
git clone https://github.com/Ayomide1x/object-store.git ~/object-store
cd ~/object-store
cp .env.prod.example .env
chmod 600 .env
sed -i "s/^OBJECTSTORE_DB_PASSWORD=.*/OBJECTSTORE_DB_PASSWORD=$(openssl rand -base64 48 | tr -d '/+=\n' | head -c 40)/" .env
grep -c '^OBJECTSTORE_DB_PASSWORD=.\{40\}$' .env        # 1
```

**About `.env`:**
- It selects the production override for every `docker compose` command run in `~/object-store`, and names the project `objectstore`.
- The database password in it takes effect when the `objectstore_pgdata` volume is first created. Changing it later also needs `ALTER USER` inside Postgres.
- If you lose it, `docker compose exec postgres psql -U objectstore` still works: inside the container, the local socket needs no password.

**Check what Compose will run.** Only the gateways may publish a port, and only on 127.0.0.1:
```sh
docker compose config --format json | python3 -c '
import json, sys
for name, svc in json.load(sys.stdin)["services"].items():
    print(name, [(p.get("host_ip"), p.get("published")) for p in svc.get("ports", [])], svc.get("restart"))'
```
Expected: `gateway1 [('127.0.0.1', '9081')] unless-stopped`, `gateway2 [('127.0.0.1', '9082')] unless-stopped`, and `[] unless-stopped` for the other six.

## 3. Build and start (on the VM)

```sh
cd ~/object-store
docker compose build                       # native arm64; the first build takes several minutes
docker compose up -d --wait                # returns when all 8 services are healthy
docker compose ps --format '{{.Service}}\t{{.Status}}\t{{.Ports}}'
```

Expected: 8 lines, each `Up ... (healthy)`. Only `gateway1` and `gateway2` show a host port (`127.0.0.1:9081->9081/tcp`, `127.0.0.1:9082->9082/tcp`).

**Check from the VM, without Caddy.** The gateway answers only to its public name, so send it as Host:
```sh
curl -s -H 'Host: objectstore.ayomideo.dev' http://127.0.0.1:9081/cluster \
  | python3 -c 'import json,sys; print([(n["node_id"], n["liveness"]) for n in json.load(sys.stdin)["nodes"]])'
# [('node1', 'ALIVE'), ('node2', 'ALIVE'), ('node3', 'ALIVE'), ('node4', 'ALIVE')]
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:9081/healthz     # 403: Host 127.0.0.1:9081 isn't served
```

## 4. Caddy (on the VM)

Adding environment variables to Caddy means recreating its container. Your other two sites are down for a few seconds then.

**4.1 The password hash.** Bcrypt at cost 10, made from the password on stdin, so it never appears in a command line or your shell history. Paste the password at the silent prompt:
```sh
read -rs -p 'password: ' PW; echo
printf '%s' "$PW" | docker run --rm -i httpd:2.4.69 htpasswd -niBC 10 objectstore | head -1 | cut -d: -f2-
unset PW
```
It prints one line starting with `$2y$10$`. Cost 10, not `caddy hash-password`'s 14, keeps each wrong-password guess at about 48 ms of CPU instead of about 0.73 s. The password's length is what makes guessing hopeless (DECISIONS.md).

**4.2 Into Caddy's environment.** Add both lines to `$CADDY_DIR/.env`, with the hash in single quotes. Without them, Compose would read each `$` in the hash as a variable.
```sh
cp "$CADDY_DIR/.env" "$CADDY_DIR/.env.bak-$(date +%F)" 2>/dev/null || true
cat >>"$CADDY_DIR/.env" <<'EOF'
OBJECTSTORE_USER=objectstore
OBJECTSTORE_PASSWORD_HASH='<the $2y$10$... line from 4.1>'
EOF
chmod 600 "$CADDY_DIR/.env"
```
Then pass them to the Caddy service in `$CADDY_DIR`'s compose file, under `environment:`:
```yaml
      OBJECTSTORE_USER: ${OBJECTSTORE_USER:?}
      OBJECTSTORE_PASSWORD_HASH: ${OBJECTSTORE_PASSWORD_HASH:?}
```
Check that the hash comes through intact. Both lines must match exactly:
```sh
cd "$CADDY_DIR" && docker compose config | grep OBJECTSTORE_PASSWORD_HASH
grep OBJECTSTORE_PASSWORD_HASH .env
```

**4.3 The site block.** Append [`deploy/objectstore.caddy`](deploy/objectstore.caddy) to the Caddyfile your Caddy loads, unchanged. It is the block `scripts/caddy-check.sh` verified. Find the Caddyfile on the host:
```sh
docker inspect -f '{{range .Mounts}}{{.Source}} -> {{.Destination}}{{"\n"}}{{end}}' "$CADDY"
```
Then append the block to the host file that's mounted at `/etc/caddy/Caddyfile`, or into the directory mounted at `/etc/caddy`:
```sh
CADDYFILE=<host path of /etc/caddy/Caddyfile>
cp "$CADDYFILE" "$CADDYFILE.bak-$(date +%F)"
{ echo; cat ~/object-store/deploy/objectstore.caddy; } >>"$CADDYFILE"
```
Don't add `encode` to this block: compression would hide an aborted download's short body.

**4.4 Recreate Caddy and check.**
```sh
cd "$CADDY_DIR"
docker compose up -d                                                   # recreates Caddy with the new environment
docker exec "$CADDY" caddy validate --config /etc/caddy/Caddyfile --adapter caddyfile   # Valid configuration
docker exec "$CADDY" printenv OBJECTSTORE_PASSWORD_HASH | cut -c1-7    # $2y$10$
docker logs "$CADDY" --since 5m 2>&1 | grep -i 'objectstore.ayomideo.dev' | grep -iE 'certificate obtained|error'
```
Expect "certificate obtained successfully" for `objectstore.ayomideo.dev` within a minute. Your other sites should answer as before:
```sh
curl -s -o /dev/null -w '%{http_code}\n' https://queue.ayomideo.dev/
curl -s -o /dev/null -w '%{http_code}\n' https://api.ayomideo.dev/
```

If Caddy doesn't come back, restore both `.bak` files and run `docker compose up -d` in `$CADDY_DIR` again.

## 5. Smoke check (on your Mac)

```sh
curl -sI https://objectstore.ayomideo.dev/ | head -3     # HTTP/2 401, and a www-authenticate: Basic header
scripts/deploy-smoke.sh https://objectstore.ayomideo.dev ubuntu@129.146.48.152
```

It asks for the user (`objectstore`) and the password, and checks:
1. 401 without the password and with a wrong one.
2. `/cluster` shows 4 nodes ALIVE.
3. A 9 MiB round trip over HTTP/2 under a key with "/" in it: stat, prefix listing, and an identical download.
4. A broken download through Caddy. Over SSH, it corrupts every replica of chunk 1 of a 12 MiB object. The download must fail at curl over HTTP/2, with a short body that matches the start of the object.
5. The same in your browser. It prints a second broken object's URL: open it, watch the download fail, and answer `y`.

It ends with `PASS`. It removes what it uploaded (GC deletes the files 15 minutes later) and flips the corrupted bytes back.

Then open <https://objectstore.ayomideo.dev/>, log in, and check the UI:
- 4 Alive node cards;
- an upload by drag and drop;
- that object's chunk grid;
- a download.

## 6. Backups (on the VM)

Take one by hand first and check it:
```sh
cd ~/object-store
scripts/backup-db.sh                        # wrote /home/ubuntu/backups/objectstore/objectstore-<time>.dump (... bytes)
ls -l ~/backups/objectstore/
```

Then schedule it daily at 03:15 (VM time), keeping 14 days. `crontab -e`, and add:
```
15 3 * * * cd $HOME/object-store && scripts/backup-db.sh >>$HOME/backups/objectstore.log 2>&1
```
The next day, `tail ~/backups/objectstore.log` should show a "wrote" line.

**Restoring** a dump replaces the metadata with the dump's. Objects written since it was taken are lost. A chunk deleted by GC since then fails its reads loudly; it never returns wrong data.
```sh
cd ~/object-store
docker compose stop coordinator gateway1 gateway2
docker compose exec -T postgres pg_restore -U objectstore -d objectstore --clean --if-exists \
  <~/backups/objectstore/objectstore-<time>.dump
docker compose up -d --wait
```

**What the backup doesn't cover:**
- the chunk files;
- the dumps themselves, which sit on the same disk as everything else.

Losing the VM's disk loses everything. If the data starts to matter, copy the dumps off the VM and look at Oracle's volume backups.

## 7. Running it

**Status and logs:**
```sh
cd ~/object-store
docker compose ps
docker compose logs --since 10m coordinator
docker compose logs -f gateway1
```
Logs rotate at 5 × 10 MB per service.

**Stop and start.** Data is kept in the volumes:
```sh
docker compose stop
docker compose up -d --wait
```
Never run `docker compose down -v` here. `-v` deletes the volumes, which hold every chunk and all the metadata.

**Upgrade** to a newer commit:
```sh
cd ~/object-store
git fetch && git log --oneline HEAD..origin/main     # what's coming
git pull --ff-only
docker compose build
docker compose up -d --wait
```
Then rerun the smoke check from your Mac. The coordinator applies new Flyway migrations at startup. Take a backup first (`scripts/backup-db.sh`), since a migration can't be rolled back by checking out an older commit.

**Roll back:**
```sh
git checkout <previous commit>
docker compose build
docker compose up -d --wait
```
That only works if no migration ran in between; otherwise restore the backup taken before the upgrade.

**Upgrading Caddy:** run `scripts/caddy-check.sh` locally with the new version first (step 0.2).

**Uptime monitoring:** `/healthz` is behind the password like everything else. If you add an external monitor, give it the credentials, or add `handle /healthz { reverse_proxy 127.0.0.1:9081 }` above `basic_auth` in the site block. That exposes only "ok".

## What this deployment doesn't protect against

- **Losing the VM's disk.** All four nodes, Postgres and the backups share it, so three replicas protect against a node failing, not the disk.
- **A flood of password guesses.** Each costs about 48 ms of CPU, so a sustained flood can still slow the VM and your other sites. Real rate limiting needs a custom Caddy build, shared with your other sites. If Caddy's logs show guessing, that's the next step, or fail2ban on Caddy's 401s.
- **The coordinator being down.** It's a single point of failure by design: uploads and downloads stop, and the nodes keep their data.
- **Anyone with the password.** There's one login, and it can read, upload and delete everything.

## Deploying anywhere else

What this guide relies on that another setup must provide too:
- **Memory:** every service has a hard limit, about 4.25 GiB in total, derived from worst-case bounds and checked by saturating each cap (see DECISIONS.md, stages 9 and 10).
- **No authentication in the services:** keep every port on loopback and put the gateways behind a proxy that authenticates. The proxy must pass `%2F` in paths through undecoded, must not buffer bodies or impose a whole-request timeout, must abort a response the gateway aborts, and must pass Host on unchanged (or add the name it sends to `GATEWAY_ALLOWED_HOSTS`). `scripts/caddy-check.sh` shows how to check each of these.
- **Watching costs:** each open UI tab polls every 2 s. The coordinator caches the expensive counts for `COORDINATOR_STATUS_CACHE_TTL` (1 s), so the scan behind them runs at most once a second however many tabs watch; it still grows with the number of chunks.
- **Keepalive pings on the network path:** every long-lived gRPC connection (coordinator to nodes, nodes' heartbeats, gateways to the coordinator) sends an HTTP/2 PING after 10 s without traffic. Anything between the services, such as a NAT, firewall or load balancer, must pass those pings and have an idle timeout over 10 s, and a proxy that terminates gRPC must permit a ping every 10 s. A half-open connection is then found within 12 s (13 s for a heartbeat connection, which a node normally replaces after one timed-out beat, about 4 s). The values are code constants that must match on both sides (DECISIONS.md, stage 11).
