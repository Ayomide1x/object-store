# Shared by the acceptance scripts. Expects the array $compose, for example
# compose=(docker compose -f "$root/docker-compose.yml").
#
# The node image is distroless (no shell), so chunk files are reached from a
# throwaway alpine container that mounts the node's /data with --volumes-from.
# That works while the node is stopped or paused.

# node_container NODE: the container ID of a Compose node service.
node_container() { "${compose[@]}" ps -aq "$1"; }

# flip_byte NODE CHUNK_ID OFFSET: inverts one byte of a chunk file on NODE's
# volume and prints the byte before and after. Flipping twice restores it.
# Fails if the file is missing (dd would otherwise create one).
flip_byte() {
  docker run --rm --volumes-from "$(node_container "$1")" alpine:3 sh -c '
    f=/data/chunks/$1; off=$2
    if [ ! -f "$f" ]; then echo "MISSING $1: not flipped"; exit 1; fi
    before=$(od -An -tu1 -j "$off" -N1 "$f" | tr -d " ")
    printf "$(printf "\\%03o" $((before ^ 255)))" | dd of="$f" bs=1 seek="$off" count=1 conv=notrunc 2>/dev/null
    after=$(od -An -tu1 -j "$off" -N1 "$f" | tr -d " ")
    echo "byte at $off: $before -> $after"
  ' sh "$2" "$3"
}

# byte_at NODE CHUNK_ID OFFSET: the byte's value (0-255), or "missing".
byte_at() {
  docker run --rm --volumes-from "$(node_container "$1")" alpine:3 sh -c '
    f=/data/chunks/$1
    if [ -f "$f" ]; then od -An -tu1 -j "$2" -N1 "$f" | tr -d " "; else echo missing; fi
  ' sh "$2" "$3"
}

# corrupt_byte NODE CHUNK_ID OFFSET LOG: flips the byte (prints it before and
# after) and appends "NODE CHUNK_ID OFFSET FLIPPED_VALUE" to LOG, for
# restore_flipped and check_touched.
corrupt_byte() {
  local out
  out="$(flip_byte "$1" "$2" "$3")" || { echo "$out"; return 1; }
  echo "$out"
  echo "$1 $2 $3 ${out##*-> }" >> "$4"
}

# unflip NODE CHUNK_ID OFFSET FLIPPED_VALUE: flips the byte back only if it
# still holds the value the flip wrote. Since stage 8, GC may delete a BAD
# replica's file and repair write a good copy under the same name. Every copy
# of a chunk has the same bytes, and a flip XORs with 255, so a good copy never
# holds the flipped value. The inode can't tell the two files apart: the
# filesystem reuses a freed inode number for the next file it creates. Prints
# what it did; always succeeds.
unflip() {
  local now
  now="$(byte_at "$1" "$2" "$3")"
  if [[ "$now" == "$4" ]]; then
    flip_byte "$1" "$2" "$3" || true
  elif [[ "$now" == missing ]]; then
    echo "not flipped back: the file is gone (deleted by GC)"
  else
    echo "not flipped back: byte is $now, not the flipped $4 (restored already, or rewritten by repair)"
  fi
}

# restore_flipped LOG: unflips every byte recorded in LOG, printing one line
# per byte. Safe to run more than once.
restore_flipped() {
  local node chunk off val
  while read -r node chunk off val; do
    [[ -n "$node" ]] && echo "  $node $chunk: $(unflip "$node" "$chunk" "$off" "$val")"
  done <"$1"
}

# check_touched LOG: run after restore_flipped. Each file recorded in LOG must
# be gone, or, if Postgres has a HEALTHY row for it, pass the header, chunk-ID
# and SHA-256 checks. Prints one line per file and returns 1 if any fails.
check_touched() {
  local node chunk rc=0 file row
  while read -r node chunk; do
    # </dev/null: compose exec forwards stdin (-T only drops the TTY), and
    # would otherwise swallow the rest of the list this loop reads.
    file="$(check_chunk_files "$node" "$chunk" </dev/null | cut -d' ' -f1)"
    row="$("${compose[@]}" exec -T postgres psql -U objectstore -d objectstore -tA \
      -c "SELECT state FROM chunk_replicas WHERE chunk_id = '$chunk' AND node_id = '$node'" </dev/null)"
    if [[ "$file" == MISSING ]]; then
      echo "  ok: touched file $node $chunk: gone (row: ${row:-none})"
    elif [[ "$row" != HEALTHY ]]; then
      echo "  ok: touched file $node $chunk: $file on disk, row ${row:-none} (not served)"
    elif [[ "$file" == OK ]]; then
      echo "  ok: touched file $node $chunk: HEALTHY row, file verifies"
    else
      echo "  FAIL: touched file $node $chunk: HEALTHY row, but the file fails verification"
      rc=1
    fi
  done < <(cut -d' ' -f1,2 "$1" | sort -u)
  return "$rc"
}

# delete_chunk_file NODE CHUNK_ID: removes a chunk file from NODE's volume.
delete_chunk_file() {
  docker run --rm --volumes-from "$(node_container "$1")" alpine:3 rm "/data/chunks/$2"
}

# check_chunk_files NODE CHUNK_ID...: for each chunk, prints "OK id" if NODE's
# file passes the header (magic), chunk-ID and SHA-256 checks, else "BAD id" or
# "MISSING id". Works while the node is stopped.
check_chunk_files() {
  local node="$1"; shift
  docker run --rm --volumes-from "$(node_container "$node")" alpine:3 sh -c '
    for id in "$@"; do
      f=/data/chunks/$id
      if [ ! -f "$f" ]; then echo "MISSING $id"; continue; fi
      magic=$(head -c 8 "$f")
      hid=$(dd if="$f" bs=1 skip=8 count=16 2>/dev/null | od -An -tx1 -v | tr -d " \n")
      hsum=$(dd if="$f" bs=1 skip=24 count=32 2>/dev/null | od -An -tx1 -v | tr -d " \n")
      dsum=$(tail -c +57 "$f" | sha256sum | cut -d" " -f1)
      if [ "$magic" = OSCHUNK1 ] && [ "$hid" = "$(echo "$id" | tr -d -)" ] && [ "$hsum" = "$dsum" ]; then
        echo "OK $id"
      else
        echo "BAD $id"
      fi
    done' sh "$@"
}
