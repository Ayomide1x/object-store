# The HTTP gateway

The gateway's HTTP API: endpoints, how keys are encoded, status codes and the gateway's bounds. Start the stack first ([quick start](../README.md#quick-start)). Configuration is environment variables, documented in [`docker-compose.yml`](../docker-compose.yml).

Two identical, stateless gateways (ports 9081 and 9082) serve the same objects: what one stores, the other serves, and restarting either loses nothing.

```sh
curl -T ./photo.jpg   http://127.0.0.1:9081/objects/photos%2F1.jpg        # PUT
curl -o copy.jpg      http://127.0.0.1:9082/objects/photos%2F1.jpg        # GET, from the other gateway
curl                  http://127.0.0.1:9081/objects/photos%2F1.jpg/stat   # chunks and replicas
curl                 'http://127.0.0.1:9081/objects?prefix=photos%2F&limit=100'
curl -X DELETE        http://127.0.0.1:9081/objects/photos%2F1.jpg
curl                  http://127.0.0.1:9081/cluster                       # liveness, repair, GC
```

- **A key is one path segment.** Encode `/` in a key as `%2F` (`encodeURIComponent`), and a key that is exactly `.` or `..` as `%2E`. A raw slash gets 400 with that rule. This keeps `/objects/{key}/stat` unambiguous.
- **PUT needs `Content-Length`** (411 without one, chunked included) and is capped at 5 GiB (413). Both are checked before the body is sent. The body is streamed: the gateway holds one 4 MiB chunk per upload, and the client hanging up or stalling for 30 s means nothing commits.
- **GET streams verified chunks only.** The status line goes out with the first verified chunk, so a failure before that is a proper status. A failure after it aborts the connection: the client sees a short body and a broken transfer (`curl: (18)`), never a complete-looking response.
- **Listing is paged by key:** `start_after` and `limit` (default and maximum 1000) in, `next_start_after` out, empty on the last page.
- **Status codes:** 400 bad request, 404 no such key, 503 with `Retry-After` when the cluster or the gateway is busy or short of nodes, 500 when a chunk has no healthy replica, 502 or 504 when upstream fails. Errors are JSON, `{"error": ..., "code": ...}`.
- **Bounds:** 8 uploads and 16 downloads at once per gateway (more get 503 at once), a 384 MiB container limit derived from those caps, and no whole-request timeout. A transfer is cut off only if it stops moving for `GATEWAY_STALL_TIMEOUT`.
