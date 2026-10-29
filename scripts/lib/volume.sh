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
flip_byte() {
  docker run --rm --volumes-from "$(node_container "$1")" alpine:3 sh -c '
    f=/data/chunks/$1; off=$2
    before=$(od -An -tu1 -j "$off" -N1 "$f" | tr -d " ")
    printf "$(printf "\\%03o" $((before ^ 255)))" | dd of="$f" bs=1 seek="$off" count=1 conv=notrunc 2>/dev/null
    after=$(od -An -tu1 -j "$off" -N1 "$f" | tr -d " ")
    echo "byte at $off: $before -> $after"
  ' sh "$2" "$3"
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
