#!/bin/sh
# Runs the required demonstrations against a running cluster (start it with "make up").
#
#   scripts/demo.sh storage       upload and retrieve an object; compare checksums
#   scripts/demo.sh chunking      a multi-MiB object split into 1 MiB chunks spread over the workers
#   scripts/demo.sh replication   two copies of every chunk, on different workers
#   scripts/demo.sh metadata      custom metadata at upload; read and change it without re-uploading
#   scripts/demo.sh failure       docker stop/start worker2: reads, repair and replication status
#   scripts/demo.sh integrity     corrupt a stored chunk; it is detected, never served, and rewritten
#   scripts/demo.sh scrub         corrupt a chunk nobody reads; the background scrubber finds it
#   scripts/demo.sh all           everything except scrub, in order
#
# PAUSE=1 waits for Enter between steps. OBJCTL overrides the client binary.
set -eu
SELF=$(cd "$(dirname "$0")" && pwd)/$(basename "$0")
cd "$(dirname "$SELF")/.."

OBJCTL=${OBJCTL:-./bin/objctl}
if [ ! -x "$OBJCTL" ]; then
	go build -o bin/ ./cmd/objctl
fi
OBJCTL=$(cd "$(dirname "$OBJCTL")" && pwd)/$(basename "$OBJCTL")
# Work in a scratch directory so file names stay short in the output.
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
cd "$WORK"

bold() { printf '\n\033[1m%s\033[0m\n' "$*"; }
note() { printf '   %s\n' "$*"; }
run() {
	printf '\033[2m$ %s\033[0m\n' "$*"
	"$@"
}
# runs: run a shell pipeline given as one string
runs() {
	printf '\033[2m$ %s\033[0m\n' "$1"
	eval "$1"
}
pause() {
	if [ "${PAUSE:-0}" = 1 ]; then
		printf '\033[2m[enter]\033[0m'
		read -r _
	fi
}
objctl() { "$OBJCTL" "$@"; }
sha() {
	if command -v sha256sum >/dev/null; then sha256sum "$@"; else shasum -a 256 "$@"; fi
}
# random FILE MIB: a file of random bytes
random() { head -c "$(($2 * 1048576))" /dev/urandom >"$1"; }
# upload FILE [objctl put flags]: prints the new object ID
upload() {
	f=$1
	shift
	objctl put "$@" "$f" >put.out
	head -1 put.out | awk '{print $2}'
}
worker_state() { objctl workers | awk -v w="$1" '$1 == w {print $2}'; }
is_state() { [ "$(worker_state "$1")" = "$2" ]; }
is_corrupt() { objctl chunks "$1" | grep "^$2" | grep -q CORRUPT; }
maintained() { objctl status | head -1 | grep -q ': MAINTAINED$'; }
# wait_for SECONDS WHAT COMMAND...: polls COMMAND once a second
wait_for() {
	secs=$1 what=$2
	shift 2
	printf '   waiting for %s ' "$what"
	i=0
	while ! "$@" >/dev/null 2>&1; do
		i=$((i + 1))
		if [ "$i" -ge "$secs" ]; then
			echo "(gave up after ${secs}s)"
			return 1
		fi
		printf '.'
		sleep 1
	done
	echo "(${i}s)"
}

demo_storage() {
	bold "1. Object storage: upload, retrieve, compare"
	random "report.bin" 2
	id=$(upload "report.bin")
	cat put.out
	pause
	run objctl get -o "report.copy" "$id"
	run sha "report.bin" "report.copy"
	cmp -s "report.bin" "report.copy" && note "identical"
	pause
}

demo_chunking() {
	bold "2. Chunking and distribution: a 5.5 MiB object"
	head -c 5767168 /dev/urandom >"video.mp4"
	id=$(upload "video.mp4")
	cat put.out
	note "5 full 1 MiB chunks and a 0.5 MiB last chunk, in order, each with its own ID."
	pause
	for w in worker1 worker2 worker3 worker4; do
		printf '%s: ' "$w"
		objctl chunks "$w" | grep -c "^$id" || true
	done
	note "chunks of this object held by each worker"
	pause
}

demo_replication() {
	bold "3. Replication: two copies of each chunk on different workers"
	random "photos.zip" 4
	id=$(upload "photos.zip")
	run objctl info "$id"
	pause
	w=$(objctl info "$id" | awk '$1 == "0" {sub(",", "", $4); print $4}')
	note "chunk 0 as listed by $w itself:"
	runs "objctl chunks $w | grep ^${id}_000000"
	run objctl status
	pause
}

demo_metadata() {
	bold "4. Metadata: custom key-values, read and changed without re-uploading"
	random "notes.txt" 1
	id=$(upload "notes.txt" -meta owner=alice -meta project=cc-demo)
	run objctl meta "$id"
	pause
	sleep 1
	run objctl meta "$id" set status=reviewed owner=bob
	run objctl meta "$id" unset project
	run objctl info "$id"
	note "same object ID and chunks; only the metadata and modified time changed"
	pause
}

demo_failure() {
	bold "5. Worker failure: docker stop worker2, then docker start worker2"
	since=$(date -u +%Y-%m-%dT%H:%M:%SZ)
	random "dataset.bin" 12
	id=$(upload "dataset.bin")
	n=$(objctl info "$id" | awk '/_0000/ && /worker2/' | wc -l | tr -d ' ')
	note "uploaded $id: 12 chunks, $n with a copy on worker2"
	run objctl status
	pause

	bold "   stop worker2"
	run docker stop worker2
	wait_for 10 "worker2 to be suspect" is_state worker2 suspect
	run objctl workers
	run objctl status
	note "suspect: worker2's copies still count for a grace period; nothing is copied yet"
	run objctl get -o "dataset.copy" "$id"
	pause

	wait_for 30 "worker2 to be declared dead" is_state worker2 dead
	run objctl status
	wait_for 30 "repair to restore 2 copies" maintained
	run objctl status
	note "replication factor restored while worker2 is still down"
	run objctl get -o "dataset.copy" "$id"
	pause

	bold "   start worker2"
	run docker start worker2
	wait_for 10 "worker2 to come back (rejoining)" is_state worker2 suspect
	run objctl workers
	wait_for 30 "worker2 to be alive again" is_state worker2 alive
	note "the next repair pass sees 3 copies of some chunks and trims each back to 2"
	sleep 6
	run objctl status
	runs "docker logs --since $since controller 2>&1 | grep -E 'state changed|chunk copied|copy dropped'"
	run objctl get -o "dataset.copy" "$id"
	pause
}

demo_integrity() {
	bold "6. Data integrity: corrupt a stored chunk"
	since=$(date -u +%Y-%m-%dT%H:%M:%SZ)
	random "archive.tar" 3
	id=$(upload "archive.tar")
	chunk="${id}_000000"
	w=$(objctl info "$id" | awk '$1 == "0" {sub(",", "", $4); print $4}')
	note "chunk 0 is $chunk; corrupting the copy on $w"
	runs "docker run --rm -v objstore_${w}-data:/data alpine sh -c 'printf CORRUPTED | dd of=/data/chunks/$chunk bs=1 seek=5000 conv=notrunc 2>/dev/null'"
	pause
	run objctl get -o "archive.copy" "$id"
	run sha "archive.tar" "archive.copy"
	note "correct bytes: the worker refused its corrupt copy and the other copy was used"
	sleep 1
	runs "docker logs --since $since controller 2>&1 | grep $chunk"
	runs "objctl chunks $w | grep ^$chunk"
	note "the copy on $w was rewritten from the verified copy"
	pause
}

demo_scrub() {
	bold "Background scrubbing: corrupt a chunk that nobody reads"
	random "backup.bin" 1
	id=$(upload "backup.bin")
	chunk="${id}_000000"
	w=$(objctl info "$id" | awk '$1 == "0" {sub(",", "", $4); print $4}')
	runs "docker run --rm -v objstore_${w}-data:/data alpine sh -c 'printf CORRUPTED | dd of=/data/chunks/$chunk bs=1 seek=100 conv=notrunc 2>/dev/null'"
	note "workers re-verify every chunk each SCRUB_INTERVAL (1m by default)"
	wait_for 75 "the scrubber on $w to find it" is_corrupt "$w" "$chunk"
	runs "objctl chunks $w | grep ^$chunk"
	run objctl status
	wait_for 15 "repair to rewrite it" maintained
	runs "objctl chunks $w | grep ^$chunk"
	pause
}

case "${1:-all}" in
storage) demo_storage ;;
chunking) demo_chunking ;;
replication) demo_replication ;;
metadata) demo_metadata ;;
failure) demo_failure ;;
integrity) demo_integrity ;;
scrub) demo_scrub ;;
all)
	demo_storage
	demo_chunking
	demo_replication
	demo_metadata
	demo_failure
	demo_integrity
	;;
*)
	sed -n '2,13p' "$SELF" | sed 's/^# \{0,1\}//'
	exit 2
	;;
esac
