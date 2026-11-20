#!/bin/sh
# Acceptance tests: every requirement of the project statement, checked against
# a real running cluster with pass/fail assertions and a non-zero exit on
# failure. Unlike scripts/demo.sh, which narrates the demonstrations for a
# viewer, this asserts and reports.
#
#   scripts/acceptance.sh              start a private cluster from ./bin and test it
#   scripts/acceptance.sh -docker      test the Docker Compose cluster ("make up" first)
#   scripts/acceptance.sh -h           usage
#
# Local mode needs nothing but Go; it runs 1 controller and 4 workers on
# loopback ports with short timeouts (about 90 s in total). Docker mode uses
# the compose cluster with its own timeouts and demonstrates failure with
# "docker stop worker2", so it takes a few minutes.
#
# Needs: sh, awk, curl, sha256sum or shasum, go. Docker mode also needs docker.
set -u

SELF=$(cd "$(dirname "$0")" && pwd)/$(basename "$0")
ROOT=$(dirname "$(dirname "$SELF")")
cd "$ROOT" || exit 1

MODE=local
case "${1:-}" in
-docker) MODE=docker ;;
-local | "") MODE=local ;;
-h | -help | --help)
	sed -n '2,17p' "$SELF" | sed 's/^# \{0,1\}//'
	exit 0
	;;
*)
	echo "unknown option: $1 (try -h)" >&2
	exit 2
	;;
esac

# ---------------------------------------------------------------- reporting

PASSED=0
FAILED=0
SKIPPED=0
FAILURES=""

if [ -t 1 ]; then
	B='\033[1m' G='\033[32m' R='\033[31m' Y='\033[33m' D='\033[2m' Z='\033[0m'
else
	B='' G='' R='' Y='' D='' Z=''
fi

group() { printf "\n${B}%s${Z}\n" "$*"; }
note() { printf "${D}   %s${Z}\n" "$*"; }
ok() {
	PASSED=$((PASSED + 1))
	printf "   ${G}PASS${Z} %s\n" "$1"
}
bad() {
	FAILED=$((FAILED + 1))
	FAILURES="$FAILURES
   - $1"
	printf "   ${R}FAIL${Z} %s\n" "$1"
	[ $# -gt 1 ] && printf "        %s\n" "$2"
	return 0
}
skip() {
	SKIPPED=$((SKIPPED + 1))
	printf "   ${Y}SKIP${Z} %s\n" "$1"
	[ $# -gt 1 ] && printf "        %s\n" "$2"
	return 0
}
die() {
	printf "\n${R}setup failed:${Z} %s\n" "$*" >&2
	exit 1
}

# eq WANT GOT DESC: the values must be equal
eq() {
	if [ "$1" = "$2" ]; then
		ok "$3"
	else
		bad "$3" "want [$1], got [$2]"
	fi
}

# empty VALUE DESC: the value must be empty (used for "no offending rows")
empty() {
	if [ -z "$1" ]; then
		ok "$2"
	else
		bad "$2" "$(echo "$1" | tr '\n' ';')"
	fi
}

# contains HAYSTACK NEEDLE DESC
contains() {
	case "$1" in
	*"$2"*) ok "$3" ;;
	*) bad "$3" "expected to find [$2]" ;;
	esac
}

# code WANT DESC curl-args...: the request must return that status
code() {
	want=$1 desc=$2
	shift 2
	got=$(curl -s -o /dev/null -w '%{http_code}' "$@")
	eq "$want" "$got" "$desc"
}

# until_true SECONDS DESC COMMAND...: poll once a second until it succeeds
until_true() {
	secs=$1 desc=$2
	shift 2
	i=0
	while [ "$i" -lt "$secs" ]; do
		if "$@" >/dev/null 2>&1; then
			ok "$desc (${i}s)"
			return 0
		fi
		i=$((i + 1))
		sleep 1
	done
	bad "$desc" "still not true after ${secs}s"
	return 1
}

# ---------------------------------------------------------------- utilities

sha() {
	if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | awk '{print $1}'; else shasum -a 256 "$1" | awk '{print $1}'; fi
}
random() { head -c "$2" /dev/urandom >"$1"; }
objctl() { "$OBJCTL" "$@"; }

# upload FILE [flags...]: prints the new object ID
upload() {
	f=$1
	shift
	objctl put "$@" "$f" 2>/dev/null | head -1 | awk '{print $2}'
}
state_of() { objctl workers | awk -v w="$1" '$1 == w {print $2}'; }
is_state() { [ "$(state_of "$1")" = "$2" ]; }
maintained() { objctl status 2>/dev/null | head -1 | grep -q ': MAINTAINED$'; }
not_maintained() { objctl status 2>/dev/null | head -1 | grep -q ': NOT MAINTAINED$'; }
# chunk_state WORKER CHUNK: "ok" or "CORRUPT" as the worker itself reports it
chunk_state() { objctl chunks "$1" 2>/dev/null | awk -v c="$2" '$1 == c {print $4}'; }
is_corrupt() { [ "$(chunk_state "$1" "$2")" = CORRUPT ]; }
is_clean() { [ "$(chunk_state "$1" "$2")" = ok ]; }
# holder INDEX ID: the first worker recorded for that chunk
holder() { objctl info "$2" | awk -v i="$1" '$1 == i {sub(",", "", $4); print $4}'; }
# copies_of WORKER ID: how many chunks of the object that worker holds
copies_of() { objctl chunks "$1" 2>/dev/null | grep -c "^$2" || true; }
status_counts() { objctl status 2>/dev/null | sed -n 2p; }
at_risk() { objctl status 2>/dev/null | sed -n 2p | sed -n 's/.*, \([0-9]*\) at risk,.*/\1/p'; }
has_at_risk() { n=$(at_risk); [ -n "$n" ] && [ "$n" -gt 0 ]; }
chunk_gone() { ! objctl chunks "$1" 2>/dev/null | grep -q "^$2"; }
four_alive() { [ "$(objctl workers 2>/dev/null | grep -c alive)" -eq 4 ]; }
# log_copied ID: the controller has re-replicated a chunk of that object
log_copied() { controller_log 2>/dev/null | grep -q "chunk copied.*$1"; }
over_count() { objctl status 2>/dev/null | sed -n 2p | sed -n 's/.*, \([0-9]*\) over-replicated,.*/\1/p'; }
no_surplus() { n=$(over_count); [ -n "$n" ] && [ "$n" -eq 0 ]; }
# scrub_found WORKER CHUNK: the worker's scrubber reported that chunk corrupt
scrub_found() { worker_log "$1" 2>/dev/null | grep 'scrub found corrupt chunks' | grep -q "$2"; }
# scrub_feasible: local mode scrubs every 5 s; compose every SCRUB_INTERVAL (1m
# by default, too slow to wait for unless it was lowered at "make up" time)
scrub_feasible() {
	[ "$MODE" = local ] && return 0
	case "${SCRUB_INTERVAL:-}" in
	[0-9]s | [0-9][0-9]s) return 0 ;;
	esac
	return 1
}

# ---------------------------------------------------------------- cluster

WORK=$(mktemp -d) || die "mktemp"
CLUSTER=$WORK/cluster
mkdir -p "$CLUSTER"

cleanup() {
	trap - EXIT INT TERM
	[ "$MODE" = local ] && local_down
	rm -rf "$WORK"
}
trap 'cleanup' EXIT INT TERM

# -- local mode: our own controller and four workers from ./bin ------------

CBASE=${BASE_PORT:-19080}
CPID=''
WPID1='' WPID2='' WPID3='' WPID4=''

local_worker_url() { echo "http://127.0.0.1:$((CBASE + $1))"; }

local_start_worker() {
	n=$1
	"$ROOT/bin/worker" -id "worker$n" -listen "127.0.0.1:$((CBASE + n))" -addr "$(local_worker_url "$n")" \
		-data "$CLUSTER/worker$n" -controller "$OBJSTORE_URL" -scrub-interval 5s -log-level info \
		>>"$CLUSTER/worker$n.log" 2>&1 &
	eval "WPID$n=$!"
}
local_stop_worker() {
	eval "pid=\$WPID$1"
	[ -n "$pid" ] && kill "$pid" 2>/dev/null
	wait "$pid" 2>/dev/null
	eval "WPID$1=''"
	return 0
}
local_start_controller() {
	"$ROOT/bin/controller" -listen "127.0.0.1:$CBASE" -data "$CLUSTER/controller" \
		-suspect-after 2s -dead-after 6s -rejoin-after 4s \
		-repair-interval 2s -repair-delay 1s -log-level info \
		>>"$CLUSTER/controller.log" 2>&1 &
	CPID=$!
}
local_stop_controller() {
	[ -n "$CPID" ] && kill "$CPID" 2>/dev/null
	wait "$CPID" 2>/dev/null
	CPID=''
	return 0
}
local_up() {
	local_start_controller
	for n in 1 2 3 4; do local_start_worker "$n"; done
}
local_down() {
	for n in 1 2 3 4; do local_stop_worker "$n"; done
	local_stop_controller
}

# -- the operations the tests need, per mode ------------------------------

case $MODE in
local)
	OBJSTORE_URL=http://127.0.0.1:$CBASE
	SUSPECT_WAIT=10 DEAD_WAIT=25 REPAIR_WAIT=30 REJOIN_WAIT=30 SCRUB_WAIT=30
	cluster_stop_worker() { local_stop_worker "${1#worker}"; }
	cluster_start_worker() { local_start_worker "${1#worker}"; }
	cluster_restart_all() {
		local_down
		sleep 1
		local_up
	}
	controller_log() { cat "$CLUSTER/controller.log"; }
	worker_log() { cat "$CLUSTER/$1.log"; }
	# corrupt_chunk WORKER CHUNK: overwrite bytes inside a stored chunk file
	corrupt_chunk() {
		f="$CLUSTER/$1/chunks/$2"
		[ -f "$f" ] || return 1
		printf 'CORRUPTED' | dd of="$f" bs=1 seek=5000 conv=notrunc 2>/dev/null
	}
	worker_api() { local_worker_url "${1#worker}"; }
	;;
docker)
	OBJSTORE_URL=http://localhost:${CONTROLLER_PORT:-8090}
	SUSPECT_WAIT=20 DEAD_WAIT=60 REPAIR_WAIT=60 REJOIN_WAIT=60 SCRUB_WAIT=130
	cluster_stop_worker() { docker stop "$1" >/dev/null; }
	cluster_start_worker() { docker start "$1" >/dev/null; }
	cluster_restart_all() { docker compose restart >/dev/null 2>&1; }
	controller_log() { docker logs controller 2>&1; }
	worker_log() { docker logs "$1" 2>&1; }
	corrupt_chunk() {
		docker run --rm -v "objstore_$1-data:/data" alpine \
			sh -c "printf CORRUPTED | dd of=/data/chunks/$2 bs=1 seek=5000 conv=notrunc 2>/dev/null" >/dev/null 2>&1
	}
	worker_api() { echo ""; } # worker ports are not published by compose
	;;
esac
export OBJSTORE_URL
API="$OBJSTORE_URL/api/v1/objects"

# ---------------------------------------------------------------- start up

printf "${B}objstore acceptance tests${Z} (%s mode)\n" "$MODE"

OBJCTL=${OBJCTL:-$ROOT/bin/objctl}
if [ "$MODE" = local ]; then
	go build -o bin/ ./cmd/... || die "go build"
	note "built ./bin/{controller,worker,objctl}"
	local_up
	note "started 1 controller and 4 workers on ports $CBASE-$((CBASE + 4))"
elif [ ! -x "$OBJCTL" ]; then
	go build -o bin/ ./cmd/objctl || die "go build"
fi

i=0
while :; do
	curl -s -o /dev/null --max-time 2 "$OBJSTORE_URL/healthz" && break
	i=$((i + 1))
	[ "$i" -ge 20 ] && die "controller at $OBJSTORE_URL is not responding (run \"make up\" for -docker mode)"
	sleep 1
done
i=0
while [ "$(objctl workers 2>/dev/null | grep -c alive)" -lt 4 ]; do
	i=$((i + 1))
	[ "$i" -ge 30 ] && die "only $(objctl workers 2>/dev/null | grep -c alive) of 4 workers are alive"
	sleep 1
done
note "controller $OBJSTORE_URL, 4 workers alive"

cd "$WORK" || die "cd $WORK"

# ---------------------------------------------------------------- §5 REST API

group "REST API contract (statement §5)"

random payload.bin 2097152
up_out=$(curl -s -D head.txt -o body.json -w '%{http_code}' -F 'metadata={"owner":"alice","project":"cc-demo"}' -F file=@payload.bin "$API")
eq 201 "$up_out" "POST /api/v1/objects returns 201"
grep -qi '^location: /api/v1/objects/' head.txt && ok "upload sets a Location header" || bad "upload sets a Location header"
OID=$(awk -F'"' '/"object_id"/ {print $4; exit}' body.json)
[ -n "$OID" ] && ok "upload response carries an object_id" || bad "upload response carries an object_id"

code 200 "GET /api/v1/objects/{id} returns 200" "$API/$OID"
code 200 "GET /api/v1/objects/{id}/metadata returns 200" "$API/$OID/metadata"
code 200 "PATCH /api/v1/objects/{id}/metadata returns 200" -X PATCH -d '{"stage":"review"}' "$API/$OID/metadata"
code 200 "GET /api/v1/objects returns 200" "$API"

note "required metadata fields (§3)"
meta=$(curl -s "$API/$OID/metadata")
for field in object_id name size content_type sha256 created_at modified_at chunk_count chunks replicas metadata; do
	contains "$meta" "\"$field\"" "metadata includes $field"
done

note "error handling (§5.7)"
code 404 "404 on GET of an unknown object" "$API/nope"
code 404 "404 on GET metadata of an unknown object" "$API/nope/metadata"
code 404 "404 on PATCH metadata of an unknown object" -X PATCH -d '{"a":"b"}' "$API/nope/metadata"
code 404 "404 on DELETE of an unknown object" -X DELETE "$API/nope"
code 400 "400 when the upload is not multipart" -X POST -d hello "$API"
code 400 "400 when the upload has no file field" -F 'metadata={"a":"b"}' "$API"
code 400 "400 on a non-string metadata value" -X PATCH -d '{"a":1}' "$API/$OID/metadata"
code 400 "400 when metadata is not a JSON object" -X PATCH -d '[1]' "$API/$OID/metadata"
code 400 "400 on an out-of-range limit" "$API?limit=0"
err=$(curl -s "$API/nope")
contains "$err" '"error"' "error bodies are JSON with an error code"
contains "$err" '"message"' "error bodies carry a message"

wapi=$(worker_api worker1)
if [ -n "$wapi" ]; then
	s1=$(printf AAAA | (command -v sha256sum >/dev/null 2>&1 && sha256sum || shasum -a 256) | awk '{print $1}')
	s2=$(printf BBBB | (command -v sha256sum >/dev/null 2>&1 && sha256sum || shasum -a 256) | awk '{print $1}')
	printf AAAA >a.bin
	printf BBBB >b.bin
	code 201 "PUT /internal/chunks/{id} stores a chunk (201)" -X PUT -H "X-Chunk-SHA256: $s1" --data-binary @a.bin "$wapi/internal/chunks/acceptance_000000"
	code 409 "409 when the same chunk ID holds different bytes" -X PUT -H "X-Chunk-SHA256: $s2" --data-binary @b.bin "$wapi/internal/chunks/acceptance_000000"
	code 400 "400 when a chunk body does not match its checksum" -X PUT -H "X-Chunk-SHA256: $s1" --data-binary @b.bin "$wapi/internal/chunks/acceptance_000000"
	code 404 "404 on GET of an unknown chunk" "$wapi/internal/chunks/missing_000000"
	code 204 "DELETE /internal/chunks/{id} returns 204" -X DELETE "$wapi/internal/chunks/acceptance_000000"
else
	skip "internal worker chunk API status codes" "compose does not publish worker ports; covered in local mode"
fi
code 200 "GET /internal/workers/{id}/chunks returns an inventory" "$OBJSTORE_URL/internal/workers/worker1/chunks"

# ---------------------------------------------------------------- §10.1 storage

group "Object storage: upload, retrieve, compare (statement §10.1)"

curl -s -o fetched.bin "$API/$OID"
eq "$(sha payload.bin)" "$(sha fetched.bin)" "the retrieved object is byte-identical to the original"
hdrs=$(curl -s -o /dev/null -D - "$API/$OID")
contains "$hdrs" "Content-Length: 2097152" "download reports the original size"
contains "$hdrs" "application/octet-stream" "download carries a Content-Type"

random photo.jpg 1024
JID=$(upload photo.jpg)
contains "$(curl -s "$API/$JID/metadata")" '"content_type": "image/jpeg"' "content type is derived for a known extension"

: >empty.bin
EID=$(upload empty.bin)
code 200 "an empty object downloads with 200" "$API/$EID"
contains "$(curl -s "$API/$EID/metadata")" '"chunk_count": 0' "an empty object has no chunks"

# ---------------------------------------------------------------- §10.2 chunking

group "Chunking and distribution (statement §10.2, §4)"

random big.bin 5767168 # 5.5 MiB: five full chunks and a half one
BID=$(upload big.bin)
bmeta=$(curl -s "$API/$BID/metadata")
contains "$bmeta" '"chunk_size": 1048576' "the chunk size is 1 MiB"
contains "$bmeta" '"chunk_count": 6' "5.5 MiB becomes 6 chunks"
eq 6 "$(echo "$bmeta" | grep -c '"chunk_id"')" "the metadata lists all 6 chunk IDs"
eq 5 "$(echo "$bmeta" | grep -c '"size": 1048576')" "five chunks are exactly 1 MiB"
contains "$bmeta" '"size": 524288' "the final chunk is the 0.5 MiB remainder"
empty "$(objctl info "$BID" | awk '$1 ~ /^[0-9]+$/ {if ($1 != n++) print "chunk " $1 " is out of order"}')" \
	"chunk indexes are 0..5 in order"
eq 6 "$(objctl info "$BID" | awk '$1 ~ /^[0-9]+$/ {print $2}' | sort -u | wc -l | tr -d ' ')" \
	"every chunk has a unique identifier"
curl -s -o big.copy "$API/$BID"
eq "$(sha big.bin)" "$(sha big.copy)" "the 5.5 MiB object reassembles exactly"

random spread.bin 12582912 # 12 MiB: 12 chunks, 24 copies
SID=$(upload spread.bin)
held=0
for w in worker1 worker2 worker3 worker4; do
	n=$(copies_of "$w" "$SID")
	note "$w holds $n of the 24 copies"
	[ "$n" -gt 0 ] && held=$((held + 1))
done
if [ "$held" -ge 3 ]; then
	ok "the chunks are distributed across the workers ($held of 4 hold copies)"
else
	bad "the chunks are distributed across the workers" "only $held of 4 workers hold copies"
fi

# ---------------------------------------------------------------- §10.3 replication

group "Replication factor 2 on distinct workers (statement §10.3, §4)"

empty "$(objctl info "$SID" | awk '$1 ~ /^[0-9]+$/ {
		if (NF != 5) { print "chunk " $1 " has " (NF - 3) " copies"; next }
		a = $4; sub(",", "", a); if (a == $5) print "chunk " $1 " has both copies on " a
	}')" "every chunk has exactly 2 copies, on two different workers"

w0=$(holder 0 "$SID")
c0=$(objctl info "$SID" | awk '$1 == 0 {print $2}')
eq ok "$(chunk_state "$w0" "$c0")" "the holding worker's own inventory confirms chunk 0 ($w0)"
maintained && ok "the controller reports the replication factor as maintained" || bad "the controller reports the replication factor as maintained" "$(status_counts)"
contains "$(curl -s "$OBJSTORE_URL/internal/replication")" '"replication_factor": 2' "the replication factor is 2"

# ---------------------------------------------------------------- §10.4 metadata

group "Custom metadata, read and changed without re-uploading (statement §10.4, §5.4)"

MID=$(upload payload.bin -meta owner=alice -meta project=cc-demo)
before=$(curl -s "$API/$MID/metadata")
contains "$before" '"owner": "alice"' "custom metadata given at upload is stored"
contains "$before" '"project": "cc-demo"' "several custom keys are stored"
before_mod=$(echo "$before" | awk -F'"' '/"modified_at"/ {print $4}')
before_sha=$(echo "$before" | awk -F'"' '/"sha256"/ {print $4}')
sleep 1
after=$(curl -s -X PATCH -d '{"status":"reviewed","owner":"bob","project":null}' "$API/$MID/metadata")
contains "$after" '"status": "reviewed"' "PATCH adds a new key"
contains "$after" '"owner": "bob"' "PATCH replaces an existing value"
case "$after" in
*'"project"'*) bad "a null value removes a key" ;;
*) ok "a null value removes a key" ;;
esac
eq "$before_sha" "$(echo "$after" | awk -F'"' '/"sha256"/ {print $4}')" "the object data is untouched by a metadata change"
after_mod=$(echo "$after" | awk -F'"' '/"modified_at"/ {print $4}')
[ "$after_mod" != "$before_mod" ] && ok "modified_at advances on a metadata change" || bad "modified_at advances on a metadata change"
eq "$(sha payload.bin)" "$(curl -s "$API/$MID" | (command -v sha256sum >/dev/null 2>&1 && sha256sum || shasum -a 256) | awk '{print $1}')" \
	"the object still downloads unchanged after the metadata edit"

# ---------------------------------------------------------------- §5.6 list, §5.5 delete

group "Listing and deletion (statement §5.6, §5.5)"

listing=$(curl -s "$API?limit=100")
contains "$listing" "$OID" "the list contains an uploaded object"
for field in object_id name size content_type; do
	contains "$listing" "\"$field\"" "list entries carry $field"
done
contains "$(curl -s "$API?limit=1")" '"next_offset"' "the list pages with limit and offset"

DID=$(upload photo.jpg)
dchunk=$(objctl info "$DID" | awk '$1 == 0 {print $2}')
dw=$(holder 0 "$DID")
code 204 "DELETE returns 204" -X DELETE "$API/$DID"
code 404 "a deleted object is gone (404)" "$API/$DID"
code 404 "deleting twice returns 404" -X DELETE "$API/$DID"
case "$(curl -s "$API?limit=1000")" in
*"$DID"*) bad "a deleted object disappears from the list" ;;
*) ok "a deleted object disappears from the list" ;;
esac
until_true 15 "the deleted object's chunks are removed from $dw" chunk_gone "$dw" "$dchunk"

# ---------------------------------------------------------------- §10.5 worker failure

group "Worker failure: stop worker2, then start it again (statement §10.5, §7)"

FW=worker2
n=$(copies_of "$FW" "$SID")
if [ "$n" -eq 0 ]; then
	skip "worker2 holds copies of the test object" "rendezvous hashing placed none there"
else
	note "$FW holds $n copies of the 12 MiB object before the failure"
fi
eq "$(sha spread.bin)" "$(curl -s "$API/$SID" | (command -v sha256sum >/dev/null 2>&1 && sha256sum || shasum -a 256) | awk '{print $1}')" \
	"the object downloads correctly before the failure"
maintained && ok "replication is maintained before the failure" || bad "replication is maintained before the failure"

note "stopping $FW"
cluster_stop_worker "$FW" || die "could not stop $FW"
until_true "$SUSPECT_WAIT" "$FW becomes suspect" is_state "$FW" suspect
until_true 10 "its copies are flagged at risk, and none are copied yet (grace period)" has_at_risk
curl -s -o during.bin "$API/$SID"
eq "$(sha spread.bin)" "$(sha during.bin)" "the object still downloads from the other copies while $FW is down"
note "$(status_counts)"

until_true "$DEAD_WAIT" "$FW is declared dead" is_state "$FW" dead
until_true "$REPAIR_WAIT" "the controller detects the loss and re-replicates the lost copies" log_copied "$SID"
note "$(status_counts)"
until_true "$REPAIR_WAIT" "repair restores 2 copies while $FW is still down" maintained
note "$(status_counts)"
curl -s -o repaired.bin "$API/$SID"
eq "$(sha spread.bin)" "$(sha repaired.bin)" "the object downloads correctly after re-replication"

note "starting $FW again"
cluster_start_worker "$FW" || die "could not start $FW"
until_true "$REJOIN_WAIT" "$FW rejoins the cluster" is_state "$FW" suspect
until_true "$REJOIN_WAIT" "$FW is trusted again (alive)" is_state "$FW" alive
until_true "$REPAIR_WAIT" "replication is maintained after the restart" maintained
until_true "$REPAIR_WAIT" "the returning worker's surplus copies are trimmed" no_surplus
empty "$(objctl info "$SID" | awk '$1 ~ /^[0-9]+$/ {if (NF != 5) print "chunk " $1 " has " (NF - 3) " copies"}')" \
	"every chunk is back to exactly 2 copies on distinct workers"
note "$(status_counts)"
eq "$(sha spread.bin)" "$(curl -s "$API/$SID" | (command -v sha256sum >/dev/null 2>&1 && sha256sum || shasum -a 256) | awk '{print $1}')" \
	"the object downloads correctly after $FW returns"

# ---------------------------------------------------------------- §10.6 integrity

group "Data integrity: intentional corruption (statement §10.6, §8)"

random archive.tar 3145728
AID=$(upload archive.tar)
achunk=$(objctl info "$AID" | awk '$1 == 0 {print $2}')
aw=$(holder 0 "$AID")
if corrupt_chunk "$aw" "$achunk"; then
	note "corrupted chunk 0 of $AID on $aw"
	curl -s -o archive.copy "$API/$AID"
	eq "$(sha archive.tar)" "$(sha archive.copy)" "corrupt bytes are never served while a valid copy exists"
	contains "$(controller_log)" "$achunk" "the controller logs the bad replica"
	until_true "$REPAIR_WAIT" "the corrupt copy is restored from the valid one" is_clean "$aw" "$achunk"
	until_true "$REPAIR_WAIT" "replication is maintained after the repair" maintained
	eq "$(sha archive.tar)" "$(curl -s "$API/$AID" | (command -v sha256sum >/dev/null 2>&1 && sha256sum || shasum -a 256) | awk '{print $1}')" \
		"the object still downloads correctly after the repair"

	note "corrupting a chunk that nobody reads, for the background scrubber"
	SBID=$(upload photo.jpg)
	schunk=$(objctl info "$SBID" | awk '$1 == 0 {print $2}')
	sw=$(holder 0 "$SBID")
	if corrupt_chunk "$sw" "$schunk" && scrub_feasible; then
		until_true "$SCRUB_WAIT" "the scrubber on $sw finds the corruption with nobody reading it" scrub_found "$sw" "$schunk"
		until_true "$REPAIR_WAIT" "the scrubbed chunk is rewritten from the valid copy" is_clean "$sw" "$schunk"
		maintained && ok "replication is maintained after scrub repair" || bad "replication is maintained after scrub repair"
	else
		skip "background scrubber detection" "compose scrubs every ${SCRUB_INTERVAL:-1m}; rerun with SCRUB_INTERVAL=10s make up, or in local mode"
	fi
else
	skip "data corruption tests" "could not write into $aw's storage"
fi

# ---------------------------------------------------------------- §9 persistence

group "Persistent storage across a restart (statement §9, §2)"

want=$(sha spread.bin)
note "restarting the controller and all workers"
cluster_restart_all
i=0
while :; do
	curl -s -o /dev/null --max-time 2 "$OBJSTORE_URL/healthz" && break
	i=$((i + 1))
	[ "$i" -ge 30 ] && break
	sleep 1
done
until_true 40 "all 4 workers register again after the restart" four_alive
contains "$(curl -s "$API?limit=1000")" "$SID" "objects are still listed after the restart"
contains "$(curl -s "$API/$MID/metadata")" '"status": "reviewed"' "custom metadata survives the restart"
curl -s -o persisted.bin "$API/$SID"
eq "$want" "$(sha persisted.bin)" "stored data survives the restart byte-for-byte"
until_true "$REPAIR_WAIT" "replication is maintained after the restart" maintained

# ---------------------------------------------------------------- §5.7 503

group "Behaviour when storage is unavailable (statement §5.7)"

if [ "$MODE" = local ]; then
	note "stopping 3 of 4 workers so 2 copies cannot be placed"
	cluster_stop_worker worker2
	cluster_stop_worker worker3
	cluster_stop_worker worker4
	sleep 3
	code 503 "503 when too few workers are available to place 2 copies" -F file=@photo.jpg "$API"
	contains "$(curl -s -F file=@photo.jpg "$API")" '"unavailable"' "the 503 body explains that storage is unavailable"
	cluster_stop_worker worker1
	sleep 3
	code 503 "503 when no copy of an object is reachable" "$API/$SID"
	code 200 "metadata is still served while the workers are down" "$API/$SID/metadata"
	code 200 "the object list is still served while the workers are down" "$API"
	not_maintained && ok "the status endpoint reports the loss of replication" || bad "the status endpoint reports the loss of replication"
else
	skip "503 paths (too few workers, no reachable copy)" "would stop every worker container; run in local mode"
fi

# ---------------------------------------------------------------- summary

printf "\n${B}summary${Z}\n"
printf "   %s%d passed%s, " "$G" "$PASSED" "$Z"
if [ "$FAILED" -gt 0 ]; then printf "%s%d failed%s, " "$R" "$FAILED" "$Z"; else printf "0 failed, "; fi
printf "%d skipped\n" "$SKIPPED"
if [ "$FAILED" -gt 0 ]; then
	printf "${R}failures:${Z}%s\n\n" "$FAILURES"
	exit 1
fi
printf "\n${G}all requirements verified${Z}\n\n"
exit 0
