#!/bin/bash

# Wrapper for parallel-ssh (pssh) execution of maptrap across all live nodes
# in an instance.  This can be run from the TorMS in /srv/rocky.  pssh depends
# upon the manifest specifying a phraseless keypair as l4tm_pubkey, and the
# invoking ToRMS user having the matching private key in ~/.ssh/config

# Host node*
#	User l4tm
#	IdentityFile /srv/rocky/id_rsa.nophrase

###########################################################################
# Process command line and environment variables; set scalar globals.

THREADS=${THREADS:-CORES}	# a number, or ALL (=HT on) or CORES (=HT off)
TMHOSTS=${TMHOSTS:-}

TOPHINT=

DEVNULL='> /dev/null 2>&1'
if [ "$1" = "-v" ]; then
	QUIET=""
	shift
else
	QUIET="$DEVNULL"
fi

set -u

BIGFILE=/lfs/bigfile
EXEC=/srv/rocky/maptrap
PSSHOSTS=$HOME/hosts.pssh
shopt -s expand_aliases

if [ ! "$TMHOSTS" ]; then
  TMHOSTS=`jq -r '.racks[].enclosures[].nodes[].soc.hostname' < /etc/tmconfig | tr '\n' ' '`
fi
# echo $TMHOSTS

###########################################################################

function trace() {
    [ "$QUIET" ] || echo -e "\n------ $*\n"
}

DIE_GENERIC=1
DIE_SETUP=2
DIE_RUNTIME=3

function die() {
	MSG=$1
	[ $# -eq 2 ] && CODE=$2 || CODE=$DIE_RUNTIME
	echo "$MSG" >&2
	exit $CODE
}

###########################################################################
# Give values to globally-scoped arrays.

declare -ag HOSTNAMES NODE_IDS

function set_globals() {
    trace "Set globals"

    # Which of the nodes configured in /etc/tmconfig are actually running?
    HOSTNAMES=()
    for H in $TMHOSTS; do
	eval ping -c1 $H $DEVNULL
	[ $? -eq 0 ] && HOSTNAMES+=($H) || echo "No ping response from $H" >&2
    done
    [ ${#HOSTNAMES[*]} -eq 0 ] && die "All nodes fail ping" $DIE_SETUP
    # echo ${HOSTNAMES[*]}
    echo ${HOSTNAMES[*]} | tr ' ' '\n' > $PSSHOSTS

    # Assumes hostnames are of the form "nodeXX"
    NODE_IDS=()
    for H in ${HOSTNAMES[*]}; do
	NODE_IDS+=(${H:4:2})
    done
    echo "Pinged nodes: ${NODE_IDS[*]}"

    alias pssh="parallel-ssh -i -h $PSSHOSTS -l l4tm -p ${#NODE_IDS[*]}"

    # Can I hit them all?  If not, it's usually an ssh config problem
    # but may be a mismatch between /etc/tmconfig and reality.
    echo Testing parallel-ssh connectivity...
    eval pssh -t 20 echo $DEVNULL
    [ $? -ne 0 ] && die "pssh echo failed" $DIE_SETUP

    # Topology hint

    TPC=`ssh $HOSTNAMES lscpu | awk '/per core:/ {print $NF}'`
    [ $TPC -eq 4 ] && TOPHINT='-H4,4' || TOPHINT='-H1,1'

    return 0
}

###########################################################################
# Can't really run on nodes booted "nosmp".

function verify_boottype() {
	NOSMP=0
	for H in ${HOSTNAMES[*]}; do
		ssh $H grep -q nosmp /proc/cmdline
		let RET=1-$?
		let NOSMP+=$RET
		[ $RET -ne 0 ] && echo $H is running nosmp >&2
	done
	[ $NOSMP -gt 0 ] && die "Re-bind and reboot those nodes" $DIE_SETUP
	return 0
}

###########################################################################
# Synchronous kill and wait

function killwait() {
    eval pssh sudo killall -9 $1 $DEVNULL
    for H in ${HOSTNAMES[*]}; do
    	while ssh $H pgrep $1; do sleep 1; done
    done
}

###########################################################################
# Read the database directly, useful during error triage.

function jfdi() {
	DB=/var/hpetm/librarian.db
	trace "sqlite3 \"file:$DB?mode=ro\" \"$*\""
	sudo sqlite3 $DB "$*"
	return $?
}

###########################################################################
# If there are other files, this won't go to zero.  If "fake_zero" is not
# set in the Librarian, this may take a while to go to zero.

function remove_pmaptrap_files()
{
	# Kill the current file as it now has a private policy.
	eval pssh rm -f '/lfs/\`hostname\`' $DEVNULL
	[ $? -ne 0 ] && echo "pssh rm failed" >&2 && return 1

	ssh $HOSTNAMES rm -f $BIGFILE	# no subscript == first one

	NFILES=99
	let THEN=`date +%s`+30
	while [ $NFILES -gt 2 ]; do	# dot and dotdot
		echo "Waiting for files on /lfs to clear"
		sleep 3
		NFILES=`ssh $HOSTNAMES ls -a /lfs | wc -l`
		[ `date +%s` -gt $THEN ] && echo "/lfs clear timeout"
	done
	return 0
}

###########################################################################
# Set the default allocation policy on all nodes

function parallel_policy () {
	POLICY=$1

	eval pssh setfattr -n user.LFS.AllocationPolicyDefault -v $POLICY /lfs $DEVNULL
	[ $? -ne 0 ] && echo "pssh set default policy $POLICY failed" >&2 && return 1
	return 0
}

###########################################################################
# Create per-node files of a given size under a given allocation policy.
# Note that the escaping of the backquotes makes the command "hostname"
# execute on the node, not here on the ToRMS.

function allocate_pernode_files() {
	POLICY=$1
	SIZE=$2

	remove_pmaptrap_files

	parallel_policy $POLICY
	[ $? -ne 0 ] && return 1

	eval pssh truncate -s $SIZE '/lfs/\`hostname\`' $DEVNULL
	[ $? -ne 0 ] && echo "pssh truncate $SIZE failed" >&2 && return 1
	
	# Without subscript, uses the first one
	# ssh $HOSTNAMES getfattr -e hex -n user.LFS.Interleave /lfs/*

	return 0
}

###########################################################################
# Standard maptrap options are passed in.  If the final argument is not an
# existing file, calculate the filename.  See comments in 
# allocate_pernode_files() about the funky backslashes.  Failures are fatal.

function parallel_maptrap() {
	killwait maptrap
	# $? is multivalued, just ignore for now
	MSG="$1"
	PTHREADS=$2
	PLIMIT=$3
	shift 3
	declare -a TMP=($*)

	let TIMEOUT=$PLIMIT/10
	[ $TIMEOUT -lt 15 ] && TIMEOUT=15
	[ $TIMEOUT -gt 60 ] && TIMEOUT=60
	let TIMEOUT+=$PLIMIT
	ARGS="$TOPHINT -T $PTHREADS -L $PLIMIT ${TMP[*]}"

	# Is the last argument an optional file name?
	LASTARG=${TMP[-1]}
	ssh $HOSTNAMES ls $LASTARG $DEVNULL
	test $? -eq 0 -a ${LASTARG:0:1} = '/'
	if [ $? -eq 0 ]; then	# Yes it was
		LASTARG=
		MSG="$MSG ($ARGS)"
	else
		LASTARG="'/lfs/\`hostname\`'"
		MSG="$MSG ($ARGS /lfs/nodeXX)"
	fi
	trace "$MSG"
	[ "$QUIET" ] && echo -n "$MSG..."
	eval pssh -t $TIMEOUT $EXEC $ARGS $LASTARG
	[ $? -ne 0 ] && die "parallel_maptrap() FAILED" $DIE_RUNTIME
	[ "$QUIET" ] && echo "passed"
	return 0
}

###########################################################################
#  Have every file live on one node.  At 128G NVM DIMMs, max of 3 books.

function allocate_one_node_files() {
    let IG=$1-1
    SIZE=$2
    IG=`printf "0x%02X" $IG`
    remove_pmaptrap_files

    eval pssh touch '/lfs/\`hostname\`' $DEVNULL
    [ $? -ne 0 ] && echo "pssh touch failed" >&2 && return 1
        
    eval pssh setfattr -n user.LFS.AllocationPolicy -v RequestIG \
    	'/lfs/\`hostname\`' $DEVNULL
    [ $? -ne 0 ] && echo "pssh setfattr RequestIG failed" >&2 && return 1

    eval pssh setfattr -n user.LFS.InterleaveRequest -v $IG \
    	'/lfs/\`hostname\`' $DEVNULL
    [ $? -ne 0 ] && echo "pssh setfattr IG request $IG failed" >&2 && return 1

    eval pssh truncate -s $SIZE '/lfs/\`hostname\`' $QUIET
    [ $? -ne 0 ] && echo "pssh truncate $SIZE failed" >&2 && return 1

    return 0
}

###########################################################################
# Use the -h option of maptrap which uses fast random number generation 
# that only spans 0 thru 2^31, thus it only needs a 2G+ file.   It's still
# bigger than the CPU cache.

function hispeed() {
	POLICY=$1
	LIMIT=$2
	allocate_pernode_files $POLICY 2100M
	[ $? -ne 0 ] && die "File allocation failed" $DIE_RUNTIME

	# Shawn Walker asked for this, but it's not exercising FAM, just cache.
	# parallel_maptrap "HiSpeed fixed read $POLICY" $THREADS $LIMIT -h1

	parallel_maptrap "HiSpeed random read $POLICY" $THREADS $LIMIT -h8

	parallel_maptrap "HiSpeed random R-M-W $POLICY" $THREADS $LIMIT -h9
}

###########################################################################
# MAIN.  Begin with setup/cleanup.

set_globals

verify_boottype

killwait $EXEC
