/*
 Copyright 2017 Hewlett Packard Enterprise Development LP

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License, version 2 as
 published by the Free Software Foundation.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License along
 with this program.  If not, write to the Free Software Foundation, Inc.,
 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
*/

// mmap() exerciser with looping, increments, single-stepping and more.

// gcc -o maptrap -O2 -Wall -Werror -pthread maptrap.c
// to compile and see intermixed assembly:
// http://www.systutorials.com/240/generate-a-mixed-source-and-assembly-listing-using-gcc/
// gcc -o maptrap -Wall -Werror -pthread -g -Wa,-adhln maptrap.c > maptrap.s
// To see thread bindings, "ps -mo pid,tid,fname,user,psr -p `pgrep maptrap`"

// Origin: exerciser for MMS PoC development in 2013
// Rocky Craig rocky.craig@hpe.com
// Simplest distillation of strace() from mongodb startup that crashes
// story C.  Real Mongo uses O_RDWR | O_NOATIME on open and PROT_WRITE
// in mmap, but their absence doesn't affect the vm_insert_pfn BUG check.

#define _GNU_SOURCE	// O_NOATIME

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <stdbool.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <stdarg.h>
#include <pthread.h>
#include <signal.h>
#include <locale.h>

#include <sys/time.h>

#define _GNU_SOURCE	// man 3 CPU_SET
#include <sched.h>

#ifndef PAGE_SIZE
#define PAGE_SIZE 4096
#endif

#define READIT	0x01	// cuz O_RDONLY == 0x0 and I need bit OR'ing
#define WRITEIT	0x02

#define die(...) { fprintf(stderr, __VA_ARGS__); exit(1); }

// Return value is tri-state: 0 (success), a voodoo singleton, or error
#define BARRIER_WAIT_OR_DIE(pBARRIER) { \
	int bret = pthread_barrier_wait(pBARRIER); \
	if (bret && bret != PTHREAD_BARRIER_SERIAL_THREAD) \
		die("pthread_barrier_wait() failed: %s", strerror(bret)); \
}

// thread values: some are cmdline options, some are computed.
struct tvals_t {
	int fd, overcommit, RW, nthreads, unmap, no_sleep, stride, walking,
	    jumparound;
	long loop;
	unsigned int *mapped, *last_byte, hiperf, seconds, proceed;
	unsigned long flags, access_offset;
	size_t fsize;
	char syncit[10];
	char *fname;
	pthread_t *tids;		// base of dynamic array
	unsigned long *naccesses;	// ditto
};

///////////////////////////////////////////////////////////////////////////
// globals

static int prompt_arg = 0, verbose = 0,
    nLinuxCPUs = 0, nSockets = 0, nCoresPerSocket = 0, nThreadsPerCore = 0;
static pthread_barrier_t barrier;
static pthread_t thread0 = 0;
struct timespec start, stop;

///////////////////////////////////////////////////////////////////////////
// tracing and/or single-stepping

void prompt(char *fmt, ...)
{
    volatile int resp;
    va_list ap;

    if (prompt_arg < 0)	// -q
    	return;	
    if (thread0 && thread0 != pthread_self())
    	return;

    va_start(ap, fmt);
    if (!prompt_arg) {
	vprintf(fmt, ap);
	printf("\n");
    } else {
	printf("\nPress return to ");
	vprintf(fmt, ap);
	printf("...");
	fflush(stdout);
	scanf("%c", (char *)&resp);
    }
    va_end(ap);
}

///////////////////////////////////////////////////////////////////////////

void usage() {
    fprintf(stderr,
    	"usage: maptrap <-P|-S> [-c:CfFH:l:L:moOprRs:t:T:uw:W ] filename\n");
    fprintf(stderr, "\t-S    MAP_SHARED (default)\n\n");
    fprintf(stderr, "\t-P    MAP_PRIVATE\n");
    fprintf(stderr, "\t-c 1  create/populate the file via write(2)\n");
    fprintf(stderr, "\t-c 2     \"       \"     \"   \"    \"  ftruncate(2)\n");
    fprintf(stderr, "\t-c 3     \"       \"     \"   \"    \"  truncate(2)\n");
    fprintf(stderr, "\t-C    close(2) and reopen file after creation\n");
    fprintf(stderr, "\t-d    delete file before creating it\n");
    fprintf(stderr, "\t-f    fsync() after update\n");
    fprintf(stderr, "\t-F    fdatasync() after update\n");
    fprintf(stderr, "\t-h n  high-performance test n: each thread does...\n");
    fprintf(stderr, "\t   1  fixed reads from per-thread cache line\n");
    fprintf(stderr, "\t   2  cacheline walk load  in 1st 2G of a file (full)\n");
    fprintf(stderr, "\t   3  cacheline walk load  in 1st 2G of a file (chunk)\n");
    fprintf(stderr, "\t   4  cacheline walk store to 1st 2G of a file (full)\n");
    fprintf(stderr, "\t   5  cacheline walk store to 1st 2G of a file (chunk)\n");
    fprintf(stderr, "\t   6  cacheline walk LD-ST in 1st 2G of a file (full)\n");
    fprintf(stderr, "\t   7  cacheline walk LD-ST in 1st 2G of a file (chunk)\n");
    fprintf(stderr, "\t   8  random LD            in 1st 2G of a file\n");
    fprintf(stderr, "\t   9  random LD-incr-ST    in 1st 2G of a file\n");
    fprintf(stderr, "\t-j    jump around (random access across entire file)\n");
    fprintf(stderr, "\t-l n  loop n times (default 1)\n");
    fprintf(stderr, "\t-L n  loop for n seconds (default: unused, see -l)\n");
    fprintf(stderr, "\t-m    msync() after update\n");
    fprintf(stderr, "\t-p    pause/prompt for each step\n");
    fprintf(stderr, "\t-q    no output, suppress 1-second loop delay\n");
    fprintf(stderr, "\t-o    initial offset (bytes)\n");
    fprintf(stderr, "\t-O    overcommit memory (accesses start beyond EOF)\n");
    fprintf(stderr, "\t-r    read(2) the file first\n");
    fprintf(stderr, "\t-R    read memory accesses\n");
    fprintf(stderr, "\t-s n  stride (in bytes) for each loop iteration\n");
    fprintf(stderr, "\t-t n  size of file for (f)truncate (default 16M)\n");
    fprintf(stderr, "\t-T n  number of threads (default 1)\n");
    fprintf(stderr, "\t      'ALL' will get the max %d\n", nLinuxCPUs);
    fprintf(stderr, "\t      'CORES' will get one thread per core\n");
    fprintf(stderr, "\t-u    suppress final munmap()\n");
    fprintf(stderr, "\t-v    increase verbosity (might hurt performance)\n");
    fprintf(stderr, "\t-w n  walk the entire space, stride n, obey -l|-L\n");
    fprintf(stderr, "\t-W    write memory accesses\n");
    fprintf(stderr, "\t-Z    suppress inter-step sleep\n");
    exit(1);
}

///////////////////////////////////////////////////////////////////////////
// file creation routines
// (f)truncate: file must be open and writeable.  Leave it empty, 
//              it's supposed to return zeros
// write: will be opened and written

int create_file(char *fname) {
    int fd;

    prompt("open(%s, O_CREAT)", fname);
    if ((fd = open(fname, O_RDWR | O_CREAT, 0777)) == -1) {
    	perror("file creation failed");
	exit(1);
    }
    return fd;
}

int create_ftruncate(char *fname, size_t size) {
    int fd;

    fd = create_file(fname);
    prompt("ftruncate(%s, %lld)", fname, size);
    if (ftruncate(fd, size) == -1) {
    	perror("ftruncate failed");
	exit(1);
    }
    return fd;
}

int create_truncate(char *fname, size_t size) {
    int fd;

    fd = create_file(fname);
    close(fd);
    prompt("truncate(%s, %lld)", fname, size);
    if (truncate(fname, size) == -1) {
    	perror("truncate failed");
	exit(1);
    }
    return -1;		// cuz I didn't really open it
}

// Fill it with capital A, then update to B, C, D....
int create_write(char *fname) {
    int fd;

    char *A = "AAAAAAAA";
    
    prompt("create %s containing \"%s\"", fname, A);
    fd = create_file(fname);
    if ((write(fd, A, strlen(A))) != strlen(A)) {
    	perror("fill file failed");
	exit(1);
    }
    return fd;
}

///////////////////////////////////////////////////////////////////////////
// hiperf helpers and payloads

void *hiperf_fixed_read_personal_cacheline(
	struct tvals_t *tvals, unsigned int myindex)
{
    unsigned int *access = NULL;
    volatile unsigned int currval, *proceed;
    unsigned long naccesses = 0;

    access = (void *)((unsigned long)tvals->mapped + (64 * myindex));
    proceed = &(tvals->proceed);	// unroll this reference

    while (*proceed) {
    	currval = *access;
	naccesses++;
    }
    tvals->naccesses[myindex] = naccesses;
    if (verbose > 1) printf("%s index %3u had %'lu accesses\n",
	__FUNCTION__, myindex, naccesses);
    myindex = currval;	// forestall "unused variable" compiler whining
    return NULL;
}

void set_hiperf_limits(
	unsigned int **access, unsigned long *limit, unsigned int **reset,
	struct tvals_t *tvals, unsigned int myindex, int full)
{
    unsigned long base = (unsigned long)tvals->mapped,
    	          span = 1024L * 1024L * 64L;  // 32 threads in 2G file

    if (tvals->nthreads > 32 && !myindex)	// don't need copies
    	die("hiperf_xxx() are limited to 32 threads * 64M == 2G files\n");
    *access = (void *)(base + myindex * span);
    if (full) {
	*limit = base + (1L << 31);
	*reset = (void *)base;
    } else {
	*limit = (unsigned long)(*access) + span;
	*reset = *access;
    }
}

void *hiperf_walk_read_2G(struct tvals_t *tvals, unsigned int myindex, int full)
{
    unsigned int *access, *reset;
    volatile unsigned int currval, *proceed;
    unsigned long naccesses = 0, limit;

    set_hiperf_limits(&access, &limit, &reset, tvals, myindex, full);
    proceed = &(tvals->proceed);

    while (*proceed) {
    	currval = *access;
	naccesses++;
	access = (void *)((unsigned long)access + 64);
	if ((unsigned long)access >= limit)
		access = reset;
    }
    tvals->naccesses[myindex] = naccesses;
    if (verbose > 1) printf("%s index %3u had %'lu accesses\n",
	__FUNCTION__, myindex, naccesses);
    myindex = currval;	// forestall "unused variable" compiler whining
    return NULL;
}

void *hiperf_walk_RW_2G(struct tvals_t *tvals, unsigned int myindex, int full)
{
    unsigned int *access, *reset;
    volatile unsigned int currval = 42, *proceed;
    unsigned long naccesses = 0, limit;

    set_hiperf_limits(&access, &limit, &reset, tvals, myindex, full);
    proceed = &(tvals->proceed);
    while (*proceed) {
    	currval = *access;
    	*access = currval;
	naccesses++;	// same cache line
	access = (void *)((unsigned long)access + 64);
	if ((unsigned long)access >= limit)
		access = reset;
    }
    tvals->naccesses[myindex] = naccesses;
    if (verbose > 1) printf("%s index %3u had %'lu accesses\n",
	__FUNCTION__, myindex, naccesses);
    myindex = currval;	// forestall "unused variable" compiler whining
    return NULL;
}

void *hiperf_walk_write_2G(struct tvals_t *tvals, unsigned int myindex, int full)
{
    unsigned int *access, *reset;
    volatile unsigned int currval = 42, *proceed;
    unsigned long naccesses = 0, limit;

    set_hiperf_limits(&access, &limit, &reset, tvals, myindex, full);
    proceed = &(tvals->proceed);
    while (*proceed) {
    	*access = currval;
	naccesses++;
	access = (void *)((unsigned long)access + 64);
	if ((unsigned long)access >= limit)
		access = reset;
    }
    tvals->naccesses[myindex] = naccesses;
    if (verbose > 1) printf("%s index %3u had %'lu accesses\n",
	__FUNCTION__, myindex, naccesses);
    myindex = currval;	// forestall "unused variable" compiler whining
    return NULL;
}

void *hiperf_random_read_2G(struct tvals_t *tvals, unsigned int myindex)
{
    unsigned int *seedp;
    volatile unsigned int *access, currval, *proceed;
    unsigned long naccesses = 0, base, mask;

    // Unroll some references
    base = (unsigned long)tvals->mapped;
    proceed = &(tvals->proceed);

    // I need per-thread space for the RNG rolling seed.  man 3 rand_r
    seedp = (void *)&tvals->naccesses[myindex];
    *seedp = myindex * 1000 + time(NULL);

    mask = ~(sizeof(*access) - 1UL);
    while (*proceed) {
	access = (void *)(base + (rand_r(seedp) & mask));
    	currval = *access;
	naccesses++;
    }
    tvals->naccesses[myindex] = naccesses;
    if (verbose > 1) printf("%s index %3u had %'lu accesses\n",
	__FUNCTION__, myindex, naccesses);
    myindex = currval;	// forestall "unused variable" compiler whining
    return NULL;
}

void *hiperf_random_incr_2G(struct tvals_t *tvals, unsigned int myindex)
{
    volatile unsigned int *access, currval, *proceed;
    unsigned int *seedp;
    unsigned long naccesses = 0, base, mask;

    // Unroll some references
    base = (unsigned long)tvals->mapped;
    proceed = &(tvals->proceed);

    // I need per-thread space for the RNG rolling seed.  man 3 rand_r
    seedp = (void *)&tvals->naccesses[myindex];
    *seedp = myindex * 1000 + time(NULL);

    mask = ~(sizeof(*access) - 1UL);
    while (*proceed) {
	unsigned long offset;

	offset = rand_r(seedp) & mask;
	access = (void *)(base + offset);
	currval = *access;
	// printf("0x%16lu = %d\n", offset, currval);
	currval++;
	*access = currval;
	naccesses += 2;
    }
    tvals->naccesses[myindex] = naccesses;
    if (verbose > 1) printf("%s index %3u had %'lu accesses\n",
	__FUNCTION__, myindex, naccesses);
    myindex = currval;	// forestall "unused variable" compiler whining
    return NULL;
}

///////////////////////////////////////////////////////////////////////////
// Threaded routine

void *payload(void *threadarg)
{
    struct tvals_t *tvals = (struct tvals_t *)threadarg;
    pthread_t mytid = pthread_self();
    unsigned long foffset, stride, naccesses = 0;
    char *s;
    unsigned int *access, curr_val, myindex, ncores;
    volatile unsigned int *proceed;
    long loop;
    int LinuxCPU;
    cpu_set_t cpuset;
    
    for (myindex = 0; myindex < tvals->nthreads; myindex++) {
	if (mytid == tvals->tids[myindex])
	    break;
    }
    if (myindex >= tvals->nthreads) die("Cannot find my TID\n");

    // In TM there are 4 threads per core (if enabled) and only
    // one socket.  So Linux logical CPU 0-3 is core 0 HT 0-3 and so on, as
    // verified via "ps -mo pid,tid,fname,user,psr -p `pgrep maptrap`".
    // Thus the core-major, HT-minor approach is Linux affinity 0,4,8...124
    // for the first 32 threads, then 1,5,9...125 for the next 32 and so on.

    ncores = nSockets * nCoresPerSocket;
    LinuxCPU = myindex; 	// Is this always true?
    if (verbose)	// also use -L1 to see this
    	printf("ncores %d index %d cpu %u\n", ncores, myindex, LinuxCPU);

    CPU_ZERO(&cpuset);
    CPU_SET(LinuxCPU, &cpuset);
    if (pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset) == -1) {
    	perror("pthread_setaffinity_np() failed");
	return NULL;
    }
    usleep(50000);	// force a reschedule and allow affinity to kick in

    // Let the thread startup settle and start the clock.

    BARRIER_WAIT_OR_DIE(&barrier);

    switch (tvals->hiperf) {
    case 1:
    	return hiperf_fixed_read_personal_cacheline(tvals, myindex);
    case 2:
    	return hiperf_walk_read_2G(tvals, myindex, 1);
    case 3:
    	return hiperf_walk_read_2G(tvals, myindex, 0);
    case 4:
    	return hiperf_walk_write_2G(tvals, myindex, 1);
    case 5:
    	return hiperf_walk_write_2G(tvals, myindex, 0);
    case 6:
    	return hiperf_walk_RW_2G(tvals, myindex, 1);
    case 7:
    	return hiperf_walk_RW_2G(tvals, myindex, 0);
    case 8:
    	return hiperf_random_read_2G(tvals, myindex);
    case 9:
    	return hiperf_random_incr_2G(tvals, myindex);
    }

    curr_val = 0x42424241;	// For WRONLY, gotta start somewhere
    stride = 0;			// Even if unused, avoid compiler warnings

    // bytes
    if (tvals->jumparound) {
	// random(3): 0 - RAND_MAX == 2G.   Period approx 16 * 2^32.  
	// FIXME: see hiperf case 2 for thread-local seeds
	unsigned long delta = random() % (tvals->fsize - sizeof(*access));
	access = (void *)((unsigned long)tvals->mapped + (delta & (~15L)));
    } else if (tvals->stride > 0) {
	stride = tvals->stride;
	access = (void *)((unsigned long)tvals->mapped + tvals->access_offset);
	if (tvals->overcommit)
		access += tvals->fsize;
    } else {
	stride = -tvals->stride;
	access = (void *)(((unsigned long)tvals->last_byte) + 1 - stride);
    }

    loop = tvals->loop;	// poor man's TLS
    proceed = &(tvals->proceed);	// unroll this reference
    do {
    	if (!prompt_arg) printf("\n");

    	if (!tvals->overcommit && tvals->access_offset >= tvals->fsize)
		die("access offset beyond EOF\n");

	foffset = (unsigned long)access - (unsigned long)tvals->mapped;
	if (tvals->RW & READIT) {
		prompt("integer get @ offset %llu (0x%p)", foffset, access);
		curr_val = *access;
		naccesses++;
		if (prompt_arg >= 0)
			printf("               = 0x%08x\n", curr_val);
	}
	if (tvals->RW & WRITEIT) {
		prompt("integer put @ offset %llu (0x%p)", foffset, access);
		curr_val += 1;
		*access = curr_val;
		naccesses++;
		if (prompt_arg >= 0)
			printf("              -> 0x%08x\n", curr_val);
	}

	for (s = tvals->syncit; *s; s++) switch(*s) {
	case 'f':
		prompt("fsync");
		if (fsync(tvals->fd) == -1)
			perror("fsync failed");
		break;
	case 'F':
		prompt("fdatasync");
		if (fdatasync(tvals->fd) == -1)
			perror("fdatasync failed");
		break;
	case 'm':
		prompt("msync");
		if (msync(access, sizeof(curr_val), MS_SYNC) == -1)
			perror("msync failed");
		break;
	}
	if (!prompt_arg && !tvals->no_sleep)
		sleep(1);

	// Handle wraparound, do everything inline.
	if (tvals->jumparound) {
		// random(3): 0 - RAND_MAX == 2G.   Period == 2^32.  
		unsigned long delta = random() % (tvals->fsize - 4);
		access = (void *)((unsigned long)tvals->mapped + (delta & (~15L)));
	} else if (tvals->stride > 0) {
		access = (void *)(unsigned long)access + stride;
		if ((unsigned long)access > (unsigned long)tvals->last_byte - stride) {
			access = tvals->mapped;
			if (tvals->walking)
				loop--;
		}
	} else {
		access = (unsigned int *)((unsigned long)access - stride);
		if ((unsigned long)access < (unsigned long)tvals->mapped) {
			access = (void *)((unsigned long)(tvals->last_byte) + 1 - stride);
			if (tvals->walking)
				loop--;
		}
	}
	// Loop count for walks is handled in the wraparound fixups
	if (!tvals->walking)
		loop--;

    } while (loop > 0 || *proceed);
    tvals->naccesses[myindex] = naccesses;
    return NULL;
}

///////////////////////////////////////////////////////////////////////////

long bignum(char const *instr, int Tsigned_Funsigned) {
    char *mult;
    unsigned long tmp = strtoul(instr, &mult, 10);

    if (tmp < 0 && !Tsigned_Funsigned) {
    	fprintf(stderr, "%s cannot be negative here\n", instr);
	usage();
    }

    switch (*mult) {

    case 0: return tmp;		// NUL term

    case 'k':
    case 'K': return tmp << 10;

    case 'm':
    case 'M': return tmp << 20;

    case 'g':
    case 'G': return tmp << 30;
	      
    }
    die("Bad multiplier on %s\n", optarg);
    return 0;
}

///////////////////////////////////////////////////////////////////////////
// Be lazy, lscpu has done a lot of work.

static char *filterlscpu = "lscpu | awk '"
	"BEGIN {S=CPS=TPC=41}"
	"/^Core.s. per socket:/ {CPS=$NF}"
	"/^Socket.s.:/ {S=$NF}"
	"/^Thread.s. per core:/ {TPC=$NF}"
	"END {print S, CPS, TPC}'";

static void get_cpuinfo()
{
    FILE *lscpu;

    nSockets = nCoresPerSocket = nThreadsPerCore = -1;
    if (!(lscpu = popen(filterlscpu, "r")))
    	die("Cannot run lscpu: %s", strerror(errno));
    if (fscanf(lscpu, "%d %d %d",
	&nSockets, &nCoresPerSocket, &nThreadsPerCore) != 3)
	    die("fscanf(lscpu) failed: %s", strerror(errno));
    pclose(lscpu);
    if ((nLinuxCPUs = sysconf(_SC_NPROCESSORS_ONLN)) < 0)
    	die("Cannot determine active logical CPU count\n");
    printf("%4d logical CPUs reported\n", nLinuxCPUs);
    printf("%4d sockets\n", nSockets);
    printf("%4d cores per socket\n", nCoresPerSocket);
    printf("%4d threads per core\n", nThreadsPerCore);
    if (nLinuxCPUs != nSockets * nCoresPerSocket * nThreadsPerCore)
	die("CPU info doesn't line up");
}

///////////////////////////////////////////////////////////////////////////

void cmdline_prepfile(struct tvals_t *tvals, int argc, char *argv[])
{
    int opt, do_create = 0, do_delete = 0, do_close = 0, read1st = 0;
    struct stat buf;
    char *s, *bigT = NULL;

    get_cpuinfo();

    // Initialize default thread payload values
    memset(tvals, 0, sizeof(struct tvals_t));
    tvals->fd = -1;
    tvals->fsize = 16 * 1024 * 1024;	// typical MongoDB
    tvals->flags = MAP_SHARED;		// Not even sure if -P makes sense
    tvals->nthreads = 1;		// main() is a thread
    tvals->stride = sizeof(int);	// that which gets accessed
    tvals->unmap = 1;			// usually call munmap()

    s = tvals->syncit;
    while ((opt = getopt(argc, argv, "c:CdfFh:jl:L:mo:OpPqrRSs:t:T:uvw:WZ"))
		!= EOF)
      switch (opt) {

	// Required
    	case 'P':	tvals->flags = MAP_PRIVATE; break;
    	case 'S':	tvals->flags = MAP_SHARED; break;

    	case 'R':	tvals->RW |= READIT; break;
    	case 'W':	tvals->RW |= WRITEIT; break;

	// Optional
    	case 'c':	do_create = atoi(optarg); break;
    	case 'C':	do_close = 1; break;
    	case 'd':	do_delete = 1; break;

    	case 'f':	
    	case 'F':
    	case 'm':
		if (sizeof(tvals->syncit) - strlen(tvals->syncit) < 2)
			die("Too many syncs\n");
		*s++ = opt;
		break;

	case 'h':	tvals->hiperf = bignum(optarg, 0); break;
	case 'j':	tvals->jumparound = 1; break;
    	case 'l':	tvals->loop = bignum(optarg, 0); break;
    	case 'L':	tvals->seconds = bignum(optarg, 0); break;
    	case 'o':	tvals->access_offset = strtoul(optarg, NULL, 0); break;
	case 'O':	tvals->overcommit = 1; break;
    	case 'p':	prompt_arg = 1; break;
	case 'q':	prompt_arg = -1; break;
    	case 'r':	read1st = 1; break;
    	case 't': 	tvals->fsize = bignum(optarg, 0); break;
    	case 'T':	bigT = optarg; break;
	case 'u':	tvals->unmap = 0; break;
	case 'v':	verbose++; break;

    	case 'w':	tvals->walking = 1;	// fall through
    	case 's':	tvals->stride = bignum(optarg, 1); break;
			if (abs(tvals->stride) < 4)
				die("|stride| must be at least 4");
			break;

    	case 'Z':	tvals->no_sleep = 1; break;

	default:	usage();
    }

    // Idiot checks
    if (tvals->loop && tvals->seconds)
	die("Only one of -l | -L");
    if (!tvals->loop) tvals->loop = 1;
    if (tvals->seconds)		// Loop counter will be ignored.
	tvals->proceed = 1;	// Flushed at pthread_barrier_xxx.

    if (tvals->hiperf) {		// canned runs
    	prompt_arg = -1;		// Some expertise is assumed
	tvals->flags = MAP_SHARED;
	tvals->RW = READIT | WRITEIT;	// probably ignored
    	if (!tvals->seconds)
	    tvals->seconds = 10;
    } else {			
	if (!tvals->RW)
    	    die("Use at least one of -R|-W, or -h which ignores them\n");
    }

    if (bigT) {
    	if (!strcmp(bigT, "ALL"))
		tvals->nthreads = nLinuxCPUs;
    	else if (!strcmp(bigT, "CORES"))
		// affinity logic does cores first
		tvals->nthreads = nLinuxCPUs / nThreadsPerCore;
    	else
		tvals->nthreads = atol(bigT);
    }
    printf("%3d threads will be spawned\n", tvals->nthreads);
    if (tvals->nthreads > 1 && nLinuxCPUs == 1)
    	die("Can't effectively multithread on a nosmp system\n");
    if (tvals->nthreads < 1 || tvals->nthreads > nLinuxCPUs)
    	die("Dude, %d threads?  Nice try.\n", tvals->nthreads);

    // do_delete and do_close only make sense when trying to create a file

    tvals->fname = argv[optind];
    if (!do_create) {
	if (stat(tvals->fname, &buf) == -1)
	    die("Cannot access %s: %s\n", tvals->fname, strerror(errno));
	if (buf.st_size)	// real file, not res2hotchar
	    tvals->fsize = buf.st_size;
    } else {
    	if (do_delete) {	// remove it first, ignore most errors
	    prompt("unlink %s", tvals->fname);
	    if (unlink(tvals->fname)) {
		if (errno != ENOENT)
	    	    die("unlink failed: %d %s\n", errno, strerror(errno));
	    }
	} else if (stat(tvals->fname, &buf) != -1)
	    die("%s already exists\n", tvals->fname);

	switch (do_create) {
	case 1: tvals->fd = create_write(tvals->fname); break;
	case 2: tvals->fd = create_ftruncate(tvals->fname, tvals->fsize); break;
	case 3: tvals->fd = create_truncate(tvals->fname, tvals->fsize); break;
	default:
		die("Unknown create option %d\n", do_create);
	}

	if (do_close && tvals->fd >= 0) {
	    if (close(tvals->fd)) {
	    	perror("close after create failed");
		exit(1);
	    }
	    tvals->fd = -1;
	}
    }

    if (tvals->fd == -1) {
	prompt("open %s", tvals->fname);
	if ((tvals->fd = open(tvals->fname, O_RDWR)) == -1) {
	    perror("open failed");
	    exit(1);
	}
    }

    if (read1st) {
	unsigned int junk;

	if ((read(tvals->fd, &junk, sizeof(junk))) == -1)
	    perror("read failed");
	else
    	    printf("read() got 0x%x\n", junk);
    }

    // Thread tracking
    if (!(tvals->tids = calloc(tvals->nthreads, sizeof(pthread_t)))) {
    	fprintf(stderr, "calloc(%d threads) failed: %s\n",
		tvals->nthreads, strerror(errno));
	exit(1);
    }
    if (!(tvals->naccesses = calloc(tvals->nthreads, sizeof(unsigned long)))) {
    	fprintf(stderr, "calloc(%d naccesses) failed: %s\n",
		tvals->nthreads, strerror(errno));
	exit(1);
    }

    // Final idiot checks
    switch (tvals->hiperf) {
    case 2 ... 7:
	tvals->stride = 64;	// only used during final numbers report
				// fall through
    case 8 ... 9:
	if (tvals->fsize < (1L<<31) - 1L)
	    die("%s size must be at least 2G", tvals->fname);
	break;
    }
}

///////////////////////////////////////////////////////////////////////////
// POSIX: Depending on flags, this could create a COW, and it seems to be
// working via the rest of the kernel PT magic.

void mmapper(struct tvals_t *tvals)
{
    char cmd[80];

    prompt("mmap(%s) offset 0 for %lu bytes", tvals->fname, tvals->fsize);
    if ((tvals->mapped = mmap(NULL,
    			tvals->fsize,
			PROT_READ | PROT_WRITE,
			tvals->flags | MAP_NORESERVE,
			tvals->fd,
			0)) == MAP_FAILED) {
	perror("mmap failed");
	if (close(tvals->fd) == -1)
	    	perror("close failed after mmap failed");
	exit(1);
    }
    tvals->last_byte = (void *)((unsigned long)tvals->mapped + tvals->fsize - 1);

    if (verbose) {
    	printf("PID = %d, map range = 0x%p - 0x%p\n",
    		getpid(),
		tvals->mapped,
		tvals->last_byte);
#if 1
    	sprintf(cmd, "/bin/grep '^%lx' /proc/%d/maps",
    		(unsigned long)tvals->mapped, getpid());
    	printf("%s\n", cmd);
    	system(cmd);
#endif
    }
}

///////////////////////////////////////////////////////////////////////////

void printtime(
	struct tvals_t *tvals, struct timespec *start, struct timespec *stop)
{
    unsigned long naccesses;
    int i;
    float delta =
        ((float)(stop->tv_nsec) / 1000000000.0) + (float)(stop->tv_sec)
        -
        ((float)(start->tv_nsec) / 1000000000.0 + (float)(start->tv_sec));

    naccesses = 0;
    for (i = 0; i < tvals->nthreads; i++) {
    	naccesses += tvals->naccesses[i];
	if (verbose)
	    printf("Thread %4d: %'22lu accesses\n", i, tvals->naccesses[i]);
    }

    printf("%'20lu accesses across %d threads in %.2f seconds\n",
	 naccesses, tvals->nthreads, delta);
    if (delta > 0.99) {
        unsigned long aps = (float)naccesses / delta;

        printf("%'20lu accesses/second\n", aps);
        printf("%'20lu accesses/thread/second\n",
	    (unsigned long)((float)aps/(float)tvals->nthreads));
	if (tvals->stride >= 64) 
	    // Each access is a cache line of FAM, that's << 6 b/s.
	    // Output as MB/sec, that's >> 20.
            printf("%20lu MB/sec cache traffic\n", aps >> 14);
    }
}

int main(int argc, char *argv[])
{
    struct tvals_t tvals;
    int i, ret;

    setlocale(LC_NUMERIC, "");          // backtick for radix comma
    cmdline_prepfile(&tvals, argc, argv);
    mmapper(&tvals);
    srandom(time(NULL));		// Randomize the seed

    // load and go.  Threads pause at the barrier.  The "+1" is this main
    // thread which does timing and other stuff.

    if (pthread_barrier_init(&barrier, NULL, tvals.nthreads + 1) == -1)
    	die("pthread_barrier_init() failed");

    for (i = 0; i < tvals.nthreads; i++) {
	if ((ret = pthread_create(&tvals.tids[i], NULL, payload, &tvals)))
		die("pthread_create() failed");
    }
    thread0 = tvals.tids[0];
    BARRIER_WAIT_OR_DIE(&barrier);
    clock_gettime(CLOCK_MONOTONIC, &start);

    if (tvals.seconds) {
	sleep(tvals.seconds);
	tvals.proceed = 0;
    }

    for (i = 0; i < tvals.nthreads; i++) {
    	void *payload_ret;

	if ((ret = pthread_join(tvals.tids[i], &payload_ret)))
		die("pthread_join() failed");
    }
    clock_gettime(CLOCK_MONOTONIC, &stop);

    printtime(&tvals, &start, &stop);

    //---------------------------------------------------------------------
    if (tvals.unmap) {
    	prompt("final munmap(%s)", tvals.fname);
    	if (munmap(tvals.mapped, PAGE_SIZE) == -1)
		perror("munmap failed");
    }

    prompt("final close(%s)", tvals.fname);
    if (close(tvals.fd) == -1)
	perror("final close failed");

    exit(0);
}
