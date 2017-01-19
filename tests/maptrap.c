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

// gcc -o maptrap -Wall -Werror -pthread maptrap.c

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

#ifndef PAGE_SIZE
#define PAGE_SIZE 4096
#endif

#define READIT	0x01	// cuz O_RDONLY == 0x0 and I need bit OR'ing
#define WRITEIT	0x02

#define MAXTHREADS 10

#define die(...) { fprintf(stderr, __VA_ARGS__); exit(1); }

static int do_prompt = 0;

// thread values: some are cmdline options, some are computed.
struct tvals_t {
	int fd, overcommit, RW, nthreads, unmap, no_sleep, stride, walking,
	    jumparound;
	long loop;
	unsigned int *mapped, *last_byte;
	unsigned long flags, access_offset;
	size_t fsize;
	char syncit[10];
	pthread_t *tids;
	char *fname;
};

///////////////////////////////////////////////////////////////////////////
// tracing and/or single-stepping

void prompt(char *fmt, ...)
{
    volatile int resp;
    va_list ap;

    if (do_prompt < 0) return;

    va_start(ap, fmt);
    if (!do_prompt) {
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
    fprintf(stderr, "usage: maptrap <-P|-S> [-c n -CfFlmoOprRstW ] filename\n");
    fprintf(stderr, "\t-P    MAP_PRIVATE\n");
    fprintf(stderr, "\t-S    MAP_SHARED\n\n");
    fprintf(stderr, "\t-c 1  create/populate the file via write(2)\n");
    fprintf(stderr, "\t-c 2     \"       \"     \"   \"    \"  ftruncate(2)\n");
    fprintf(stderr, "\t-c 3     \"       \"     \"   \"    \"  truncate(2)\n");
    fprintf(stderr, "\t-d    delete file before creating it\n");
    fprintf(stderr, "\t-C    close(2) and reopen file after creation\n");
    fprintf(stderr, "\t-f    fsync() after update\n");
    fprintf(stderr, "\t-F    fdatasync() after update\n");
    fprintf(stderr, "\t-j    jump around (random access)\n");
    fprintf(stderr, "\t-l n  loop n times (default 1)\n");
    fprintf(stderr, "\t-m    msync() after update\n");
    fprintf(stderr, "\t-p    pause/prompt for each step\n");
    fprintf(stderr, "\t-q    no output, suppress 1-second loop delay\n");
    fprintf(stderr, "\t-o    initial offset (bytes)\n");
    fprintf(stderr, "\t-O    overcommit memory (accesses start beyond EOF)\n");
    fprintf(stderr, "\t-r    read(2) the file first\n");
    fprintf(stderr, "\t-R    read-only memory accesses\n");
    fprintf(stderr, "\t-s n  stride (in bytes) for each loop iteration\n");
    fprintf(stderr, "\t-t n  size of file for (f)truncate (default 16M)\n");
    fprintf(stderr, "\t-T n  number of threads (default 1, max %d)\n", MAXTHREADS);
    fprintf(stderr, "\t-u    suppress final munmap()\n");
    fprintf(stderr, "\t-w n  walk the entire space, stride n, obey -l\n");
    fprintf(stderr, "\t-W    write-only memory accesses\n");
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
// Threaded routine

void *payload(void *threadarg)
{
    struct tvals_t *tvals = (struct tvals_t *)threadarg;
    unsigned long foffset, stride;
    char *s;
    unsigned int *access, naccesses = 0;
    unsigned int curr_val;

    curr_val = 0x42424241;	// For WRONLY, gotta start somewhere

    // bytes
    if (tvals->jumparound) {
	// random(3): 0 - RAND_MAX == 2G.   Period == 2^32.  
	unsigned long delta = random() % (tvals->fsize - 4);
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

    do {
    	if (!do_prompt) printf("\n");

    	if (!tvals->overcommit && tvals->access_offset >= tvals->fsize)
		die("access offset beyond EOF\n");

	foffset = (unsigned long)access - (unsigned long)tvals->mapped;
	if (tvals->RW & READIT) {
		prompt("integer get @ offset %llu (0x%p)", foffset, access);
		curr_val = *access;
		naccesses++;
		if (do_prompt >= 0)
			printf("               = 0x%08x\n", curr_val);
	}
	if (tvals->RW & WRITEIT) {
		prompt("integer put @ offset %llu (0x%p)", foffset, access);
		curr_val += 1;
		*access = curr_val;
		naccesses++;
		if (do_prompt >= 0)
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
	if (!do_prompt && !tvals->no_sleep)
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
				tvals->loop--;
		}
	} else {
		access = (unsigned int *)((unsigned long)access - stride);
		if ((unsigned long)access < (unsigned long)tvals->mapped) {
			access = (void *)((unsigned long)(tvals->last_byte) + 1 - stride);
			if (tvals->walking)
				tvals->loop--;
		}
	}
	// Loop count for walks is handled in the wraparound fixups
	if (!tvals->walking)
		tvals->loop--;

    } while (tvals->loop != 0);
    printf("\nTotal accesses: %u\n", naccesses);
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

void cmdline_prepfile(struct tvals_t *tvals, int argc, char *argv[])
{
    int opt, do_create = 0, do_delete = 0, do_close = 0, read1st = 0;
    struct stat buf;
    char *s;

    // Initialize default thread payload values
    memset(tvals, 0, sizeof(struct tvals_t));
    tvals->fd = -1;
    tvals->fsize = 16 * 1024 * 1024;	// typical MongoDB
    tvals->nthreads = 1;		// main() is a thread
    tvals->stride = sizeof(int);	// that which gets accessed
    tvals->unmap = 1;			// usually call munmap()

    s = tvals->syncit;
    while ((opt = getopt(argc, argv, "c:CdfjFl:mo:OpPqrRSs:t:T:uw:WZ")) != EOF)
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

	case 'j':	tvals->jumparound = 1; break;
    	case 'l':	tvals->loop = bignum(optarg, 1); break;
    	case 'o':	tvals->access_offset = strtoul(optarg, NULL, 0); break;
	case 'O':	tvals->overcommit = 1; break;
    	case 'p':	do_prompt = 1; break;
	case 'q':	do_prompt = -1; break;
    	case 'r':	read1st = 1; break;
    	case 't': 	tvals->fsize = bignum(optarg, 0); break;
    	case 'T':	tvals->nthreads = atol(optarg); break;
	case 'u':	tvals->unmap = 0; break;

    	case 'w':	tvals->walking = 1;	// fall through
    	case 's':	tvals->stride = bignum(optarg, 1); break;
			if (abs(tvals->stride) < 4)
				die("|stride| must be at least 4");
			break;

    	case 'Z':	tvals->no_sleep = 1; break;

	default:	usage();
    }

    // Idiot checks
    if (!tvals->loop) tvals->loop = 1;
    if (!tvals->flags) {
    	fprintf(stderr, "Thou shalt use one and only one of -P | -S\n");
	usage();
    }
    if (!tvals->RW) {
    	fprintf(stderr, "Thou shalt use at least one of -R | -W\n");
	usage();
    }
    if (tvals->nthreads < 1 || tvals->nthreads > MAXTHREADS ) {
    	fprintf(stderr, "Dude, %d threads?  Nice try.\n", tvals->nthreads);
	usage();
    }
    if (tvals->nthreads > 1) do_prompt = -1;
    tvals->nthreads--;	// how many times do I have to call pthread_create?

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

    printf("PID = %d, map range = 0x%p - 0x%p\n",
    	getpid(),
	tvals->mapped, tvals->last_byte);
#if 1
    sprintf(cmd, "/bin/grep '^%lx' /proc/%d/maps",
    	(unsigned long)tvals->mapped, getpid());
    printf("%s\n", cmd);
    system(cmd);
#endif
}

///////////////////////////////////////////////////////////////////////////

int main(int argc, char *argv[])
{
    struct tvals_t tvals;
    pthread_t *tids;
    int i;

    cmdline_prepfile(&tvals, argc, argv);
    mmapper(&tvals);

    if (tvals.nthreads && 
        !(tids = calloc(tvals.nthreads, sizeof(pthread_t)))) {
    	fprintf(stderr, "calloc(%d threads) failed: %s\n",
		tvals.nthreads, strerror(errno));
	exit(1);
    }

    //---------------------------------------------------------------------
    // load and go

    for (i = 0; i < tvals.nthreads; i++) {
	int ret;

	if ((ret = pthread_create(&tids[i], NULL, payload, &tvals))) {
		perror("pthread_create() failed");
		exit(1);
	}
    }	
    payload(&tvals);	// I'm a thread, too

    for (i = 0; i < tvals.nthreads; i++) {
	int ret;
    	void *payload_ret;

	if ((ret = pthread_join(tids[i], &payload_ret))) {
		perror("pthread_join() failed");
		exit(1);
	}
    }
    free(tids);
    tids = NULL;

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
