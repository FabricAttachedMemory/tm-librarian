#!/usr/bin/python3
#
# Test file create/open/close/remove handling
#

import sys
import os
import random
import multiprocessing
import argparse

verbose = 0

#
# single_open_close() - create data files
#   l_fd - list of file names
#   bn - data file basename
#   ft - total number of data files
#
def create_data_files(l_fd, bn, ft):

    for i_ft in range(ft):
        c_fn = '%s.%s' % (bn, str(i_ft))
        if verbose > 1:
            print('create file %s' % c_fn)
        fd = open(c_fn, 'w+b', buffering=0)
        fd.close()
        l_fd.append(c_fn)

#
# remove_data_files() - remove data files
#   l_fd - list of file names
#
def remove_data_files(l_fd):

    for i_ft in reversed(range(len(l_fd))):
        c_fn = l_fd[i_ft]
        if verbose > 1:
            print('remove file [%d] %s' % (i_ft, c_fn))
        os.unlink(c_fn)
        del l_fd[i_ft]

#
# single_open_close()
#   l_fd - list of file names
#   loop - number of iterations to execute
#   moc - number of opens for each file
#   st - 'seq' sequential or 'rand' random close order
#
def single_open_close(l_fd, loop, moc, st):

    if verbose > 2:
        print('open/close (loop = %d, moc = %d, st = %s)' % (loop, moc, st))

    pid = os.getpid()

    for i_loop in range(loop):

        fd = {}

        for i_ft in range(len(l_fd)):
            fl = []
            for i_moc in range(moc):
                c_fn = l_fd[i_ft]
                if verbose > 1:
                    print('open fh[%d/%d/%d] %s' % (i_ft, i_moc, pid, c_fn))
                fl.append(open(c_fn, 'w+b', buffering=0))

            fd[i_ft] = fl

        for i_ft in reversed(range(len(l_fd))):
            for i_moc in reversed(range(moc)):

                t_fl = fd[i_ft]

                if st == 'seq':
                    cur = i_moc
                else:
                    cur = random.randrange(len(t_fl))

                if verbose > 1:
                    print('close t_fl[%d/%d/%d]: %s (len(t_fl) = %d)' % (i_ft, cur, pid, t_fl[cur], len(t_fl)))
                t_fl[cur].close()
                t_fl.remove(t_fl[cur])

        del fd

#
# multi_open_close()
#   l_fd - list of file names
#   loop - number of iterations to execute
#   moc - number of opens for each file
#   st - 'seq' sequential or 'rand' random close order
#   tcnt - number of threads to spawn
#
def multi_open_close(l_fd, loop, moc, st, tcnt):

    if verbose > 2:
        print('multi open/close (loop = %d, moc = %d, st = %s, tcnt = %d)' % (loop, moc, st, tcnt))

    processes = []
    for tn in range(tcnt):
        try:
            p = multiprocessing.Process(target=single_open_close, args=(l_fd, loop, moc, st), name='t_open_close')
            processes.append(p)
        except Exception as e:
            raise SystemExit("Error %s raised creating thread" % (str(e)))

    for p in processes:
        try:
            p.start()
        except Exception as e:
            raise SystemExit("Error %s raised starting process: %s" % (str(e), str(p.name)))

    for p in processes:
        try:
            p.join()
        except Exception as e:
            raise SystemExit("Error %s raised joining process: %s" % (str(e), str(p.name)))

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '-v',
        action='store',
        dest='verbose',
        type=int,
        default=0,
        help='verbose level, 0 = none, 1 = routine entry, 2 = create/open/close/unlink')
    parser.add_argument(
        '-i',
        action='store',
        dest='iter_cnt',
        type=int,
        default=1,
        help='number of iterations to perform (create/open/close/unlink')
    parser.add_argument(
        '-l',
        action='store',
        dest='loop_cnt',
        type=int,
        default=1,
        help='number of loops to perform (open/close')
    parser.add_argument(
        '-f',
        action='store',
        dest='file_cnt',
        type=int,
        default=5,
        help='number of files to operate on')
    parser.add_argument(
        '-p',
        action='store',
        dest='open_cnt',
        type=int,
        default=1,
        help='number of parallel opens per file')
    parser.add_argument(
        '-t',
        action='store',
        dest='thread_cnt',
        type=int,
        default=1,
        help='number of parallel threads to fork')
    parser.add_argument(
        '-o',
        choices=['seq','rnd'],
        default='seq',
        dest='h_type',
        help='file close handling (seq = same order as open, rnd = random order')
    parser.add_argument(
        '-b',
        action='store',
        dest='file_base',
        type=str,
        default='/lfs/data',
        help='base name for data files')

    args = parser.parse_args()

    if args.iter_cnt < 1:
        parser.error("Must supply a positive iteration count (-i ITER_CNT) !!!")
    if args.loop_cnt < 1:
        parser.error("Must supply a positive loop count (-l LOOP_CNT) !!!")
    if args.file_cnt < 1:
        parser.error("Must supply a positive file count (-f FILE_CNT) !!!")
    if args.open_cnt < 1:
        parser.error("Must supply a positive open count (-p OPEN_CNT) !!!")
    if args.thread_cnt < 1:
        parser.error("Must supply a positive thread count (-t THREAD_CNT) !!!")

    verbose = args.verbose

    if verbose > 0:
        print('verbose    = %d' % verbose)
        print('iter_cnt   = %d' % args.iter_cnt)
        print('loop_cnt   = %d' % args.loop_cnt)
        print('file_cnt   = %d' % args.file_cnt)
        print('open_cnt   = %d' % args.open_cnt)
        print('thread_cnt = %d' % args.thread_cnt)
        print('h_type     = %s' % args.h_type)

    l_fd = []

    for ii in range(args.iter_cnt):
        create_data_files(l_fd, args.file_base, args.file_cnt)
        multi_open_close(l_fd, args.loop_cnt, args.open_cnt, args.h_type, args.thread_cnt)
        remove_data_files(l_fd)

    sys.exit(0)
