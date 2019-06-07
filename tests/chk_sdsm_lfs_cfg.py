#!/usr/bin/python3 -tt

# Copyright 2019 Hewlett Packard Enterprise Development LP

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2 as
# published by the Free Software Foundation.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License along
# with this program.  If not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA

import os
import sys
import stat
import re

from argparse import Namespace  # result of an argparse sequence
from pdb import set_trace

from backend_sqlite3 import LibrarianDBackendSQLite3
from book_shelf_bos import TMBook, TMShelf, TMBos
from frdnode import BooksIGInterpretation as BII

_debug = os.getenv('DEBUG', 0)

###########################################################################
# Make this WHOLE script a class someday.

_errors = []


def addError(msg='', reset=False):
    global _errors
    if reset:
        _errors = []
        return
    msg = msg.strip()
    if msg:
        _errors.append(msg)
        if _debug:
            print(msg)

###########################################################################


def scan_SDSM_UEFI():
    uefi_variable_file = "/sys/firmware/efi/vars/SDSM_MEMORY_RANGES-99ee5a68-7f17-4e39-b3e5-59085b65790d/data"
    all_regions = []
    all_partitions = []

    # Are we are running on a Superdome Flex with the petrock firmware
    if not os.path.isfile(uefi_variable_file):
        return (None, None)

    # We are interested in the lines like this
    # Start            Size            En Partition
    # 0000000100000000 000002DCC000000 02 p1
    # 0000003040000000 000002E86000000 00 p1
    pattern = re.compile(r'([0-9a-f]+) ([0-9a-f]+) ([0-9a-f]+) ([a-z0-9]+)')
    with open(uefi_variable_file) as f:
        regionnum = 0
        lastpart = None
        for thisline in f:
            match = re.search(pattern, thisline.lower().strip())
            if match:
                # we have a memmap line that matches the regex search
                mm_start = int(match.group(1), 16)
                mm_size = int(match.group(2), 16)
                mm_end = mm_start + mm_size - 1
                mm_enclosure = int(match.group(3), 16)
                mm_partition = match.group(4)
                if lastpart is None:
                    lastpart = mm_partition
                if lastpart != mm_partition:
                    regionnum = 0
                lastpart = mm_partition

                # All of this memory is available as FAM/SDSM memory
                # save this memory region to the list
                if mm_size > 0:
                    all_regions.append(
                        (mm_start,
                         mm_end,
                         '%s:%d' % (mm_partition, regionnum)))
                    regionnum += 1
                    if mm_partition not in all_partitions:
                        all_partitions.append(mm_partition)
    return (all_partitions, all_regions)

###########################################################################
# get the librarian ranges directly from the librarian DB.


def get_librarian_regions(db):
    db.execute('SELECT book_size_bytes FROM globals')
    book_size_bytes = db.fetchone()[0]
    if _debug:
            print('book size = %d bytes' % (book_size_bytes))

    # query the Librarian database for all books, then calculate ranges.
    # It's segmented by IG number, each starting with a book 0 which
    # represents the base address.  GROUP BY for the win!

    db.execute('''SELECT intlv_group & ? AS IG, id AS base, count(*) as nBooks
                  FROM books
                  GROUP BY intlv_group
                  ORDER BY id''', BII.VALUE_MASK)
    db.iterclass = 'generic'
    regions = [
        (ro.base, ro.base + ro.nBooks * book_size_bytes - 1, ro.IG)
        for ro in db ]
    return regions

###########################################################################
# Return None on problems, ranges like get_librarian_regions on success.
# The text is fairly "packed" with respect to odd spaces.
# https://elixir.bootlin.com/linux/v4.12.14/source/Documentation/admin-guide/kernel-parameters.txt#L2136


def get_proc_cmdline_regions():
    seps = {
        '@': 'Force-use',
        '#': 'ACPI',
        '$': 'Reserved',
        '!': 'NVDIM',
    }
    with open('/proc/cmdline', 'r') as f:
        cmdline = f.read()
    if 'memmap' not in cmdline:
        return None, None
    regions = []
    stanzas = cmdline.split('memmap=')[1]
    stanza0 = stanzas.split()[0]
    regionstrs = stanza0.split(',')
    for r in regionstrs:
        for sep, use in seps.items():
            try:
                size, base = r.split(sep)
            except ValueError as e:
                continue
            break
        else:
            addError('Invalid memmap stanza "%s"' % r)
            continue
        try:
            base = int(base, 16)
            mult = size[-1].upper()
            if mult in 'MGT':
                size = int(size[:-1], 16)
                if mult == 'M':
                    size *= (1 << 20)
                elif size == 'G':
                    size *= (1 << 30)
                elif size == 'T':
                    size *= (1 << 40)
            else:
                size = int(size, 16)
        except Exception as err:
            addError('Cannot parse base or size in "%s"' % r)
            continue
        regions.append((base, base + size - 1, -1))   # partition == noop
    return regions

###########################################################################
# Return None on problems, ranges like get_librarian_regions on success.


def tmfs_split(modline, arg):
    stanzas = modline.split('%s=' % arg)[1]
    stanza0 = stanzas.split()[0]
    elems = [ int(s, 16) for s in stanza0.split(',') ]
    return elems


def get_tmfs_module_regions():
    with open('/etc/modprobe.d/tmfs.conf', 'r') as f:
        modline = f.read()
    if 'tmfs_phys' not in modline:
        return None, None
    regions = []
    try:
        phys_base = tmfs_split(modline, 'tmfs_phys_base')
        phys_bound = [ b - 1 for b in tmfs_split(modline, 'tmfs_phys_bound') ]
        assert len(phys_base) == len(phys_bound), 'TMFS base/bound length bad'
        for r in zip(phys_base, phys_bound):
            regions.append(list(r))
            regions[-1].append(-1)
        return regions
    except Exception as err:
        print(str(err), file=sys.stderr)
    return None, None

###########################################################################

_firmware_regions = None


def misfits_in_firmware(who, regions, forceFail=False):
    if _debug:
        print('Validating %s range(s) fit in firmware range(s)' % who)
    for (start, end, partition) in regions:
        docstr = rangestr(start, end)
        if _debug:
            print('\t%s ' % docstr, end='')
        for (fw_start, fw_end, fw_partition) in _firmware_regions:
            if not forceFail and start >= fw_start and end <= fw_end:
                if _debug:
                    print('in', fw_partition)
                break
        else:   # Yes, else on for is cool
            if _debug:
                print('NO MATCH')
            msg = '%s: range %s is unavailable in FW' % (who, docstr)
            addError(msg)

###########################################################################
# The Librarian DB is the authoritative source of information.  The
# Librarian configuration file may be out of date WRT the Librarian
# database.  The Firmware information (only available on Superdome Flex
# (special firmware)) will represent the SDSM/FAM information on the
# running system.
# If there has been a change to the system (since the Librarian DB was
# setup), then it is possible that the Librarian is depending on memory
# that might not be available for the Librarian to use at this time.  Since
# this condition is detectable, save the user some potential problems.


def rangestr(start, end):
    units = 'MB'
    size = (end - start + 1) // 1024 // 1024
    assert end > start and size >= 2, 'Range too small'
    if size >= 1000:
        size //= 1024
        units = 'GB'
    if size >= 1000:
        size //= 1024
        units = 'TB'
    return '0x%016x - 0x%016x (%d %s)' % (start, end, size, units)


def check_all_ranges(db):
    global _firmware_regions

    # find all the possible SDSM/FAM memory based on the SDSM firmware (needs
    # to be FW bios.1_2_2_m_3.fd or later)

    (firmware_partitions, _firmware_regions) = scan_SDSM_UEFI()
    if firmware_partitions is None or _firmware_regions is None:
        # we are not on a Superdome Flex: return zero for no errors
        return 0

    # print the firmware information
    if _debug:
        print("Firmware partitions:", firmware_partitions)
        for (fw_start, fw_end, fw_partition) in _firmware_regions:
            print('Partition:range %s = %s' % (
                fw_partition, rangestr(fw_start, fw_end)))
        print("")

    # get the various regions
    librarian_regions = get_librarian_regions(db)
    if not librarian_regions:
        addError('Cannot retrieve librarian regions.')

    memmap_regions = get_proc_cmdline_regions()
    if not memmap_regions:
        addError('Cannot retrieve memmap regions.')

    tmfs_regions = get_tmfs_module_regions()
    if not tmfs_regions:
        print('Cannot retrieve tmfs regions.')

    if _errors:
        return _errors[:]   # a copy

    # Top-level comparisons.  More are needed...
    others = max(len(librarian_regions), len(memmap_regions), len(tmfs_regions))
    if (others > len(_firmware_regions) or
            len(librarian_regions) > len(memmap_regions) or
            len(librarian_regions) > len(tmfs_regions)):
        addError(
            'Region element mismatch: FW=%d, libr=%d, cmdline=%d, tmfs=%d.' % (
            len(_firmware_regions), len(librarian_regions),
            len(memmap_regions), len(tmfs_regions)))

    # insure all the regions fit inside the firmware ranges.
    misfits_in_firmware('Librarian', librarian_regions)
    misfits_in_firmware('cmdline memmap', memmap_regions)
    misfits_in_firmware('tmfs module', tmfs_regions)
    return _errors[:]   # a copy

###########################################################################


if __name__ == '__main__':

    if len(sys.argv) < 2:
        raise SystemExit("usage: %s <librarian-data-base>" % (sys.argv[0]))

    # If you import this file as a module, you need to do this in your
    # importer.
    db_file = sys.argv[1]
    try:
        # Using this instead of SQLite3assist gets higher level ops
        db = LibrarianDBackendSQLite3(Namespace(db_file=db_file))
    except Exception as e:
        raise SystemExit('Cannot open "%s": %s' % (db_file, str(e)))

    # Check the Librarian configuration versus the system address space setup.
    errors = check_all_ranges(db)
    db.close()
    if errors:
        if not _debug:
            print('Rerun this with DEBUG=1', file=sys.stderr)
        raise SystemExit(
            'Found %d memory range violation(s) against FW config:\n%s' % (
                (len(errors)), '\n'.join(errors)))
    print('No problems found.')
    raise SystemExit(0)
