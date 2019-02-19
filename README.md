# The Librarian File System (LFS) Suite

The Machine from HPE consists of a set of compute nodes that each host a portion of Fabric-Attached Memory (FAM).  The FAM of all nodes is combined into a global pool visible from any node.  In addition, a separate Top-of-Rack-Management Server (ToRMS) oversees and controls the hardware cluster.  Management of the FAM is behind the Linux file system API via a custom file system, LFS: the Librarian File System.  The name comes from the granularity of The Machine FAM: eight gigabyte "books" (contiguous sets of memory pages).  LFS manages books and collections of books between all the nodes.

The Librarian is an application that runs on the TORMS and manages the metadata associated with LFS. The Librarian sees only metadata, never any actual book contents in FAM.  The Librarian keeps track of the cluster topology and book allocations in an SQL database, and communicates with a daemon on every node.

Each node presents the LFS to its Linux instance via a (file system) daemon. The LFS daemon (named lfs_fuse.py) manages the overall metadata by communicating with the Librarian over LAN.  As metadata operations should be a small fraction of operation, speed is not a primary concern.   Once metadata is established, the LFS daemon handles familiar open/close/truncate/read/write calls, and most specifically, mmap().   This is the real power of Fabric-Attached Memory: directly mapping FAM persistent storage into user process space.

The Librarian is used in several environments:

- Real Hardware with FAM behind a prototype of Gen-Z fabric.  A Top of Rack Server is present.
- Fabric-Attached Memory Emulation (FAME) (simulated FAM using QEMU VMs as nodes and IVSHMEM)
- Future hardware (as experimental/demonstration concept)
- HPE proprietary simulator for The Machine (aka TMAS)

## Configuring the Librarian
The Librarian needs a database which stores knows the topology and state of the cluster, namely
* The number and (FAM NUMA) location of nodes
* How much FAM is hosted by each node
* Other optional information, mostly for true hardware

The Machine organizes a maximum of ten nodes in a single enclosure, and a maximum of four enclosures in one instance.  Nodes 1-10 go in enclosure 1, 11-20 in enclosure 2, and so on.  The node population may be sparse.  Any topology is described in a configuration file in [legacy "INI" format](https://en.wikipedia.org/wiki/INI_file).  A very simple INI file follows; many values are extrapolated or defaulted from this file.

    [global]
    node_count = 4
    book_size_bytes = 8G
    bytes_per_node = 512B
    
This describes 4 nodes, numbered 1-4, given hostnames "node01" - "node04" (a dense population of the first 4 slots in a single enclosure #1).  There are 512 books hosted by each node for a total of 4T of FAM per node, or 16T total.  Here is the same effect done in a more explicit form:

    [global]
    node_count = 4
    book_size_bytes = 8G

    [node01]
    node_id = 1
    nvm_size = 512B

    [node02]
    node_id = 2
    nvm_size = 512B

    [node03]
    node_id = 3
    nvm_size = 512B

    [node04]
    node_id = 4
    nvm_size = 512B

With this notation different nodes can have different memory sizes, node populations can be sparse, etc.  There are indeed NUMA considerations on the actual hardware but that is beyond the scope of this discussion.  The full set of recognized keywords:

* [global]        - (STR) global section name
* node_count      - (INT) total number of nodes
* book_size_bytes - (INT) book size (M = Megabytes and G = Gigabytes)
* [node##]        - (STR) unique section name which doubles as the hostname of Linux on the SoC; not needed for short form when nvm_size_per_node is given
* node_id         - (INT) unique global node ID for SoC (limit: 1-40, determines its location)
* nvm_size        - (INT) total size of NVM hosted by node
* bytes_per_node  - (INT) total NVM per node in bytes

book_size_bytes and nvm_size also support suffix multipliers:

* M = MB (size * 1024 * 1024)
* G = GB (size * 1024 * 1024 * 1024)
* T = TB (size * 1024 * 1024 * 1024 * 1024)
* B = books (nvm_size only)    

Finally, the database holding book metadata can be created using the setup script book_register.py.  By default the database is expected to be found at /var/hpetm/librarian.db:

    sudo mkdir -p /var/hpetm
    sudo book_register.py -f -d /var/hpetm/librarian.db myconfig.ini

But first, the code needs to be downloaded somehow.

## Obtaining the Librarian

The nodes run an image whose creation is done elsewhere...The Librarian runs on the ToRMS on real hardware so its installation is handled via proprietary means.  If the setup is FAME, there are two major cases of the QEMU/KVM host:

1. "mostly Stretch" host (Debian 9.x, Ubuntu 16.04 or later)
1. non-Debian host (CentOS or SLES)

### Debian-based FAME host

1. [Run it from source](https://github.com/FabricAttachedMemory/tm-librarian)
2. Pull down packages from [the public repo](https://downloads.linux.hpe.com/SDR/repo/l4fame/) and dpkg -i
    1. python3-tm-librarian.deb
    1. tm-librarian.deb

### Non-Debian FAME host

[Get the container](https://github.com/FabricAttachedMemory/librarian-container) and run it according to instructions found on that website.

## Usage

To run, simply run the librarian.py and give it the location of the database file (if not using the above default):

    ./librarian.py /path/to/librarian.db

## Librarian Server

Script | Function
-------------|--------------
Librarian.py|main librarian module
backend_sqlite3.py|sqlite3 interface module
sqlassist.py|generic sql database helper
engine.py|process supported librarian commands
book_shelf_bos.py|book, shelf, bos class and methods
cmdproto.py|command definition for server and client
genericobj.py|base object class with dictionary helper
socket_handling.py|server/client socket handling
librarian_chain.py|chain conversion functions for librarian
function_chain.py|generic function chain helper

## Librarian FUSE interface

This interface will be used by applications to allocate NVM, it indirectly
interfaces with the Librarian to manage NVM related operations. It can also
be used interactively to test or query the Librarian and to pre-initialize
NVM for use by an application.

Script | Function
-------|---------
lfs_fuse.py|main librarian filesystem interface module
book_shelf_bos.py|book, shelf, bos class and methods
cmdproto.py|command definition for server and client
lfs_shadow.py|common support routines encapsulated by different classes for different execution environments

## Book Allocation Policies

There is some NUMA-awareness behind the (book) allocation algorithm used
for sizing a file.  The default allocation policy randomly allocates books
all nodes that still have books available.  The default policy is an
extended file attribute of the LFS mount point on nodes, typically /lfs.
See the man pages for attr(5), xattr(7), getfattr(1), setfattr(1) and
related system calls for programmatic use.


```
$ getfattr -n user.LFS.AllocationPolicyDefault /lfs
getfattr: Removing leading '/' from absolute path names
# file: lfs
user.LFS.AllocationPolicyDefault="RandomBooks"
```

When a file is created the default policy is assigned for space allocation
for that file.  The file's current policy can be changed at any time;
future allocations follow the current policy.
Books are deallocated from a file on a LIFO basis (ie, simple truncation).

```
$ touch /lfs/newfile
$ getfattr -d /lfs/newfile
getfattr: Removing leading '/' from absolute path names
# file: lfs/newfile
user.LFS.AllocationPolicy="RandomBooks"
user.LFS.AllocationPolicyList="RandomBooks,LocalNode,LocalEnc,NonLocal_Enc,Nearest,NearestRemote,NearestEnc,NearestRack,LZAascending,LZAdescending,RequestIG"
user.LFS.Interleave
user.LFS.InterleaveRequest
user.LFS.InterleaveRequestPos
```

Policies are best understood with respect to the [topology of The Machine](https://github.com/FabricAttachedMemory/Emulation/wiki):
* A single node contains FAM considered to be locally-attached to its SoC.
* Up to ten nodes fit in one enclosure which has a fabric switch joining all the FAM across its nodes.  Nodes 1-10 are in enclosure 1, 11-20 in enclosure 2, etc.
* Up to four chassis make up a rack with a Top-of-Rack switch joining all the the enclosures.

From a NUMA standpoint,
1. Local FAM is "one-hop" (a fabric bridge exists between the SoC and FAM)
2. Intra-enclosure FAM is three hops (bridgeA-enclosure switch-bridgeB)
3. Inter-enclosure is five hops (bridgeA-encA switch-ToR switch-encB switch-bridgeB)

Each bridge/switch incurs a few percent of latency delay.

Consider a half-full 20 node system with enclosures 1 and 2 fully populated,
and an application running on node 15 (ie, in enclosure 2).  Here's how
the allocation policies pan out.  Allocations that run out of space will
return an error.

Policy | Description | Candidate nodes
-------|-------------|----------------
RandomBooks | Any node with free books, non-weighted choice | 1-20
LocalNode| Only the node running the application | 15
LocalEnc| Nodes in the same enclosure | 11-20
NonLocal_Enc| Nodes in the same enclosure except this node | 11-14 &amp; 16-20
Nearest| Start with LocalNode until it's full, then the local enclosure, then anywhere| 15, then 11-14 &amp; 16-20, finally 1-10
NearestEnc|Like Nearest, but skip LocalNode|11-14 &amp; 16-20, finally 1-10
NearestRack|Not in this enclosure | 1-10
NearestRemote|Not this node| 1-14 &amp; 16-20
RequestIG|Follow a specific list of nodes, with wraparound|See following text
LZAascending|Ordered blocks from lowest node to highest, useful in Librarian development| 1, 2, 3, ... 19, 20
LZAdescending| Reverse of LZAascending| 20, 19, 18, ... 2, 1

RequestIG must be followed by another attribute with the list.  This list
is one byte per book, numbering starts at zero, and is repeated as needed.
For example, to allocate books for a file from only nodes 3 and 4,

```
$ touch /lfs/only3and4
$ setfattr -n user.LFS.InterleaveRequest -v 0x0203 /lfs/only3and4  
$ getfattr -d /lfs/only3and4
getfattr: Removing leading '/' from absolute path names
# file: lfs/only3and4
user.LFS.AllocationPolicy="RequestIG"
user.LFS.AllocationPolicyList="RandomBooks,LocalNode,LocalEnc,NonLocal_Enc,Nearest,NearestRemote,NearestEnc,NearestRack,LZAascending,LZAdescending,RequestIG"
user.LFS.Interleave
user.LFS.InterleaveRequest=0sAgM=
user.LFS.InterleaveRequestPos="0"
```

Interleave variables are encoded in hex, use the -e hex arguments.
InterleaveRequestPos is the index in the list of the next node to use for
allocation; usually it's left alone.  After allocating 5 books for the file,
look at the actual allocation:

```
$ getfattr -e hex -d /lfs/only3and4 | grep Interleave
user.LFS.Interleave=0x0203020302
user.LFS.InterleaveRequest=0x0203
user.LFS.InterleaveRequestPos=0x31
```

This data can also be seen on the ToRMS by examining the SQLite database at
the heart of the Librarian.  There are five wrapper scripts to assist with
this: lslfs, lllfs, dulfs, dflfs, and alloclfs.
