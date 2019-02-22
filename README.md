# The Librarian File System (LFS) Suite

The Machine from HPE consists of a set of compute nodes that each host a portion of Fabric-Attached Memory (FAM).  The FAM of all nodes is combined into a global pool visible from any node.  In addition, a separate Top-of-Rack-Management Server (ToRMS) oversees and controls the hardware cluster.  Management of the FAM is behind the Linux file system API via a custom file system, LFS: the Librarian File System.  The name comes from the granularity of The Machine FAM: eight gigabyte "books" (contiguous sets of memory pages).  LFS manages books and collections of books between all the nodes.

The Librarian is an application that runs on the TORMS and manages the metadata associated with LFS. The Librarian sees only metadata, never any actual book contents in FAM.  The Librarian keeps track of the cluster topology and book allocations in an SQL database, and communicates with a daemon on every node.

Each node presents the LFS to its Linux instance via a (file system) daemon. The LFS daemon (named lfs_fuse.py) manages the overall metadata by communicating with the Librarian over LAN.  As metadata operations should be a small fraction of operation, speed is not a primary concern.   Once metadata is established, the LFS daemon handles familiar open/close/truncate/read/write calls, and most specifically, mmap().   This is the real power of Fabric-Attached Memory: directly mapping FAM persistent storage into user process space.

The Librarian File System suite is used in several environments:

- Real Hardware with FAM behind a prototype of Gen-Z fabric.  A separate Linux-based Top-of-Rack Management Server (ToRMS) is present to run support software.
- [Fabric-Attached Memory Emulation "FAME"](https://github.com/FabricAttachedMemory/Emulation) (emulated FAM using QEMU VMs as nodes)
- HPE SuperDome Flex

## The Machine topology and NUMA considerations

The remaining discussions are best understood with respect to the [topology of The Machine](https://github.com/FabricAttachedMemory/Emulation/wiki):
* A single node contains FAM considered to be locally-attached to its SoC.
* Up to ten nodes fit in one enclosure which has a fabric switch joining all the FAM across its nodes.  Nodes 1-10 are in enclosure 1, 11-20 in enclosure 2, etc.
* Up to four enclosures make up a full rack with a Top-of-Rack switch joining all the the enclosures.
* A maximum of 40 nodes are supported.  Enclosures may be sparsely populated.

|FAM location | Hops to CPU | Path |
|-------------|:-----------:|:-----|
|Local FAM|1|single fabric bridge|
|Intra-enclosure|3|bridgeA - enclosure switch - bridgeB|
|Inter-enclosure|5|bridgeA - encA switch - ToR switch - encB switch - bridgeB|

Each bridge/switch incurs a few percent of latency delay.

## Obtaining the Librarian

The source in this repo includes Librarian server code plus the client code
that runs on nodes.  Configuring the nodes (with appropriate kernel) is 
beyond the scope of this document.  [See the "Emulation" repo for details.](https://github.com/FabricAttachedMemory/Emulation)

The Librarian must run on a system reachable via LAN from all nodes.  The
Machine cluster runs the Librarian on its ToRMS.  Any system that runs 
Python 3 and SQLite will work (Linux is suggested :-)

1. You can run programs directly from the "src" directory of this repo.
1. Pull down Debian packages from [the public HPE SDR repo](https://downloads.linux.hpe.com/SDR/repo/l4fame/) and dpkg -i
    1. python3-tm-librarian.deb
    1. tm-librarian.deb
1. [There is a Docker container](https://github.com/FabricAttachedMemory/librarian-container).  Run it according to instructions found on that website.

## Configuring the Librarian

[This is the hard part and is covered separately](docs/configure.md).

## Usage

To run, simply run the librarian.py and give it the location of the database file (if not using the above default):

    $ sudo src/librarian.py /path/to/librarian.db

Or, if you managed to install the Debian package,

    $ sudo systemctl start tm-librarian

## Book Allocation Policies

[This topic is also covered in a separate page.](docs/allocation.md)
The nodes must be booted and running the client code that is connected
to a Librarian server process.

## Source file reference

### For the (ToRMS) server

Script | Function
-------------|--------------
librarian.py|main Librarian module
backend_sqlite3.py|sqlite3 interface module
sqlassist.py|generic sql database helper
engine.py|process supported librarian commands
book_shelf_bos.py|book, shelf, bos class and methods
cmdproto.py|command definition for server and client
genericobj.py|base object class with dictionary helper
socket_handling.py|server/client socket handling
librarian_chain.py|chain conversion functions for librarian
function_chain.py|generic function chain helper

### For the node/clients


Script | Function
-------|---------
lfs_fuse.py|main daemon filesystem interface module
book_shelf_bos.py|book, shelf, bos class and methods
cmdproto.py|command definition for server and client
lfs_shadow.py|common support routines encapsulated by different classes for different execution environments

