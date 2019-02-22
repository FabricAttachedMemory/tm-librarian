## Allocation Policy

There is some NUMA-awareness behind the (book) allocation algorithm used
for sizing a file.  The default allocation policy randomly allocates books
from all nodes that still have books available.  The default policy is an
extended file attribute of the LFS mount point on nodes, typically /lfs.
See the man pages for attr(5), xattr(7), getfattr(1), setfattr(1) and
related system calls for programmatic use.  To see the current default
policy on your system, log into a node and execute...


```
$ getfattr -n user.LFS.AllocationPolicyDefault /lfs
getfattr: Removing leading '/' from absolute path names
# file: lfs
user.LFS.AllocationPolicyDefault="RandomBooks"
```

When a file is created the default policy becomes the starting policy
for that file.  The file's policy can be changed at any time;
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

Consider a 20-node system with enclosures 1 and 2 fully populated (nodes
1-20 exist).  There is an application running on node 15 (in enclosure 2).
Here's how the allocation policies pan out.  Allocations that run out of space
will return an ENOSPC error.

Policy | Description | Candidate nodes
-------|-------------|:---------------
RandomBooks | Any node with free books | 1-20
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
