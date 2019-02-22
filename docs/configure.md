## Configuring the Librarian

The Librarian uses an SQLite database to store the topology and state of the
cluster, namely
* The number and location of nodes
* How much FAM is hosted by each node
* Other information, mostly for true hardware

Recall that The Machine accepts a maximum of ten nodes in a single enclosure
and a maximum of four enclosures in one full rack.  Nodes 1-10 go in enclosure
1, 11-20 in enclosure 2, and so on.  The node population may be sparse.
The topology is statically described in a configuration file in 
[legacy "INI" format](https://en.wikipedia.org/wiki/INI_file).  

A very simple INI file follows; many values are extrapolated or defaulted
from this file.  This file uses values, especially book_size_bytes,
suitable for the FAME or Superdome Flex environments.

    [global]
    node_count = 4
    book_size_bytes = 8M
    bytes_per_node = 512B
    
This describes 4 nodes, numbered 1-4, given hostnames "node01" - "node04"
(a dense population of the first 4 slots in a single enclosure #1).
There are 512 books hosted by each node for a total of 4G of FAM per node,
or 16G total.  Here is the same effect done in a more explicit form:

    [global]
    node_count = 4
    book_size_bytes = 8M

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

In general you should keep the node_id and [hostname] in sync as shown.
Also, use "node" as the base of the hostname.

With this notation different nodes can have different memory sizes, 
populations can be sparse, etc.  There are indeed NUMA considerations on the
actual hardware [which you can read about here.](allocation.md)

The useful set of recognized INI keywords (others are reserved for true
instances of The Machine):

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

The database holding book metadata can be created using the setup script
book_register.py.  If running from source, it's at "src/book_register.py";
from an installed package, it's "/usr/bin/tm-book_register".

By default the database is expected to be found at /var/hpetm/librarian.db:

    sudo mkdir -p /var/hpetm
    sudo src/book_register.py -f -d /var/hpetm/librarian.db myconfig.ini

## Examining the database

It's possible to look at the database on the system hosting the file.
You can examine it directly with the "sqlitebrowser", or use some convenience
routines for a higher-level snapshot.  They are included in the source but
not installed (as of this writing).

Run "sudo src/lslfs --install" (the file is at /usr/bin/python3.x/dist-packages/tm_librarian/src when installed from a Debian pacakge).  This creates five
commands in /usr/local/bin which examine the database in $DBPATH 
(defaults to /var/hpetm/librarian.db):

|Script|Function|
|:-----|:-------|
|lslfs |A simple listing of all files and directories|
|lllfs |A more detailed listing|
|dflfs |Free disk space per the "df(1)" command|
|dulfs |Used disk space per the "du(1)" command|
|alloclfs|An extensive usage report: book assignments per file and books used by node|

