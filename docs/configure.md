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

