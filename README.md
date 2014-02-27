MDHIM - Multi-Dimensional Hashing Indexing Middleware


Description
---------------
MDHIM is a parallel key/value store framework written in MPI.  
Unlike other big data solutions, MDHIM has been designed for an HPC 
environment and to take advantage of high speed networks.


Requirements
---------------
Leveldb: http://code.google.com/p/leveldb/
Mysqldb: http://www.mysql.com/
Mysqldb C Connector: http://dev.mysql.com/downloads/connector/c/
An MPI distribution that supports MPI_THREAD_MULTIPLE well (this excludes OpenMPI)


Building the Library
---------------
1. Modify Makefile.cfg to point to your leveldb and MPI installations
2. Type: make
3. If all went well, you have the library in src/libmdhim.a


Building the Tests
---------------
1. cd tests
2. Type: make
3. If all went well, you have all the tests compiled

