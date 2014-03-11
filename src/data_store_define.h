/* Storage Methods */
#define LEVELDB 1 //LEVELDB storage method
#define MYSQLDB 3
#define ROCKSDB 4 //RocksDB
/* mdhim_store_t flags */
#define MDHIM_CREATE 1 //Implies read/write 
#define MDHIM_RDONLY 2
#define MDHIM_RDWR 3

/* Keys for stats database */
#define MDHIM_MAX_STAT 1
#define MDHIM_MIN_STAT 2
#define MDHIM_NUM_STAT 3
