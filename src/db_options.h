/*
 * DB usage options. 
 * Location and name of DB, type of DataSotre primary key type,
 */

#ifndef      __OPTIONS_H
#define      __OPTIONS_H

// Options for the database (used when opening a MDHIM dataStore)
typedef struct db_options_t {
    // -------------------
    //Directory location of DBs
    char *db_path;
    
    //Name of each DB (will be modified by adding "_<RANK>" to create multiple
    // unique DB for each rank server.
    char *db_name;
    
    //Different types of dataStores
    //UNQLITE=1, LEVELDB=2 (from data_store.h)
    int db_type;
    
    //Primary key type
    //MDHIM_INT_KEY=1, MDHIM_LONG_INT_KEY=2, MDHIM_FLOAT_KEY=3, MDHIM_DOUBLE_KEY=4
    //MDHIM_LONG_DOUBLE_KEY=5, MDHIM_STRING_KEY=6, MDHIM_BYTE_KEY=7
    //(from partitioner.h)
    int db_key_type;

    //Force the creation of a new DB (deleting any previous versions if present)
    int db_create_new;

} db_options_t;

void db_options_set_path(struct db_options_t* opts, char *path);
void db_options_set_name(struct db_options_t* opts, char *name);
void db_options_set_type(struct db_options_t* opts, int type);
void db_options_set_key_type(struct db_options_t* opts, int key_type);
void db_options_set_create_new(struct db_options_t* opts, int create_new);

#endif
