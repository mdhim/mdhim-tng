/*
 * DB usage options. 
 * Location and name of DB, type of DataSotre primary key type,
 */

#ifndef      __OPTIONS_H
#define      __OPTIONS_H

#include <stdint.h>

/* Append option */
#define MDHIM_DB_OVERWRITE 0
#define MDHIM_DB_APPEND 1

// Options for the database (used when opening a MDHIM dataStore)
typedef struct mdhim_options_t {
	// -------------------
	//Directory location of DBs
	char *db_path;
   
	//Name of each DB (will be modified by adding "_<RANK>" to create multiple
	// unique DB for each rank server.
	char *db_name;
    
	//Different types of dataStores
	//LEVELDB=1 (from data_store.h)
	int db_type;
    
	//Primary key type
	//MDHIM_INT_KEY, MDHIM_LONG_INT_KEY, MDHIM_FLOAT_KEY, MDHIM_DOUBLE_KEY
	//MDHIM_STRING_KEY, MDHIM_BYTE_KEY
	//(from partitioner.h)
	int db_key_type;

	//Force the creation of a new DB (deleting any previous versions if present)
	int db_create_new;

	//Whether to append a value to an existing key or overwrite the value
	//MDHIM_DB_APPEND to append or MDHIM_DB_OVERWRITE (default)
	int db_value_append;
        
	//DEBUG level
	int debug_level;
        
        //Used to determine the number of range servers which is based in  
        //if myrank % rserver_factor == 0, then myrank is a server.
        // This option is used to set range_server_factor previously a defined var.
        int rserver_factor;
        
        //Maximum size of a slice. A ranger server may server several slices.
        uint64_t max_recs_per_slice; 

} mdhim_options_t;

struct mdhim_options_t* mdhim_options_init();
void mdhim_options_set_db_path(struct mdhim_options_t* opts, char *path);
void mdhim_options_set_db_name(struct mdhim_options_t* opts, char *name);
void mdhim_options_set_db_type(struct mdhim_options_t* opts, int type);
void mdhim_options_set_key_type(struct mdhim_options_t* opts, int key_type);
void mdhim_options_set_create_new_db(struct mdhim_options_t* opts, int create_new);
void mdhim_options_set_debug_level(struct mdhim_options_t* opts, int dbug);
void mdhim_options_set_value_append(struct mdhim_options_t* opts, int append);
void mdhim_options_set_server_factor(struct mdhim_options_t* opts, int server_factor);
void mdhim_options_set_max_recs_per_slice(struct mdhim_options_t* opts, uint64_t max_recs_per_slice);
void mdhim_options_destroy(struct mdhim_options_t *opts);

#endif
