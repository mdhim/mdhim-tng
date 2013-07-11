#include <stdlib.h>
#include <leveldb/c.h>
#include "ds_leveldb.h"

/**
 * mdhim_leveldb_open
 * Opens the database
 *
 * @param dbh            in   ** to the leveldb handle
 * @param path           in   path to the database file
 * @param flags          in   flags for opening the data store
 * @param mstore_opts    in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_leveldb_open(void **dbh, char *path, int flags, struct mdhim_store_opts_t *mstore_opts) {
	leveldb_t *db;
	leveldb_options_t *options;
	char *err = NULL;
	
	//Create the options
	options = leveldb_options_create();
	leveldb_options_set_create_if_missing(options, 1);
	//Open the database
	db = leveldb_open(options, path, &err);
	//Set the output handle
	*dbh = db;

	if (err != NULL) {
		mlog(MDHIM_SERVER_CRIT, "Error opening leveldb database");
		return MDHIM_DB_ERROR;
	}

	//Reset error variable
	leveldb_free(err); 

	//Destroy the options
	leveldb_options_destroy(options);

	return MDHIM_SUCCESS;
}

/**
 * mdhim_leveldb_put
 * Stores a single key in the data store
 *
 * @param dbh         in   pointer to the leveldb handle
 * @param key         in   void * to the key to store
 * @param key_len     in   length of the key
 * @param data        in   void * to the value of the key
 * @param data_len    in   length of the value data 
 * @param mstore_opts in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_leveldb_put(void *dbh, void *key, int key_len, void *data, int32_t data_len, 
		      struct mdhim_store_opts_t *mstore_opts) {
    leveldb_writeoptions_t *options;
    char *err = NULL;
    leveldb_t *db = (leveldb_t *) dbh;

    options = leveldb_writeoptions_create();
    leveldb_put(db, options, key, key_len, data, data_len, &err);
    if (err != NULL) {
	    mlog(MDHIM_SERVER_CRIT, "Error putting key/value in leveldb");
	    return MDHIM_DB_ERROR;
    }

    //Reset error variable
    leveldb_free(err); 
    
    //Destroy the options
    leveldb_writeoptions_destroy(options);

    return MDHIM_SUCCESS;
}

/**
 * mdhim_leveldb_get
 * Gets a value, given a key, from the data store
 *
 * @param dbh          in   pointer to the leveldb db handle
 * @param key          in   void * to the key to retrieve the value of
 * @param key_len      in   length of the key
 * @param data         out  void * to the value of the key
 * @param data_len     out  pointer to length of the value data 
 * @param mstore_opts  in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_leveldb_get(void *dbh, void *key, int key_len, void **data, int32_t *data_len, 
		      struct mdhim_store_opts_t *mstore_opts) {
	leveldb_readoptions_t *options;
	char *err = NULL;
	leveldb_t *db = (leveldb_t *) dbh;
	int ret = MDHIM_SUCCESS;

	options = leveldb_readoptions_create();
	*((char **) data) = NULL;
	*((char **) data) = leveldb_get(db, options, key, key_len, (size_t *) data_len, &err);
	if (err != NULL) {
		mlog(MDHIM_SERVER_CRIT, "Error getting value in leveldb");
		return MDHIM_DB_ERROR;
	}
	if (!*((char **) data)) {
		ret = MDHIM_DB_ERROR;
	}

	//Reset error variable
	leveldb_free(err); 
	
	//Destroy the options
	leveldb_readoptions_destroy(options);
	
	return ret;
}

int mdhim_leveldb_get_next(void *dbh, void *curh, void **key, int *key_len, 
			   void **data, int32_t *data_len, 
			   struct mdhim_store_cur_opts_t *mstore_cur_opts) {
	return MDHIM_SUCCESS;
}

int mdhim_leveldb_get_prev(void *dbh, void *curh, void **key, int *key_len, 
			   void **data, int32_t *data_len, 
			   struct mdhim_store_cur_opts_t *mstore_cur_opts) {
	return MDHIM_SUCCESS;
}

void *mdhim_leveldb_cursor_init(void *dbh) {
	return NULL;
}

int mdhim_leveldb_cursor_release(void *dbh, void *curh) {
	return MDHIM_SUCCESS;

}

/**
 * mdhim_leveldb_close
 * Closes the data store
 *
 * @param dbh         in   pointer to the leveldb db handle 
 * @param mstore_opts in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_leveldb_close(void *dbh, struct mdhim_store_opts_t *mstore_opts) {
	leveldb_t *db = (leveldb_t *) dbh;

	leveldb_close(db);
	return MDHIM_SUCCESS;
}

/**
 * mdhim_leveldb_del
 * delete the given key
 *
 * @param dbh         in   pointer to the leveldb db handle
 * @param key         in   void * for the key to delete
 * @param key_len     in   int for the length of the key
 * @param mstore_opts in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_leveldb_del(void *dbh, void *key, int key_len, 
		      struct mdhim_store_opts_t *mstore_opts) {
	leveldb_writeoptions_t *options;
	char *err = NULL;
	leveldb_t *db = (leveldb_t *) dbh;
	
	options = leveldb_writeoptions_create();
	leveldb_delete(db, options, key, key_len, &err);
	if (err != NULL) {
		mlog(MDHIM_SERVER_CRIT, "Error deleting key in leveldb");
		return MDHIM_DB_ERROR;
	}

	//Reset error variable
	leveldb_free(err); 
	
	//Destroy the options
	leveldb_writeoptions_destroy(options);

	return MDHIM_SUCCESS;
}

/**
 * mdhim_leveldb_commit
 * Commits outstanding writes the data store
 *
 * @param dbh         in   pointer to the leveldb handle 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_leveldb_commit(void *dbh) {
	return MDHIM_SUCCESS;
}
