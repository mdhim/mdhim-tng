#include <stdlib.h>
#include <linux/limits.h>
#include <stdio.h>
#include "ds_unqlite.h"

/**
 * get_unqlite_err_msg
 *
 * Gets the error message from unqlite
 * @param dh  in   pointer to the unqlite db handle
 * @return the error message string
 */
static char *get_unqlite_err_msg(unqlite *dh) {
	char *zBuf;
	int iLen;
	/* Something goes wrong, extract database error log */
	unqlite_config(dh, UNQLITE_CONFIG_ERR_LOG,  &zBuf,
		       &iLen);
	if( iLen > 0 ){
		return zBuf;
	}  

	return NULL;
}

/**
 * print_unqlite_err_msg
 *
 * prints the error message from unqlite using mlog
 * @param dh  in   pointer to the unqlite db handle
 */
static void print_unqlite_err_msg(unqlite *dh) {
	char *msg;

	mlog(MDHIM_SERVER_CRIT, "Unqlite error");	
	msg = get_unqlite_err_msg(dh);
	if (!msg) {
		return;
	}
	
	mlog(MDHIM_SERVER_CRIT, "Unqlite error: %s", msg);
}

/**
 * mdhim_unqlite_open
 * Opens the database
 *
 * @param dbh            in   ** to the unqlite db handle
 * @param path           in   path to the database file
 * @param flags          in   flags for opening the data store
 * @param mstore_opts    in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_unqlite_open(void **dbh, void **dbs, char *path, int flags, 
		       struct mdhim_store_opts_t *mstore_opts) {
	int ret = 0;
	int imode;
	unqlite **dh = (unqlite **) dbh;
	char stats_path[PATH_MAX];

	//Convert the MDHIM flags to unqlite ones
	switch(flags) {
	case MDHIM_CREATE:
		imode = UNQLITE_OPEN_CREATE;
		break;
	case MDHIM_RDONLY:
		imode = UNQLITE_OPEN_READONLY;
		break;
	case MDHIM_RDWR:
		imode = UNQLITE_OPEN_READWRITE;
		break;
	default:
		break;
	}
	
	//Open the main database
	if ((ret = unqlite_open(dh, path, imode)) != UNQLITE_OK) {
		print_unqlite_err_msg(*dh);
		return MDHIM_DB_ERROR;
	}

	dh = (unqlite **) dbs;
	//Check to see if the given path + "_stat" and the null char will be more than the max
	if (strlen(path) + 6 > PATH_MAX) {
		mlog(MDHIM_SERVER_CRIT, "Error opening leveldb database - path provided is too long");
		return MDHIM_DB_ERROR;
	}
	sprintf(stats_path, "%s_stats", path);

	//Open the stats database
	if ((ret = unqlite_open(dh, path, UNQLITE_OPEN_CREATE)) != UNQLITE_OK) {
		print_unqlite_err_msg(*dh);
		return MDHIM_DB_ERROR;
	}

	return MDHIM_SUCCESS;
}

/**
 * mdhim_unqlite_put
 * Stores a single key in the data store
 *
 * @param dbh         in   pointer to the unqlite db handle
 * @param key         in   void * to the key to store
 * @param key_len     in   length of the key
 * @param data        in   void * to the value of the key
 * @param data_len    in   length of the value data 
 * @param mstore_opts in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_unqlite_put(void *dbh, void *key, int key_len, void *data, int32_t data_len, 
		      struct mdhim_store_opts_t *mstore_opts) {
	unqlite *dh = (unqlite *) dbh;
	int ret = 0;
	int64_t dlen = data_len;
	
	if ((ret = unqlite_kv_append(dh, key, key_len, data, dlen)) != UNQLITE_OK) {
		print_unqlite_err_msg(dh);
		return MDHIM_DB_ERROR;
	}

	return MDHIM_SUCCESS;
}

/**
 * mdhim_unqlite_del
 * delete the given key
 *
 * @param dbh         in   pointer to the unqlite db handle
 * @param key         in   void * for the key to delete
 * @param key_len     in   int for the length of the key
 * @param mstore_opts in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_unqlite_del(void *dbh, void *key, int key_len, 
		      struct mdhim_store_opts_t *mstore_opts) {
	unqlite *dh = (unqlite *) dbh;
	int ret = 0;

	if ((ret = unqlite_kv_delete(dbh, key, key_len)) != UNQLITE_OK) {
		print_unqlite_err_msg(dh);
		return MDHIM_DB_ERROR;
	}

	return MDHIM_SUCCESS;
}

/**
 * mdhim_unqlite_get
 * Gets a value, given a key, from the data store
 *
 * @param dbh          in   pointer to the unqlite db handle
 * @param key          in   void * to the key to retrieve the value of
 * @param key_len      in   length of the key
 * @param data         out  void * to the value of the key
 * @param data_len     out  pointer to length of the value data 
 * @param mstore_opts  in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_unqlite_get(void *dbh, void *key, int key_len, void **data, int32_t *data_len, 
		      struct mdhim_store_opts_t *mstore_opts) {
	unqlite *dh = (unqlite *) dbh;
	int ret = 0;
	unqlite_int64 nbytes = 0;

	*((char **) data) = NULL;
	*data_len = 0;

	//Extract data size first
	if ((ret = unqlite_kv_fetch(dh, key, key_len, NULL, &nbytes)) != UNQLITE_OK) {
		print_unqlite_err_msg(dh);
		return MDHIM_DB_ERROR;
	}
	
        //Allocate the buffer to the value's size
	*((char **) data) = malloc(nbytes);

        //Copy record content in our buffer
	if ((ret = unqlite_kv_fetch(dh, key, key_len, *((char **) data), 
				    &nbytes)) != UNQLITE_OK) {
		print_unqlite_err_msg(dh);
		return MDHIM_DB_ERROR;
	}
	
	//Set the output arguments
	*data_len = (int32_t) nbytes;

	return MDHIM_SUCCESS;
}

/**
 * mdhim_unqlite_get_next
 * Gets the next key/value from the data store
 *
 * @param dbh             in   pointer to the unqlite db handle
 * @param curh            in   pointer to the db cursor
 * @param key             out  void ** to the key that we get
 * @param key_len         out  int * to the length of the key 
 * @param data            out  void ** to the value belonging to the key
 * @param data_len        out  int * to the length of the value data 
 * @param mstore_opts in   additional cursor options for the data store layer 
 * 
 */
int mdhim_unqlite_get_next(void *dbh, void **key, int *key_len, 
			   void **data, int32_t *data_len, 
			   struct mdhim_store_opts_t *mstore_opts) {
/*	unqlite *dh = (unqlite *) dbh;
	int ret = 0;
	unqlite_kv_cursor *cur;	
	char *data_buf;
	char *key_buf;
*/

	/*
	*key = NULL;
	*key_len = 0;
	*data = NULL;
	*data_len = 0;


        //Make sure the cursor isn't empty
	if (!cur) {
		mlog(MDHIM_SERVER_INFO, "Cursor empty");
		return MDHIM_DB_ERROR;
	}

	//Check if this a valid entry
	if ((ret = unqlite_kv_cursor_valid_entry(cur)) != UNQLITE_OK) {
		mlog(MDHIM_SERVER_INFO, "Next entry not found");
		return MDHIM_DB_ERROR;
	}

	//Change to seek later
	if ((ret = unqlite_kv_cursor_next_entry(cur)) != UNQLITE_OK) {
		print_unqlite_err_msg(dh);
		return MDHIM_DB_ERROR;
	}
	//Find the size of the key
	if ((ret = unqlite_kv_cursor_key(cur, NULL, key_len)) != UNQLITE_OK) {
		print_unqlite_err_msg(dh);
		return MDHIM_DB_ERROR;
	}
	
	//Get the key
	key_buf = malloc(*key_len);
	if ((ret = unqlite_kv_cursor_key(cur, key_buf, key_len)) != UNQLITE_OK) {
		print_unqlite_err_msg(dh);
		return MDHIM_DB_ERROR;
	}      
	*key = key_buf;

	//Find the size of the data
	if ((ret = unqlite_kv_cursor_data(cur, NULL, (unqlite_int64 *) data_len)) 
	    != UNQLITE_OK) {
		print_unqlite_err_msg(dh);
		return MDHIM_DB_ERROR;
	}
	
	//Get the data
	data_buf = malloc(*data_len);
	if ((ret = unqlite_kv_cursor_data(cur, data_buf, (unqlite_int64 *) data_len)) 
	    != UNQLITE_OK) {
		print_unqlite_err_msg(dh);
		return MDHIM_DB_ERROR;
	}      
	*data = data_buf;
	*/

	return MDHIM_SUCCESS;
}


/**
 * mdhim_unqlite_get_prev
 * Gets the next key/value from the data store
 *
 * @param dbh             in      pointer to the unqlite db handle
 * @param key             in/out  void ** to the last key to start from and void** 
                                  to the key that we get back
 * @param key_len         int/out int * to the length of the key to start from and
                                  to the key that we get back
 * @param data            out     void ** to the value belonging to the key
 * @param data_len        out     int * to the length of the value data 
 * @param mstore_opts in     additional cursor options for the data store layer 
 * 
 */
int mdhim_unqlite_get_prev(void *dbh, void **key, int *key_len, 
			   void **data, int32_t *data_len, 
			   struct mdhim_store_opts_t *mstore_opts) {
/*	unqlite *dh = (unqlite *) dbh;
	int ret = 0;
	unqlite_kv_cursor *cur;	
	char *data_buf;
	char *key_buf;

	*key = NULL;
	*key_len = 0;
	*data = NULL;
	data_len = 0;
*/
	/*
	//Make sure the cursor isn't empty
	if (!cur) {
		mlog(MDHIM_SERVER_INFO, "Cursor empty");
		return MDHIM_DB_ERROR;
	}

	//Check if this a valid entry
	if ((ret = unqlite_kv_cursor_valid_entry(cur)) != UNQLITE_OK) {
		mlog(MDHIM_SERVER_INFO, "Next entry not found");
		return MDHIM_DB_ERROR;
	}

	//Change to seek later
	if ((ret = unqlite_kv_cursor_prev_entry(cur)) != UNQLITE_OK) {
		print_unqlite_err_msg(dh);
		return MDHIM_DB_ERROR;
	}
	//Find the size of the key
	if ((ret = unqlite_kv_cursor_key(cur, NULL, key_len)) != UNQLITE_OK) {
		print_unqlite_err_msg(dh);
		return MDHIM_DB_ERROR;
	}
	
	//Get the key
	key_buf = malloc(*key_len);
	if ((ret = unqlite_kv_cursor_key(cur, key_buf, key_len)) != UNQLITE_OK) {
		print_unqlite_err_msg(dh);
		return MDHIM_DB_ERROR;
	}      
	*key = key_buf;

	//Find the size of the data
	if ((ret = unqlite_kv_cursor_data(cur, NULL, (unqlite_int64 *) data_len)) != UNQLITE_OK) {
		print_unqlite_err_msg(dh);
		return MDHIM_DB_ERROR;
	}
	
	//Get the data
	data_buf = malloc(*data_len);
	if ((ret = unqlite_kv_cursor_data(cur, data_buf, (unqlite_int64 *) data_len)) 
	    != UNQLITE_OK) {
		print_unqlite_err_msg(dh);
		return MDHIM_DB_ERROR;
	}      
	*data = data_buf;
	*/
	return MDHIM_SUCCESS;
}


/**
 * mdhim_unqlite_commit
 * Commits outstanding writes the data store
 *
 * @param dbh         in   pointer to the unqlite db handle 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_unqlite_commit(void *dbh) {
	int ret = 0;
	unqlite *dh = (unqlite *) dbh;
  
	if ((ret = unqlite_commit(dh)) != UNQLITE_OK) {
		print_unqlite_err_msg(dbh);
		return MDHIM_DB_ERROR;
	}

	return MDHIM_SUCCESS;
}

/**
 * mdhim_unqlite_close
 * Closes the data store
 *
 * @param dbh         in   pointer to the unqlite db handle 
 * @param mstore_opts in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_unqlite_close(void *dbh, void *dbs, struct mdhim_store_opts_t *mstore_opts) {
	int ret = 0;

	if ((ret = unqlite_close((unqlite *) dbh)) != UNQLITE_OK) {
		print_unqlite_err_msg(dbh);
		return MDHIM_DB_ERROR;
	}

	if ((ret = unqlite_close((unqlite *) dbs)) != UNQLITE_OK) {
		print_unqlite_err_msg(dbs);
		return MDHIM_DB_ERROR;
	}

	return MDHIM_SUCCESS;
}
