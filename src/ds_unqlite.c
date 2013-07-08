#include <stdlib.h>
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
	
	msg = get_unqlite_err_msg(dh);
	if (!msg) {
		return;
	}
	
	mlog(MDHIM_SERVER_CRIT, "Unqlite error: %s", msg);
}

/**
 * mdhim_unqlite_cursor_init
 * Initializes a cursor
 *
 * @param dbh   in   * to the unqlite db handle
 * @return void * to cursor or NULL on failure
 */
void *mdhim_unqlite_cursor_init(void *dbh) {
	unqlite *dh = (unqlite *) dbh;
	unqlite_kv_cursor *cursor;
	int ret = 0;
	
	if ((ret = unqlite_kv_cursor_init(dh, &cursor)) 
	    != UNQLITE_OK) {
		print_unqlite_err_msg(dh);
		return NULL;
	}

	return (void *)cursor;
}

/**
 * mdhim_unqlite_cursor_release
 * Releases a cursor
 *
 * @param dbh   in   * to the unqlite db handle
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_unqlite_cursor_release(void *dbh, void *curh) {
	unqlite *dh = (unqlite *) dbh;
	unqlite_kv_cursor *cursor = (unqlite_kv_cursor *) curh;
	int ret = 0;
	
	if ((ret = unqlite_kv_cursor_release(dh, cursor)) 
	    != UNQLITE_OK) {
		print_unqlite_err_msg(dh);
		return MDHIM_DB_ERROR;
	}

	return MDHIM_SUCCESS;
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
int mdhim_unqlite_open(void **dbh, char *path, int flags, 
		       struct mdhim_store_opts_t *mstore_opts) {
	int ret = 0;
	int imode;
	unqlite **dh = (unqlite **) dbh;

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
	
	//Open the database
	if ((ret = unqlite_open(dh, path, imode)) != UNQLITE_OK) {
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

	if ((ret = unqlite_kv_append(dh, key, key_len, data, (int64_t) data_len)) != UNQLITE_OK) {
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
	unqlite_int64 nbytes;

	*((char **) data) = NULL;
	*data_len = 0;

	//Extract data size first
	if ((ret = unqlite_kv_fetch(dh, key, key_len, NULL, &nbytes)) != UNQLITE_OK) {
		print_unqlite_err_msg(dh);
		return MDHIM_DB_ERROR;
	}
	
        //Allocate the buffer to the value's size
	*((char **) data) = (char *) malloc(nbytes);

        //Copy record content in our buffer
	if ((ret = unqlite_kv_fetch(dh, key, key_len, *((char **) data), 
				    &nbytes)) != UNQLITE_OK) {
		print_unqlite_err_msg(dh);
		return MDHIM_DB_ERROR;
	}

	mlog(MDHIM_SERVER_DBG, "Retrieved value: %d", **((int **) data));
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
 * @param mstore_cur_opts in   additional cursor options for the data store layer 
 * 
 */
int mdhim_unqlite_get_next(void *dbh, void *curh, void **key, int *key_len, 
			   void **data, int32_t *data_len, 
			   struct mdhim_store_cur_opts_t *mstore_cur_opts) {
	unqlite *dh = (unqlite *) dbh;
	int ret = 0;
	unqlite_kv_cursor *cur = (unqlite_kv_cursor *) curh;	
	char *data_buf;
	char *key_buf;

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

	return MDHIM_SUCCESS;
}


/**
 * mdhim_unqlite_get_prev
 * Gets the next key/value from the data store
 *
 * @param dbh             in   pointer to the unqlite db handle
 * @param mcur            in   pointer to the db cursor
 * @param key             out  void ** to the key that we get
 * @param key_len         out  int * to the length of the key 
 * @param data            out  void ** to the value belonging to the key
 * @param data_len        out  int * to the length of the value data 
 * @param mstore_cur_opts in   additional cursor options for the data store layer 
 * 
 */
int mdhim_unqlite_get_prev(void *dbh, void *curh, void **key, int *key_len, 
			   void **data, int32_t *data_len, 
			   struct mdhim_store_cur_opts_t *mstore_cur_opts) {
	unqlite *dh = (unqlite *) dbh;
	int ret = 0;
	unqlite_kv_cursor *cur = (unqlite_kv_cursor *) curh;	
	char *data_buf;
	char *key_buf;

	*key = NULL;
	*key_len = 0;
	*data = NULL;
	data_len = 0;

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
int mdhim_unqlite_close(void *dbh, struct mdhim_store_opts_t *mstore_opts) {
	int ret = 0;

	if ((ret = unqlite_close((unqlite *) dbh)) != UNQLITE_OK) {
		print_unqlite_err_msg(dbh);
		return MDHIM_DB_ERROR;
	}

	return MDHIM_SUCCESS;
}
