#include "ds_unqlite.h"


/*
 * mdhim_unqlite_open
 * Stores a single key in the data store
 *
 * @param dbh            in   pointer to the unqlite db handle
 * @param path           in   path to the database file
 * @param flags          in   flags for opening the data store
 * @param mstore_opts    in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_unqlite_open(void *dbh, char *path, int flags, 
		       struct mdhim_store_opts_t *mstore_opts) {
	int ret = 0;
	int imode;
	unqlite *hd = (unqlite *) dbh;

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
	if ((ret = unqlite_open(&hd, path, imode)) != UNQLITE_OK) {
		return MDHIM_DB_ERROR;
	}

	return MDHIM_SUCCESS;
}

/*
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
int mdhim_unqlite_put(void *dbh, void *key, int key_len, void *data, int data_len, 
		      struct mdhim_store_opts_t *mstore_opts) {
}

/*
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
int mdhim_unqlite_del(void *dbh, void *keys, int key_lens, 
		      struct mdhim_store_opts_t *mstore_opts) {
}

/*
 * mdhim_unqlite_get
 * Gets a value, given a key, from the data store
 *
 * @param dbh          in   pointer to the unqlite db handle
 * @param keys         in   void * to the key to retrieve the value of
 * @param key_lens     in   length of the key
 * @param data         out  void * to the value of the key
 * @param data_lens    out  length of the value data 
 * @param mstore_opts  in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_unqlite_get(void *dbh, void *key, int key_len, void *data, int data_len, 
		      struct mdhim_store_opts_t *mstore_opts) {
}

/*
 * mdhim_unqlite_get_next
 * Gets the next key/value from the data store
 *
 * @param mcur            in   pointer to the db cursor
 * @param data            out  void * to the value of the key
 * @param data_len        out  int * to the length of the value data 
 * @param mstore_cur_opts in   additional cursor options for the data store layer 
 * 
 */
int mdhim_unqlite_get_next(struct mdhim_store_cur_t *mcur, void *data, int *data_len, 
			   struct mdhim_store_cur_opts_t *mstore_cur_opts) {
}


/*
 * mdhim_unqlite_get_prev
 * Gets the previous key/value from the data store
 *
 * @param mcur            in   pointer to the db cursor
 * @param data            out  void * to the value of the key
 * @param data_len        out  int * to the length of the value data 
 * @param mstore_cur_opts in   additional cursor options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_unqlite_get_prev(struct mdhim_store_cur_t *mcur, void *data, int *data_len, 
			   struct mdhim_store_cur_opts_t *mstore_cur_opts) {
}

/*
 * mdhim_unqlite_close
 * Closes the data store
 *
 * @param dbh         in   pointer to the unqlite db handle 
 * @param mstore_opts in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_unqlite_close(void *dbh, struct mdhim_store_opts_t *mstore_opts) {
}
