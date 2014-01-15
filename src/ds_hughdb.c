#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <linux/limits.h>
#include <sys/time.h>
#include "ds_hughdb.h"

/**
 * mdhim_hughdb_open
 * Opens the database
 *
 * @param dbh            in   double pointer to the hughdb handle
 * @param dbs            in   double pointer to the hughdb statistics db handle 
 * @param path           in   path to the database file
 * @param flags          in   flags for opening the data store
 * @param mstore_opts    in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */

int mdhim_hughdb_open(void **dbh, void **dbs, char *path, int flags, 
		       struct mdhim_store_opts_t *mstore_opts) {
	hughdb_t *db;

	db = malloc(sizeof(hughdb_t));
	db->file = fopen(path, "a+");
	setbuffer(db->file, db->buf, HUGHDB_BUF_SIZE);
	if (!db->file) {
		mlog(MDHIM_SERVER_CRIT, "Error opening hughdb database: %s", strerror(errno));
		return MDHIM_DB_ERROR;
	}

	*dbh = db;
	*dbs = NULL;

	return MDHIM_SUCCESS;
}

/**
 * mdhim_hughdb_put
 * Stores a single key in the data store
 *
 * @param dbh         in   pointer to the hughdb handle
 * @param key         in   void * to the key to store
 * @param key_len     in   length of the key
 * @param data        in   void * to the value of the key
 * @param data_len    in   length of the value data 
 * @param mstore_opts in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_hughdb_put(void *dbh, void *key, int key_len, void *data, int32_t data_len, 
		      struct mdhim_store_opts_t *mstore_opts) {
    hughdb_t *db = (hughdb_t *) dbh;
    struct timeval start, end;
    size_t ret;
    hughdb_rec_t rec;

    if (!dbh) {
	    return MDHIM_DB_ERROR;
    }

    gettimeofday(&start, NULL);

    //Write the header, key and value
    rec.key_len = key_len;
    rec.data_len = data_len;
    ret = fwrite(&rec, sizeof(hughdb_rec_t), 1, db->file);    
    ret += fwrite(key, key_len, 1, db->file);
    ret += fwrite(data, data_len, 1, db->file);
    if (ret != 3) {
	    mlog(MDHIM_SERVER_CRIT, "Error writing record to hughdb database: %s", 
		 strerror(errno));
	    //return MDHIM_DB_ERROR;
    }

    gettimeofday(&end, NULL);
    mlog(MDHIM_SERVER_DBG, "Took: %d seconds to put the record", 
	 (int) (end.tv_sec - start.tv_sec));

    return MDHIM_SUCCESS;
}

/**
 * mdhim_hughdb_batch_put
 * Stores multiple keys in the data store
 *
 * @param dbh          in   pointer to the hughdb handle
 * @param keys         in   void ** to the key to store
 * @param key_lens     in   int * to the lengths of the keys
 * @param data         in   void ** to the values of the keys
 * @param data_lens    in   int * to the lengths of the value data 
 * @param num_records  in   int for the number of records to insert 
 * @param mstore_opts  in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_hughdb_batch_put(void *dbh, void **keys, int32_t *key_lens, 
			    void **data, int32_t *data_lens, int num_records,
			    struct mdhim_store_opts_t *mstore_opts) {
	int i;

	if (!dbh) {
		return MDHIM_DB_ERROR;
	}

	for (i = 0; i < num_records; i++) {
		mdhim_hughdb_put(dbh, keys[i], key_lens[i], 
				 data[i], data_lens[i], mstore_opts);
	}

	return MDHIM_SUCCESS;
}

/**
 * mdhim_hughdb_get
 * Gets a value, given a key, from the data store
 *
 * @param dbh          in   pointer to the hughdb db handle
 * @param key          in   void * to the key to retrieve the value of
 * @param key_len      in   length of the key
 * @param data         out  void * to the value of the key
 * @param data_len     out  pointer to length of the value data 
 * @param mstore_opts  in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_hughdb_get(void *dbh, void *key, int key_len, void **data, int32_t *data_len, 
		      struct mdhim_store_opts_t *mstore_opts) {
	int ret = MDHIM_SUCCESS;

	return ret;
}

/**
 * mdhim_hughdb_get_next
 * Gets the next key/value from the data store
 *
 * @param dbh             in   pointer to the unqlite db handle
 * @param key             out  void ** to the key that we get
 * @param key_len         out  int * to the length of the key 
 * @param data            out  void ** to the value belonging to the key
 * @param data_len        out  int * to the length of the value data 
 * @param mstore_opts in   additional cursor options for the data store layer 
 * 
 */
int mdhim_hughdb_get_next(void *dbh, void **key, int *key_len, 
			   void **data, int32_t *data_len, 
			   struct mdhim_store_opts_t *mstore_opts) {
	int ret = MDHIM_SUCCESS;
	return ret;
}


/**
 * mdhim_hughdb_get_prev
 * Gets the prev key/value from the data store
 *
 * @param dbh             in   pointer to the unqlite db handle
 * @param key             out  void ** to the key that we get
 * @param key_len         out  int * to the length of the key 
 * @param data            out  void ** to the value belonging to the key
 * @param data_len        out  int * to the length of the value data 
 * @param mstore_opts in   additional cursor options for the data store layer 
 * 
 */
int mdhim_hughdb_get_prev(void *dbh, void **key, int *key_len, 
			   void **data, int32_t *data_len, 
			   struct mdhim_store_opts_t *mstore_opts) {
	int ret = MDHIM_SUCCESS;
	return ret;
}

/**
 * mdhim_hughdb_close
 * Closes the data store
 *
 * @param dbh         in   pointer to the hughdb db handle 
 * @param dbs         in   pointer to the hughdb statistics db handle 
 * @param mstore_opts in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_hughdb_close(void *dbh, void *dbs, struct mdhim_store_opts_t *mstore_opts) {
	hughdb_t *db = (hughdb_t *) dbh;
	hughdb_t *db_s = (hughdb_t *) dbs;

	//Close the databases
	if (db) {
		fclose(db->file);
		free(db);
	}
	if (db_s) {
		fclose(db_s->file);
		free(db_s);
	}

	return MDHIM_SUCCESS;
}

/**
 * mdhim_hughdb_del
 * delete the given key
 *
 * @param dbh         in   pointer to the hughdb db handle
 * @param key         in   void * for the key to delete
 * @param key_len     in   int for the length of the key
 * @param mstore_opts in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_hughdb_del(void *dbh, void *key, int key_len, 
		      struct mdhim_store_opts_t *mstore_opts) {
	return MDHIM_SUCCESS;
}

/**
 * mdhim_hughdb_commit
 * Commits outstanding writes the data store
 *
 * @param dbh         in   pointer to the hughdb handle 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_hughdb_commit(void *dbh) {
	return MDHIM_SUCCESS;
}
