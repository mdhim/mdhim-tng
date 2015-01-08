#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <linux/limits.h>
#include <sys/time.h>
#include "ds_leveldb.h"

static void cmp_destroy(void* arg) { }

static int cmp_empty(const char* a, size_t alen,
		     const char* b, size_t blen) {
	int ret = 2;
	if (a && !b) {
		return 1;
	} else if (!a && b) {
		return -1;
	} else if (!a && !b) {
		return 0;
	}

	if (alen > blen) {
		return 1;
	} else if (blen > alen) {
		return -1;
	} 

	return ret;
}

static int cmp_int_compare(void* arg, const char* a, size_t alen,
			   const char* b, size_t blen) {
	int ret;

	ret = cmp_empty(a, alen, b, blen);
	if (ret != 2) {
		return ret;
	}
	if (*(uint32_t *) a < *(uint32_t *) b) {
		ret = -1;
	} else if (*(uint32_t *) a == *(uint32_t *) b) {
		ret = 0;
	} else {
		ret = 1;
	}

	return ret;
}

static int cmp_lint_compare(void* arg, const char* a, size_t alen,
			   const char* b, size_t blen) {
	int ret;

	ret = cmp_empty(a, alen, b, blen);
	if (ret != 2) {
		return ret;
	}
	if (*(uint64_t *) a < *(uint64_t *) b) {
		ret = -1;
	} else if (*(uint64_t *) a == *(uint64_t *) b) {
		ret = 0;
	} else {
		ret = 1;
	}

	return ret;
}

static int cmp_double_compare(void* arg, const char* a, size_t alen,
			      const char* b, size_t blen) {
	int ret;

	ret = cmp_empty(a, alen, b, blen);
	if (ret != 2) {
		return ret;
	}
	if (*(double *) a < *(double *) b) {
		ret = -1;
	} else if (*(double *) a == *(double *) b) {
		ret = 0;
	} else {
		ret = 1;
	}

	return ret;
}

static int cmp_float_compare(void* arg, const char* a, size_t alen,
			   const char* b, size_t blen) {
	int ret;

	ret = cmp_empty(a, alen, b, blen);
	if (ret != 2) {
		return ret;
	}
	if (*(float *) a < *(float *) b) {
		ret = -1;
	} else if (*(float *) a == *(float *) b) {
		ret = 0;
	} else {
		ret = 1;
	}

	return ret;
}


// For string, first compare for null pointers, then for order
// up to a null character or the given lengths.
static int cmp_string_compare(void* arg, const char* a, size_t alen,
			   const char* b, size_t blen) {
    int idx;

    if (a && !b) {
            return 1;
    } else if (!a && b) {
            return -1;
    } else if (!a && !b) {
            return 0;
    }

    // Do this wile they are equal and we have not reached the end of one of them
    for(idx=0; *a == *b && *a != '\0' && *b != '\0' && idx<alen && idx<blen; ) {
        idx++;
        a++;
        b++;
    }

    // If we are at the end and no difference is found, then they are equal
    if( (*a == '\0' && *b == '\0') || (alen == blen && idx == alen)) {
       return 0;
    } else if ((alen == idx || *a == '\0') && alen < blen) { // end of a?
        return -1;
    } else if ((blen == idx || *b == '\0') && blen < alen) { // end of b?
        return 1;
    } else if ( *a > *b ) { // else compare the two different characters to decide
       return 1;
    }

    // If none of the above, then b is greater
    return -1;
}

static int cmp_byte_compare(void* arg, const char* a, size_t alen,
			    const char* b, size_t blen) {
	int ret;

	ret = cmp_empty(a, alen, b, blen);
	if (ret != 2) {
		return ret;
	}

	ret = memcmp(a, b, alen);

	return ret;
}
static const char* cmp_name(void* arg) {
	return "mdhim_cmp";
}

/**
 * mdhim_leveldb_open
 * Opens the database
 *
 * @param dbh            in   double pointer to the leveldb handle
 * @param dbs            in   double pointer to the leveldb statistics db handle 
 * @param path           in   path to the database file
 * @param flags          in   flags for opening the data store
 * @param mstore_opts    in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */

int mdhim_leveldb_open(void **dbh, void **dbs, char *path, int flags, int key_type, struct mdhim_options_t *opts) {
	struct mdhim_leveldb_t *mdhimdb;
	struct mdhim_leveldb_t *statsdb;
	leveldb_t *db;
	char *err = NULL;
	char stats_path[PATH_MAX];

	mdhimdb = malloc(sizeof(struct mdhim_leveldb_t));
	memset(mdhimdb, 0, sizeof(struct mdhim_leveldb_t));
	statsdb = malloc(sizeof(struct mdhim_leveldb_t));
	memset(statsdb, 0, sizeof(struct mdhim_leveldb_t));

	//Create the options for the main database
	mdhimdb->options = leveldb_options_create();
	leveldb_options_set_create_if_missing(mdhimdb->options, 1);
	//leveldb_options_set_compression(options, 0);
	mdhimdb->filter = leveldb_filterpolicy_create_bloom(256);
	mdhimdb->cache = leveldb_cache_create_lru(50242880);
	mdhimdb->env = leveldb_create_default_env();
	mdhimdb->write_options = leveldb_writeoptions_create();
	leveldb_writeoptions_set_sync(mdhimdb->write_options, 0);
	mdhimdb->read_options = leveldb_readoptions_create();
	leveldb_options_set_cache(mdhimdb->options, mdhimdb->cache);
	leveldb_options_set_filter_policy(mdhimdb->options, mdhimdb->filter);
	//leveldb_options_set_max_open_files(mdhimdb->options, 10000);
	leveldb_options_set_max_open_files(mdhimdb->options, 10000);
	leveldb_options_set_write_buffer_size(mdhimdb->options, 50242880);
	leveldb_options_set_env(mdhimdb->options, mdhimdb->env);
	//Create the options for the stat database
	statsdb->options = leveldb_options_create();
	leveldb_options_set_create_if_missing(statsdb->options, 1);
	//leveldb_options_set_compression(stat_options, 0);
	statsdb->filter = leveldb_filterpolicy_create_bloom(16);       
	statsdb->cache = leveldb_cache_create_lru(1024);
	statsdb->env = leveldb_create_default_env();
	statsdb->write_options = leveldb_writeoptions_create();
	leveldb_writeoptions_set_sync(statsdb->write_options, 0);
	statsdb->read_options = leveldb_readoptions_create();
	leveldb_options_set_cache(statsdb->options, statsdb->cache);
	leveldb_options_set_filter_policy(statsdb->options, statsdb->filter);
	leveldb_options_set_write_buffer_size(statsdb->options, 1024);
	leveldb_options_set_env(statsdb->options, statsdb->env);

	switch(key_type) {
	case MDHIM_INT_KEY:
		mdhimdb->cmp = leveldb_comparator_create(NULL, cmp_destroy, cmp_int_compare, cmp_name);
		mdhimdb->compare = cmp_int_compare;
		break;
	case MDHIM_LONG_INT_KEY:
		mdhimdb->cmp = leveldb_comparator_create(NULL, cmp_destroy, cmp_lint_compare, cmp_name);
		mdhimdb->compare = cmp_lint_compare;
		break;
	case MDHIM_FLOAT_KEY:
		mdhimdb->cmp = leveldb_comparator_create(NULL, cmp_destroy, cmp_float_compare, cmp_name);
		mdhimdb->compare = cmp_float_compare;
		break;
	case MDHIM_DOUBLE_KEY:
		mdhimdb->cmp = leveldb_comparator_create(NULL, cmp_destroy, cmp_double_compare, cmp_name);
		mdhimdb->compare = cmp_double_compare;
		break;
	case MDHIM_STRING_KEY:
		mdhimdb->cmp = leveldb_comparator_create(NULL, cmp_destroy, cmp_string_compare, cmp_name);
		mdhimdb->compare = cmp_string_compare;
		break;
	default:
		mdhimdb->cmp = leveldb_comparator_create(NULL, cmp_destroy, cmp_byte_compare, cmp_name);
		mdhimdb->compare = cmp_byte_compare;
		break;
	}
	
	leveldb_options_set_comparator(mdhimdb->options, mdhimdb->cmp);
	//Check to see if the given path + "_stat" and the null char will be more than the max
	if (strlen(path) + 6 > PATH_MAX) {
		mlog(MDHIM_SERVER_CRIT, "Error opening leveldb database - path provided is too long");
		return MDHIM_DB_ERROR;
	}

	//Open the main database
	db = leveldb_open(mdhimdb->options, path, &err);
	mdhimdb->db = db;
	//Set the output handle
	*((struct mdhim_leveldb_t **) dbh) = mdhimdb;
	if (err != NULL) {
		mlog(MDHIM_SERVER_CRIT, "Error opening leveldb database");
		return MDHIM_DB_ERROR;
	}

	//Open the stats database
	sprintf(stats_path, "%s_stats", path);
	statsdb->compare = cmp_int_compare;
	statsdb->cmp = leveldb_comparator_create(NULL, cmp_destroy, cmp_int_compare, cmp_name);
	leveldb_options_set_comparator(statsdb->options, statsdb->cmp);
	db = leveldb_open(statsdb->options, stats_path, &err);
	statsdb->db = db;
	*((struct mdhim_leveldb_t **) dbs) = statsdb;

	if (err != NULL) {
		mlog(MDHIM_SERVER_CRIT, "Error opening leveldb database");
		return MDHIM_DB_ERROR;
	}

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
int mdhim_leveldb_put(void *dbh, void *key, int key_len, void *data, int32_t data_len) {
    leveldb_writeoptions_t *options;
    char *err = NULL;
    struct mdhim_leveldb_t *mdhimdb = (struct mdhim_leveldb_t *) dbh;
    struct timeval start, end;
    
    gettimeofday(&start, NULL);
    options = mdhimdb->write_options;    	    
    leveldb_put(mdhimdb->db, options, key, key_len, data, data_len, &err);
    if (err != NULL) {
	    mlog(MDHIM_SERVER_CRIT, "Error putting key/value in leveldb");
	    return MDHIM_DB_ERROR;
    }

    gettimeofday(&end, NULL);
    mlog(MDHIM_SERVER_DBG, "Took: %d seconds to put the record", 
	 (int) (end.tv_sec - start.tv_sec));

    return MDHIM_SUCCESS;
}

/**
 * mdhim_leveldb_batch_put
 * Stores multiple keys in the data store
 *
 * @param dbh          in   pointer to the leveldb handle
 * @param keys         in   void ** to the key to store
 * @param key_lens     in   int * to the lengths of the keys
 * @param data         in   void ** to the values of the keys
 * @param data_lens    in   int * to the lengths of the value data 
 * @param num_records  in   int for the number of records to insert 
 * @param mstore_opts  in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_leveldb_batch_put(void *dbh, void **keys, int32_t *key_lens, 
			    void **data, int32_t *data_lens, int num_records) {
	leveldb_writeoptions_t *options;
	char *err = NULL;
	struct mdhim_leveldb_t *mdhimdb = (struct mdhim_leveldb_t *) dbh;
	struct timeval start, end;
	leveldb_writebatch_t* write_batch;
	int i;

	gettimeofday(&start, NULL);
	write_batch = leveldb_writebatch_create();
	options = mdhimdb->write_options;   
	for (i = 0; i < num_records; i++) {
		leveldb_writebatch_put(write_batch, keys[i], key_lens[i], 
				       data[i], data_lens[i]);
	}

	leveldb_write(mdhimdb->db, options, write_batch, &err);
	leveldb_writebatch_destroy(write_batch);
	if (err != NULL) {
		mlog(MDHIM_SERVER_CRIT, "Error in batch put in leveldb");
		return MDHIM_DB_ERROR;
	}
	
	gettimeofday(&end, NULL);
	mlog(MDHIM_SERVER_DBG, "Took: %d seconds to put %d records", 
	     (int) (end.tv_sec - start.tv_sec), num_records);
	
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
int mdhim_leveldb_get(void *dbh, void *key, int key_len, void **data, int32_t *data_len) {
	leveldb_readoptions_t *options;
	char *err = NULL;
	struct mdhim_leveldb_t *mdhimdb = (struct mdhim_leveldb_t *) dbh;
	int ret = MDHIM_SUCCESS;
	void *ldb_data;
	size_t ldb_data_len = 0;

	options = mdhimdb->read_options;
	*data = NULL;
	ldb_data = leveldb_get(mdhimdb->db, options, key, key_len, &ldb_data_len, &err);
	if (err != NULL) {
		mlog(MDHIM_SERVER_CRIT, "Error getting value in leveldb");
		return MDHIM_DB_ERROR;
	}

	if (!ldb_data_len) {
		ret = MDHIM_DB_ERROR;
		return ret;
	}

	*data_len = ldb_data_len;
	*data = malloc(*data_len);
	memcpy(*data, ldb_data, *data_len);
	free(ldb_data);

	return ret;
}

/**
 * mdhim_leveldb_get_next
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
int mdhim_leveldb_get_next(void *dbh, void **key, int *key_len, 
			   void **data, int32_t *data_len) {
	leveldb_readoptions_t *options;
	struct mdhim_leveldb_t *mdhimdb = (struct mdhim_leveldb_t *) dbh;
	int ret = MDHIM_SUCCESS;
	leveldb_iterator_t *iter;
	const char *res;
	int len = 0;
	void *old_key;
	int old_key_len;
	struct timeval start, end;

	//Init the data to return
	*data = NULL;
	*data_len = 0;

	gettimeofday(&start, NULL);
	//Create the options and iterator
	options = mdhimdb->read_options;
	old_key = *key;
	old_key_len = *key_len;
	*key = NULL;
	*key_len = 0;

	iter = leveldb_create_iterator(mdhimdb->db, options);

	//If the user didn't supply a key, then seek to the first
	if (!old_key || old_key_len == 0) {
		leveldb_iter_seek_to_first(iter);
	} else {
		/* Seek to the passed in key.  If that doesn't exist, iterate until we find one greater
		   or until we exhaust the keys.*/
		leveldb_iter_seek(iter, old_key, old_key_len);
		if (leveldb_iter_valid(iter)) {
			leveldb_iter_next(iter);
		}
	
		if (!leveldb_iter_valid(iter)) { 
			leveldb_iter_seek_to_first(iter);
			while(leveldb_iter_valid(iter)) {
				res = leveldb_iter_key(iter, (size_t *) &len);
				if (mdhimdb->compare(NULL, res, len, old_key, old_key_len) > 0) {
					break;
				}
				
				leveldb_iter_next(iter);
			}			
		}
	}

	if (!leveldb_iter_valid(iter)) {
		goto error;
	}

	res = leveldb_iter_value(iter, (size_t *) &len);
	if (res) {
		*data = malloc(len);
		memcpy(*data, res, len);
		*data_len = len;
	} else {
		*data = NULL;
		*data_len = 0;
	}

	res = leveldb_iter_key(iter, (size_t *) key_len);
	if (res) {
		*key = malloc(*key_len);
		memcpy(*key, res, *key_len);
	} else {
		*key = NULL;
		*key_len = 0;
	}

	if (!*data) {
		goto error;
	}

        //Destroy iterator
	leveldb_iter_destroy(iter);      
	gettimeofday(&end, NULL);
	mlog(MDHIM_SERVER_DBG, "Took: %d seconds to get the next record", 
	     (int) (end.tv_sec - start.tv_sec));
	return ret;

error:	
	 //Destroy iterator
	leveldb_iter_destroy(iter);      
	*key = NULL;
	*key_len = 0;
	*data = NULL;
	*data_len = 0;
	return MDHIM_DB_ERROR;
}


/**
 * mdhim_leveldb_get_prev
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
int mdhim_leveldb_get_prev(void *dbh, void **key, int *key_len, 
			   void **data, int32_t *data_len) {
	leveldb_readoptions_t *options;
	struct mdhim_leveldb_t *mdhimdb = (struct mdhim_leveldb_t *) dbh;
	int ret = MDHIM_SUCCESS;
	leveldb_iterator_t *iter;
	const char *res;
	int len = 0;
	void *old_key;
	int old_key_len;
	struct timeval start, end;

	//Init the data to return
	*data = NULL;
	*data_len = 0;

	gettimeofday(&start, NULL);

	//Create the options and iterator
	options = mdhimdb->read_options;
	old_key = *key;
	old_key_len = *key_len;
	*key = NULL;
	*key_len = 0;

	iter = leveldb_create_iterator(mdhimdb->db, options);

	//If the user didn't supply a key, then seek to the first
	if (!old_key || old_key_len == 0) {
		leveldb_iter_seek_to_last(iter);
	} else {
		leveldb_iter_seek(iter, old_key, old_key_len);
		if (!leveldb_iter_valid(iter)) { 
			leveldb_iter_seek_to_last(iter);
			while(leveldb_iter_valid(iter)) {
				res = leveldb_iter_key(iter, (size_t *) &len);
				if (mdhimdb->compare(NULL, res, len, old_key, old_key_len) < 0) {
					break;
				}
				
				leveldb_iter_prev(iter);
			}			
		} else {
			leveldb_iter_prev(iter);
		}
	}

	if (!leveldb_iter_valid(iter)) {
		goto error;
	}

	res = leveldb_iter_value(iter, (size_t *) &len);
	if (res) {
		*data = malloc(len);
		memcpy(*data, res, len);
		*data_len = len;
	} else {
		*data = NULL;
		*data_len = 0;
	}

	res = leveldb_iter_key(iter, (size_t *) key_len);
	if (res) {
		*key = malloc(*key_len);
		memcpy(*key, res, *key_len);
	} else {
		*key = NULL;
		*key_len = 0;
	}

	if (!*data) {
		goto error;
	}

        //Destroy iterator
	leveldb_iter_destroy(iter);      
	gettimeofday(&end, NULL);
	mlog(MDHIM_SERVER_DBG, "Took: %d seconds to get the previous record", 
	     (int) (end.tv_sec - start.tv_sec));
	return ret;

error:	
	 //Destroy iterator
	leveldb_iter_destroy(iter);      
	*key = NULL;
	*key_len = 0;
	*data = NULL;
	*data_len = 0;
	return MDHIM_DB_ERROR;
}

/**
 * mdhim_leveldb_close
 * Closes the data store
 *
 * @param dbh         in   pointer to the leveldb db handle 
 * @param dbs         in   pointer to the leveldb statistics db handle 
 * @param mstore_opts in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_leveldb_close(void *dbh, void *dbs) {
	struct mdhim_leveldb_t *mdhimdb = (struct mdhim_leveldb_t *) dbh;
	struct mdhim_leveldb_t *statsdb = (struct mdhim_leveldb_t *) dbs;

	//Close the databases
	leveldb_close(mdhimdb->db);
	leveldb_close(statsdb->db);

	//Destroy the options
	leveldb_comparator_destroy(mdhimdb->cmp);
	leveldb_options_destroy(mdhimdb->options);
	leveldb_readoptions_destroy(mdhimdb->read_options);
	leveldb_writeoptions_destroy(mdhimdb->write_options);
	leveldb_filterpolicy_destroy(mdhimdb->filter);
	leveldb_comparator_destroy(statsdb->cmp);
	leveldb_options_destroy(statsdb->options);
	leveldb_readoptions_destroy(statsdb->read_options);
	leveldb_writeoptions_destroy(statsdb->write_options);
	leveldb_filterpolicy_destroy(statsdb->filter);

	free(mdhimdb);
	free(statsdb);

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
int mdhim_leveldb_del(void *dbh, void *key, int key_len) {
	leveldb_writeoptions_t *options;
	char *err = NULL;
	struct mdhim_leveldb_t *mdhimdb = (struct mdhim_leveldb_t *) dbh;
	
	options = mdhimdb->write_options;
	leveldb_delete(mdhimdb->db, options, key, key_len, &err);
	if (err != NULL) {
		mlog(MDHIM_SERVER_CRIT, "Error deleting key in leveldb");
		return MDHIM_DB_ERROR;
	}
 
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
