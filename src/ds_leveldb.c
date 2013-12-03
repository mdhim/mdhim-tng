#include <stdlib.h>
#include <string.h>

#ifndef      LEVELDB_SUPPORT
#include <rocksdb/c.h>
#else
#include <leveldb/c.h>
#endif

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

int mdhim_leveldb_open(void **dbh, void **dbs, char *path, int flags, 
		       struct mdhim_store_opts_t *mstore_opts) {
	leveldb_t *db;
	leveldb_options_t *options;
	leveldb_options_t *stat_options;
	char *err = NULL;
	leveldb_comparator_t* cmp = NULL;
	char stats_path[PATH_MAX];
	leveldb_filterpolicy_t *main_filter;
	leveldb_filterpolicy_t *stats_filter;
	leveldb_cache_t *main_cache;
	leveldb_cache_t *stats_cache;
	leveldb_env_t *main_env;
	leveldb_env_t *stats_env;

	//Create the options for the main database
	options = leveldb_options_create();
	leveldb_options_set_create_if_missing(options, 1);
	//leveldb_options_set_compression(options, 0);
	main_filter = leveldb_filterpolicy_create_bloom(256);
	main_cache = leveldb_cache_create_lru(5242880);
	main_env = leveldb_create_default_env();
	leveldb_options_set_cache(options, main_cache);
	leveldb_options_set_filter_policy(options, main_filter);
	leveldb_options_set_max_open_files(options, 10000);
	leveldb_options_set_write_buffer_size(options, 5242880);
	leveldb_options_set_env(options, main_env);

	//Create the options for the stat database
	stat_options = leveldb_options_create();
	leveldb_options_set_create_if_missing(stat_options, 1);
	//leveldb_options_set_compression(stat_options, 0);
	stats_filter = leveldb_filterpolicy_create_bloom(256);       
	stats_cache = leveldb_cache_create_lru(1024);
	stats_env = leveldb_create_default_env();
	leveldb_options_set_cache(stat_options, stats_cache);
	leveldb_options_set_filter_policy(stat_options, stats_filter);
	leveldb_options_set_write_buffer_size(stat_options, 1024);
	leveldb_options_set_env(stat_options, stats_env);

	switch(mstore_opts->key_type) {
	case MDHIM_INT_KEY:
		cmp = leveldb_comparator_create(NULL, cmp_destroy, cmp_int_compare, cmp_name);
		break;
	case MDHIM_LONG_INT_KEY:
		cmp = leveldb_comparator_create(NULL, cmp_destroy, cmp_lint_compare, cmp_name);
		break;
	case MDHIM_FLOAT_KEY:
		cmp = leveldb_comparator_create(NULL, cmp_destroy, cmp_float_compare, cmp_name);
		break;
	case MDHIM_DOUBLE_KEY:
		cmp = leveldb_comparator_create(NULL, cmp_destroy, cmp_double_compare, cmp_name);
		break;
	case MDHIM_STRING_KEY:
		cmp = leveldb_comparator_create(NULL, cmp_destroy, cmp_string_compare, cmp_name);
		break;
	default:
		cmp = leveldb_comparator_create(NULL, cmp_destroy, cmp_byte_compare, cmp_name);
		break;
	}
	
	leveldb_options_set_comparator(options, cmp);
	//Check to see if the given path + "_stat" and the null char will be more than the max
	if (strlen(path) + 6 > PATH_MAX) {
		mlog(MDHIM_SERVER_CRIT, "Error opening leveldb database - path provided is too long");
		return MDHIM_DB_ERROR;
	}
	sprintf(stats_path, "%s_stats", path);
	//Open the main database
	db = leveldb_open(options, path, &err);
	//Set the output handle
	*((leveldb_t **) dbh) = db;
	if (err != NULL) {
		mlog(MDHIM_SERVER_CRIT, "Error opening leveldb database");
		return MDHIM_DB_ERROR;
	}

	//Open the stats database
	cmp = leveldb_comparator_create(NULL, cmp_destroy, cmp_int_compare, cmp_name);
	leveldb_options_set_comparator(stat_options, cmp);
	db = leveldb_open(stat_options, stats_path, &err);
	*((leveldb_t **) dbs) = db;
	if (err != NULL) {
		mlog(MDHIM_SERVER_CRIT, "Error opening leveldb database");
		return MDHIM_DB_ERROR;
	}

	//Set the output comparator
	mstore_opts->db_ptr1 = cmp;
	//Set the generic pointers to hold the options for the mdhim db
	mstore_opts->db_ptr2 = options;
	mstore_opts->db_ptr3 = leveldb_readoptions_create();
	mstore_opts->db_ptr4 = leveldb_writeoptions_create();
	//Set the generic pointers to hold the options for the mdhim stats db
	mstore_opts->db_ptr5 = stat_options;
	mstore_opts->db_ptr6 = leveldb_readoptions_create();
	mstore_opts->db_ptr7 = leveldb_writeoptions_create();

	//Set the rest of the options that aren't used until close
	mstore_opts->db_ptr8 = main_filter;
	mstore_opts->db_ptr9 = stats_filter;
	mstore_opts->db_ptr10 = main_cache;
	mstore_opts->db_ptr11 = stats_cache;
	mstore_opts->db_ptr12 = main_env;
	mstore_opts->db_ptr13 = stats_env;

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
    struct timeval start, end;
    
    gettimeofday(&start, NULL);
    options = (leveldb_writeoptions_t *) mstore_opts->db_ptr4;    	    
    leveldb_put(db, options, key, key_len, data, data_len, &err);
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
			    void **data, int32_t *data_lens, int num_records,
			    struct mdhim_store_opts_t *mstore_opts) {
	leveldb_writeoptions_t *options;
	char *err = NULL;
	leveldb_t *db = (leveldb_t *) dbh;
	struct timeval start, end;
	leveldb_writebatch_t* write_batch;
	int i;

	gettimeofday(&start, NULL);
	write_batch = leveldb_writebatch_create();
	options = (leveldb_writeoptions_t *) mstore_opts->db_ptr4;   
	for (i = 0; i < num_records; i++) {
		leveldb_writebatch_put(write_batch, keys[i], key_lens[i], 
				       data[i], data_lens[i]);
	}

	leveldb_write(db, options, write_batch, &err);
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
int mdhim_leveldb_get(void *dbh, void *key, int key_len, void **data, int32_t *data_len, 
		      struct mdhim_store_opts_t *mstore_opts) {
	leveldb_readoptions_t *options;
	char *err = NULL;
	leveldb_t *db = (leveldb_t *) dbh;
	int ret = MDHIM_SUCCESS;
	void *ldb_data;
	size_t ldb_data_len = 0;

	options = (leveldb_readoptions_t *) mstore_opts->db_ptr3;
	*data = NULL;
	ldb_data = leveldb_get(db, options, key, key_len, &ldb_data_len, &err);
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
			   void **data, int32_t *data_len, 
			   struct mdhim_store_opts_t *mstore_opts) {
	leveldb_readoptions_t *options;
	leveldb_t *db = (leveldb_t *) dbh;
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
	options = (leveldb_readoptions_t *) mstore_opts->db_ptr3;
	old_key = *key;
	old_key_len = *key_len;
	*key = NULL;
	*key_len = 0;

	iter = leveldb_create_iterator(db, options);

	//If the user didn't supply a key, then seek to the first
	if (!old_key || old_key_len == 0) {
		leveldb_iter_seek_to_first(iter);
	} else {
		leveldb_iter_seek(iter, old_key, old_key_len);
		if (!leveldb_iter_valid(iter)) { 
			mlog(MDHIM_SERVER_DBG2, "Could not get a valid iterator in leveldb");
			goto error;
		}

		leveldb_iter_next(iter);
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

int mdhim_leveldb_get_prev(void *dbh, void **key, int *key_len, 
			   void **data, int32_t *data_len, 
			   struct mdhim_store_opts_t *mstore_opts) {
	leveldb_readoptions_t *options;
	leveldb_t *db = (leveldb_t *) dbh;
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
	options = (leveldb_readoptions_t *) mstore_opts->db_ptr3;
	old_key = *key;
	old_key_len = *key_len;
	*key = NULL;
	*key_len = 0;

	iter = leveldb_create_iterator(db, options);

	//If the user didn't supply a key, then seek to the last
	if (!old_key || old_key_len == 0) {
		leveldb_iter_seek_to_last(iter);
	} else {
		leveldb_iter_seek(iter, old_key, old_key_len);
		if (!leveldb_iter_valid(iter)) { 
			mlog(MDHIM_SERVER_DBG2, "Could not get a valid iterator in leveldb");
			goto error;
		}

		leveldb_iter_prev(iter);
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
	mlog(MDHIM_SERVER_DBG, "Took: %d seconds to get the prev record", 
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
int mdhim_leveldb_close(void *dbh, void *dbs, struct mdhim_store_opts_t *mstore_opts) {
	leveldb_t *db = (leveldb_t *) dbh;
	leveldb_t *db_s = (leveldb_t *) dbs;

	//Close the databases
	leveldb_close(db);
	leveldb_close(db_s);

	//Destroy the options
	leveldb_comparator_destroy((leveldb_comparator_t *) mstore_opts->db_ptr1);
	leveldb_options_destroy((leveldb_options_t *) mstore_opts->db_ptr2);
	leveldb_readoptions_destroy((leveldb_readoptions_t *) mstore_opts->db_ptr3);
	leveldb_writeoptions_destroy((leveldb_writeoptions_t *) mstore_opts->db_ptr4);
	leveldb_options_destroy((leveldb_options_t *) mstore_opts->db_ptr5);
	leveldb_readoptions_destroy((leveldb_readoptions_t *) mstore_opts->db_ptr6);
	leveldb_writeoptions_destroy((leveldb_writeoptions_t *) mstore_opts->db_ptr7);
	leveldb_filterpolicy_destroy((leveldb_filterpolicy_t *) mstore_opts->db_ptr8);
	leveldb_filterpolicy_destroy((leveldb_filterpolicy_t *) mstore_opts->db_ptr9);
	//	leveldb_cache_destroy((leveldb_cache_t *) mstore_opts->db_ptr10);
	//leveldb_cache_destroy((leveldb_cache_t *) mstore_opts->db_ptr11);

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
	
	options = (leveldb_writeoptions_t *) mstore_opts->db_ptr4;
	leveldb_delete(db, options, key, key_len, &err);
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
