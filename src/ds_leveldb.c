#include <stdlib.h>
#include <string.h>
#include <leveldb/c.h>
#include <stdio.h>
#include <linux/limits.h>
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
 * @param dbh            in   ** to the leveldb handle
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
	//Create the options
	options = leveldb_options_create();
	leveldb_options_set_create_if_missing(options, 1);
	leveldb_options_set_compression(options, 0);
	stat_options = leveldb_options_create();
	leveldb_options_set_create_if_missing(stat_options, 1);
	leveldb_options_set_compression(stat_options, 0);

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
	//Reset error variable
	leveldb_free(err); 

	//Open the stats database
	db = leveldb_open(options, stats_path, &err);
	*((leveldb_t **) dbs) = db;
	if (err != NULL) {
		mlog(MDHIM_SERVER_CRIT, "Error opening leveldb database");
		return MDHIM_DB_ERROR;
	}
	//Reset error variable
	leveldb_free(err); 

	//Set the output comparator
	mstore_opts->db_ptr1 = cmp;
	//Set the generic pointers to hold the options
	mstore_opts->db_ptr2 = options;
	mstore_opts->db_ptr3 = leveldb_readoptions_create();
	mstore_opts->db_ptr4 = leveldb_writeoptions_create();
	mstore_opts->db_ptr5 = stat_options;
	mstore_opts->db_ptr6 = leveldb_readoptions_create();
	mstore_opts->db_ptr7 = leveldb_writeoptions_create();
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

    options = (leveldb_writeoptions_t *) mstore_opts->db_ptr4;    	    
    leveldb_put(db, options, key, key_len, data, data_len, &err);
    if (err != NULL) {
	    mlog(MDHIM_SERVER_CRIT, "Error putting key/value in leveldb");
	    return MDHIM_DB_ERROR;
    }

    //Reset error variable
    leveldb_free(err);        

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
	  leveldb_free(err);        
	  return ret;
	}

	*data_len = ldb_data_len;
	*data = malloc(*data_len);
	memcpy(*data, ldb_data, *data_len);

	//Reset error variable
	leveldb_free(err);        
	
	return ret;
}

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

	//Init the data to return
	*data = NULL;
	*data_len = 0;

	//Create the options and iterator
	options = (leveldb_readoptions_t *) mstore_opts->db_ptr3;
	iter = leveldb_create_iterator(db, options);
	old_key = *key;
	old_key_len = *key_len;
	*key = NULL;
	*key_len = 0;

	//If the user didn't supply a key, then seek to the first
	if (!old_key || old_key_len == 0) {
		leveldb_iter_seek_to_first(iter);
	} else {
		//Otherwise, seek to the key given and then get the next key
		leveldb_iter_seek(iter, old_key, old_key_len);
		if (!leveldb_iter_valid(iter)) { 
			mlog(MDHIM_SERVER_DBG2, "Could not get a valid iterator in leveldb after seeking");
			leveldb_iter_destroy(iter);      
			return MDHIM_DB_ERROR;
		}
	
		leveldb_iter_next(iter);
	}

	if (!leveldb_iter_valid(iter)) {
		mlog(MDHIM_SERVER_DBG2, "Could not get a valid iterator in leveldb");
		*key = NULL;
		*key_len = 0;
		*data = NULL;
		*data_len = 0;
		leveldb_iter_destroy(iter);      
		return MDHIM_DB_ERROR;
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
		ret = MDHIM_DB_ERROR;
	}

        //Destroy iterator
	leveldb_iter_destroy(iter);      
	
	return ret;
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

	//Init the data to return
	*((char **) data) = NULL;
	*data_len = 0;

	//Create the options and iterator
	options = (leveldb_readoptions_t *) mstore_opts->db_ptr3;
	iter = leveldb_create_iterator(db, options);
	old_key = (void *) *((char **) key);
	old_key_len = *key_len;
	*((char **) key) = NULL;
	*key_len = 0;

	//If the user didn't supply a key, then seek to the last
	if (!old_key || old_key_len == 0) {
		leveldb_iter_seek_to_last(iter);
	} else {
		//Otherwise, seek to the key given and then get the next key
		leveldb_iter_seek(iter, old_key, old_key_len);
		if (!leveldb_iter_valid(iter)) { 
			mlog(MDHIM_SERVER_DBG2, "Could not get a valid iterator in leveldb after seeking");
			leveldb_iter_destroy(iter);      
			return MDHIM_DB_ERROR;
		}
	
		leveldb_iter_prev(iter);
	}

	if (!leveldb_iter_valid(iter)) {
		mlog(MDHIM_SERVER_DBG2, "Could not get a valid iterator in leveldb");
		*key = NULL;
		*key_len = 0;
		*data = NULL;
		*data_len = 0;
		leveldb_iter_destroy(iter);      
		return MDHIM_DB_ERROR;
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
		ret = MDHIM_DB_ERROR;
	}

        //Destroy iterator
	leveldb_iter_destroy(iter);      

	return ret;
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
int mdhim_leveldb_close(void *dbh, void *dbs, struct mdhim_store_opts_t *mstore_opts) {
	leveldb_t *db = (leveldb_t *) dbh;

	leveldb_comparator_destroy((leveldb_comparator_t *) mstore_opts->db_ptr1);
	leveldb_options_destroy((leveldb_options_t *) mstore_opts->db_ptr2);
	leveldb_readoptions_destroy((leveldb_readoptions_t *) mstore_opts->db_ptr3);
	leveldb_writeoptions_destroy((leveldb_writeoptions_t *) mstore_opts->db_ptr4);
	leveldb_options_destroy((leveldb_options_t *) mstore_opts->db_ptr5);
	leveldb_readoptions_destroy((leveldb_readoptions_t *) mstore_opts->db_ptr6);
	leveldb_writeoptions_destroy((leveldb_writeoptions_t *) mstore_opts->db_ptr7);
	leveldb_close(db);
	
	db = (leveldb_t *) dbs;
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
	
	options = (leveldb_writeoptions_t *) mstore_opts->db_ptr4;
	leveldb_delete(db, options, key, key_len, &err);
	if (err != NULL) {
		mlog(MDHIM_SERVER_CRIT, "Error deleting key in leveldb");
		return MDHIM_DB_ERROR;
	}

	//Reset error variable
	leveldb_free(err); 	

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
