#include <stdlib.h>
#include <string.h>
#include <leveldb/c.h>
#include "ds_leveldb.h"


static void cmp_destroy(void* arg) { }

static int cmp_int_compare(void* arg, const char* a, size_t alen,
			   const char* b, size_t blen) {
	int ret;

	if (*(int32_t *) a < *(int32_t *) b) {
		ret = -1;
	} else if (*(int32_t *) a == *(int32_t *) b) {
		ret = 0;
	} else {
		ret = 1;
	}

	return ret;
}

static int cmp_lint_compare(void* arg, const char* a, size_t alen,
			   const char* b, size_t blen) {
	int ret;

	if (*(int64_t *) a < *(int64_t *) b) {
		ret = -1;
	} else if (*(int64_t *) a == *(int64_t *) b) {
		ret = 0;
	} else {
		ret = 1;
	}

	return ret;
}

static int cmp_double_compare(void* arg, const char* a, size_t alen,
			      const char* b, size_t blen) {
	int ret;

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

	if (*(float *) a < *(float *) b) {
		ret = -1;
	} else if (*(float *) a == *(float *) b) {
		ret = 0;
	} else {
		ret = 1;
	}

	return ret;
}

static int cmp_ldouble_compare(void* arg, const char* a, size_t alen,
			       const char* b, size_t blen) {
	int ret;

	if (*(long double *) a < *(long double *) b) {
		ret = -1;
	} else if (*(long double *) a == *(long double *) b) {
		ret = 0;
	} else {
		ret = 1;
	}
	
	return ret;
}

static int cmp_string_compare(void* arg, const char* a, size_t alen,
			   const char* b, size_t blen) {
	int ret;

	ret = strcmp(a, b);

	return ret;
}

static int cmp_byte_compare(void* arg, const char* a, size_t alen,
			    const char* b, size_t blen) {
	int ret;

	if (alen < blen) {
		ret = -1;
	} else if (alen > blen) {
		ret = 1;
	} else {
		ret = memcmp(a, b, alen);
	}

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

#define MDHIM_INT_KEY 1
//64 bit signed integer
#define MDHIM_LONG_INT_KEY 2
#define MDHIM_FLOAT_KEY 3
#define MDHIM_DOUBLE_KEY 4
#define MDHIM_LONG_DOUBLE_KEY 5
#define MDHIM_STRING_KEY 6
//An arbitrary sized key
#define MDHIM_BYTE_KEY 7
int mdhim_leveldb_open(void **dbh, char *path, int flags, 
		       struct mdhim_store_opts_t *mstore_opts) {
	leveldb_t *db;
	leveldb_options_t *options;
	char *err = NULL;
	leveldb_comparator_t* cmp = NULL;

	//Create the options
	options = leveldb_options_create();
	leveldb_options_set_create_if_missing(options, 1);
	switch( mstore_opts->key_type) {
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
	case MDHIM_LONG_DOUBLE_KEY:
		cmp = leveldb_comparator_create(NULL, cmp_destroy, cmp_ldouble_compare, cmp_name);
		break;
	case MDHIM_STRING_KEY:
		cmp = leveldb_comparator_create(NULL, cmp_destroy, cmp_string_compare, cmp_name);
		break;
	default:
		cmp = leveldb_comparator_create(NULL, cmp_destroy, cmp_byte_compare, cmp_name);
		break;
	}
	
	leveldb_options_set_comparator(options, cmp);

	//Open the database
	db = leveldb_open(options, path, &err);
	//Set the output handle
	*((leveldb_t **) dbh) = db;
	//Set the output comparator
	mstore_opts->db_ptr1 = cmp;
	//Set the generic pointers to hold the options
	mstore_opts->db_ptr2 = options;
	mstore_opts->db_ptr3 = leveldb_readoptions_create();
	mstore_opts->db_ptr4 = leveldb_writeoptions_create();

	if (err != NULL) {
		mlog(MDHIM_SERVER_CRIT, "Error opening leveldb database");
		return MDHIM_DB_ERROR;
	}
	//Reset error variable
	leveldb_free(err); 

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

	options = (leveldb_readoptions_t *) mstore_opts->db_ptr3;
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
	
	return ret;
}

int mdhim_leveldb_get_next(void *dbh, void **key, int *key_len, 
			   void **data, int32_t *data_len, 
			   struct mdhim_store_opts_t *mstore_opts) {
	leveldb_readoptions_t *options;
	leveldb_t *db = (leveldb_t *) dbh;
	int ret = MDHIM_SUCCESS;
	leveldb_iterator_t *iter;
	const char *res1, *res2;
	int len = 0;

	//Init the data to return
	*((char **) data) = NULL;
	*data_len = 0;

	//Create the options and iterator
	options = (leveldb_readoptions_t *) mstore_opts->db_ptr3;
	iter = leveldb_create_iterator(db, options);
	
	//If the user didn't supply a key, then seek to the first
	if (!*((char **) key) || *key_len == 0) {
		leveldb_iter_seek_to_first(iter);
	} else {
		//Otherwise, seek to the key given and then get the next key
		leveldb_iter_seek(iter, *((char **)key), *key_len);
		if (!leveldb_iter_valid(iter)) { 
			mlog(MDHIM_SERVER_CRIT, "Could not get a valid iterator in leveldb after seeking");
			return MDHIM_DB_ERROR;
		}

		leveldb_iter_next(iter);
	}

	if (!leveldb_iter_valid(iter)) {
		mlog(MDHIM_SERVER_CRIT, "Could not get a valid iterator in leveldb");
		return MDHIM_DB_ERROR;
	}

	res1 = leveldb_iter_value(iter, (size_t *) &len);
	if (res1) {
		*((char **) data) = malloc(len);
		memcpy(*((char **) data), res1, len);
		*data_len = len;
	}
	res2 = leveldb_iter_key(iter, (size_t *) key_len);
	if (res2) {
		*((char **) key) = malloc(*key_len);
		memcpy(*((char **) key), res2, *key_len);
	}

	if (!*((char **) data)) {
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
	const char *res1, *res2;
	int len = 0;

	//Init the data to return
	*((char **) data) = NULL;
	*data_len = 0;

	//Create the options and iterator
	options = (leveldb_readoptions_t *) mstore_opts->db_ptr3;
	iter = leveldb_create_iterator(db, options);
	
	//If the user didn't supply a key, then seek to the first
	if (!*((char **) key) || *key_len == 0) {
		leveldb_iter_seek_to_last(iter);
	} else {
		//Otherwise, seek to the key given and then get the next key
		leveldb_iter_seek(iter, *((char **)key), *key_len);
		if (!leveldb_iter_valid(iter)) { 
			mlog(MDHIM_SERVER_CRIT, "Could not get a valid iterator in leveldb after seeking");
			return MDHIM_DB_ERROR;
		}

		leveldb_iter_prev(iter);
	}

	if (!leveldb_iter_valid(iter)) {
		mlog(MDHIM_SERVER_CRIT, "Could not get a valid iterator in leveldb");
		return MDHIM_DB_ERROR;
	}

	res1 = leveldb_iter_value(iter, (size_t *) &len);
	if (res1) {
		*((char **) data) = malloc(len);
		memcpy(*((char **) data), res1, len);
		*data_len = len;
	}
	res2 = leveldb_iter_key(iter, (size_t *) key_len);
	if (res2) {
		*((char **) key) = malloc(*key_len);
		memcpy(*((char **) key), res2, *key_len);
	}

	if (!*((char **) data)) {
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
int mdhim_leveldb_close(void *dbh, struct mdhim_store_opts_t *mstore_opts) {
	leveldb_t *db = (leveldb_t *) dbh;

	leveldb_comparator_destroy((leveldb_comparator_t *) mstore_opts->db_ptr1);
	leveldb_options_destroy((leveldb_options_t *) mstore_opts->db_ptr2);
	leveldb_readoptions_destroy((leveldb_readoptions_t *) mstore_opts->db_ptr3);
	leveldb_writeoptions_destroy((leveldb_writeoptions_t *) mstore_opts->db_ptr4);
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
