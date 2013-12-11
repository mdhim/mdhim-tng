#include <stdlib.h>
#include <string.h>
#include <sophia.h>
#include <stdio.h>
#include <linux/limits.h>
#include <sys/time.h>
#include "ds_sophia.h"

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

void open_db(void **env, void **db, char *path, int flags) {
	int rc;

	*db = NULL;
	*env = NULL;

	//Create the environment
	*env = sp_env();
	if (*env == NULL) {
		/* memory allocation error */
		mlog(MDHIM_SERVER_CRIT, "Error creating environment while opening sophia database");
		return;
	}
	
	rc = sp_ctl(*env, SPDIR, SPO_CREAT|SPO_RDWR, path);
	if (rc == -1) {
		mlog(MDHIM_SERVER_CRIT, "Error setting sophia environment: %s", sp_error(*env));
		sp_destroy(*env);
		return;
	}
	
	//Open the database
	*db = sp_open(*env);
	if (*db == NULL) {
		mlog(MDHIM_SERVER_CRIT, "Error opening sophia database: %s", sp_error(*env));
		sp_destroy(*env);
		return;
	}

	return;
}

int mdhim_sophia_open(void **dbh, void **dbs, char *path, int flags, 
		      struct mdhim_store_opts_t *mstore_opts) {
	char stats_path[PATH_MAX];
	void *env, *db;

	//Open the main database
	open_db(&env, &db, path, flags);
	if (!env || !db) {
		return MDHIM_DB_ERROR;
	}

	//Set a pointer to the environment
	mstore_opts->db_ptr1 = env;
	//Set the database handle
	*dbh = db;
	
	//Open the stats database
	sprintf(stats_path, "%s_stats", path);
	open_db(&env, &db, stats_path, flags);
	if (!env || !db) {
		return MDHIM_DB_ERROR;
	}

	//Set a pointer to the environment
	mstore_opts->db_ptr2 = env;
	//Set the database handle
	*dbs = db;

	return MDHIM_SUCCESS;
}

int mdhim_sophia_put(void *dbh, void *key, int key_len, void *data, int32_t data_len, 
		     struct mdhim_store_opts_t *mstore_opts) {
	int rc;

	rc = sp_set(dbh, key, key_len, data, data_len);
	if (rc == -1) {
		mlog(MDHIM_SERVER_CRIT, "Error putting key/value in sophiadb");
		return MDHIM_DB_ERROR;
	}

	return MDHIM_SUCCESS;

}
int mdhim_sophia_get(void *dbh, void *key, int key_len, void **data, int32_t *data_len, 
		     struct mdhim_store_opts_t *mstore_opts) {
	int rc;

	rc = sp_get(dbh, key, key_len, data, data_len);
	if (rc == -1) {
		mlog(MDHIM_SERVER_CRIT, "Error getting key/value from sophiadb");
		return MDHIM_DB_ERROR;
	}

	return MDHIM_SUCCESS;
}
int mdhim_sophia_get_next(void *dbh, void **key, int *key_len, 
			  void **data, int32_t *data_len,
			  struct mdhim_store_opts_t *mstore_opts) {
	return MDHIM_SUCCESS;
}

int mdhim_sophia_get_prev(void *dbh, void **key, int *key_len, 
			  void **data, int32_t *data_len,
			  struct mdhim_store_opts_t *mstore_opts) {
	return MDHIM_SUCCESS;
}

int mdhim_sophia_del(void *dbh, void *key, int key_len, 
		     struct mdhim_store_opts_t *mstore_opts) {
	return MDHIM_SUCCESS;
}

int mdhim_sophia_commit(void *dbh) {
	return MDHIM_SUCCESS;
}

int mdhim_sophia_batch_put(void *dbh, void **key, int32_t *key_lens, 
			   void **data, int32_t *data_lens, int num_records,
			   struct mdhim_store_opts_t *mstore_opts) {
	int i;
	int rc;

	for (i = 0; i < num_records; i++) {
		rc = mdhim_sophia_put(dbh, key[i], key_lens[i], data[i], data_lens[i], 
				      mstore_opts);
		if (rc != MDHIM_SUCCESS) {
			return rc;
		}
	}

	return MDHIM_SUCCESS;
}

int mdhim_sophia_close(void *dbh, void *dbs, struct mdhim_store_opts_t *mstore_opts) {
	sp_destroy(dbh);
	sp_destroy(dbs);
	sp_destroy(mstore_opts->db_ptr1);
	sp_destroy(mstore_opts->db_ptr2);

	return MDHIM_SUCCESS;
}
