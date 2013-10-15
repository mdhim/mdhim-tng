/*
 * MDHIM TNG
 * 
 * Data store abstraction
 */

#include <stdlib.h>
#include "data_store.h"
#include "ds_unqlite.h"
#include "ds_leveldb.h"

/**
 * mdhim_db_init
 * Initializes mdhim_store_t structure based on type
 *
 * @param type           in   Database store type to use (i.e., LEVELDB, etc)
 * @return mdhim_store_t      The mdhim storage abstraction struct
 */
struct mdhim_store_t *mdhim_db_init(int type) {
	struct mdhim_store_t *store;
	
	//Initialize the store structure
	store = malloc(sizeof(struct mdhim_store_t));
	store->type = type;
	store->db_handle = NULL;
	store->db_ptr1 = NULL;
	store->db_ptr2 = NULL;
	store->db_ptr3 = NULL;
	store->db_ptr4 = NULL;
	store->db_ptr5 = NULL;
	store->db_ptr6 = NULL;
	store->db_ptr7 = NULL;
	store->db_ptr8 = NULL;
	store->db_ptr9 = NULL;
	store->db_ptr10 = NULL;
	store->db_ptr11 = NULL;
	store->db_ptr12 = NULL;
	store->db_ptr13 = NULL;
	store->db_ptr14 = NULL;
	store->mdhim_store_stats = NULL;
	switch(type) {
	case LEVELDB:
		store->open = mdhim_leveldb_open;
		store->put = mdhim_leveldb_put;
		store->get = mdhim_leveldb_get;
		store->get_next = mdhim_leveldb_get_next;
		store->get_prev = mdhim_leveldb_get_prev;
		store->del = mdhim_leveldb_del;
		store->iter_free = mdhim_leveldb_iter_free;
		store->commit = mdhim_leveldb_commit;
		store->close = mdhim_leveldb_close;
		break;
	default:
		free(store);
		store = NULL;
		break;
	}
	
	return store;
}


