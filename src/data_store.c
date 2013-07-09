/*
 * MDHIM TNG
 * 
 * Data store abstraction
 */

#include <stdlib.h>
#include "data_store.h"
#include "ds_unqlite.h"

/**
 * mdhim_db_init
 * Initializes mdhim_store_t structure based on type
 *
 * @param type           in   Database store type to use (i.e., UNQLITE, LEVELDB, etc)
 * @return mdhim_store_t      The mdhim storage abstraction struct
 */


struct mdhim_store_t *mdhim_db_init(int type) {
	struct mdhim_store_t *store;
	
	//UNQLITE is the only data store supported at this time
	if (type != UNQLITE) {
		return NULL;
	}

	//Initialize the store structure
	store = malloc(sizeof(struct mdhim_store_t));
	store->type = type;
	store->db_handle = NULL;

	switch(type) {
	case UNQLITE:
		store->open = mdhim_unqlite_open;
		store->put = mdhim_unqlite_put;
		store->get = mdhim_unqlite_get;
		store->get_next = mdhim_unqlite_get_next;
		store->get_prev = mdhim_unqlite_get_prev;
		store->del = mdhim_unqlite_del;
		store->cursor_init = mdhim_unqlite_cursor_init;
		store->cursor_release = mdhim_unqlite_cursor_release;
		store->commit = mdhim_unqlite_commit;
		store->close = mdhim_unqlite_close;
		break;
	default:
		break;
	}
	
	return store;
}

