#include "ds_unqlite.h"

/*
 * mdhim_unqlite_open
 * Stores a single key in the data store
 *
 * @param mstore         in   pointer to the mdhim storage abstraction
 * @param path           in   path to the database file
 * @param flags          in   flags for opening the data store
 * @param mstore_opts    in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_unqlite_open(struct mdhim_store_t *mstore, char *path, int flags, struct mdhim_store_opts_t *mstore_opts) {
}

/*
 * mdhim_unqlite_put
 * Stores a single key in the data store
 *
 * @param mstore      in   pointer to the mdhim storage abstraction
 * @param key         in   void * to the key to store
 * @param key_len     in   length of the key
 * @param data        in   void * to the value of the key
 * @param data_len    in   length of the value data 
 * @param mstore_opts in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_unqlite_put(struct mdhim_store_t *mstore, void *key, int key_len, void *data, int data_len, 
		      struct mdhim_store_opts_t *mstore_opts) {
}

/*
 * mdhim_unqlite_bput
 * Bulk put
 * Stores multiple keys in the data store
 *
 * @param mstore      in   pointer to the mdhim storage abstraction
 * @param key         in   void ** for the keys to store
 * @param key_len     in   int * for the lengths of the keys
 * @param data        in   void ** to the values of the keys
 * @param data_len    in   int * for the lengths of the value data 
 * @param mstore_opts in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_unqlite_bput(struct mdhim_store_t *mstore, void **keys, int *key_lens, void **data, int *data_lens, 
		       struct mdhim_store_opts_t *mstore_opts) {
}

/*
 * mdhim_unqlite_get
 * Gets a value, given a key, from the data store
 *
 * @param mstore       in   pointer to the mdhim storage abstraction
 * @param keys         in   void * to the key to retrieve the value of
 * @param key_lens     in   length of the key
 * @param data         out  void * to the value of the key
 * @param data_lens    out  length of the value data 
 * @param mstore_opts  in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_unqlite_get(struct mdhim_store_t *mstore, void *key, int key_len, void *data, int data_len, 
		      struct mdhim_store_opts_t *mstore_opts) {
}

/*
 * mdhim_unqlite_bget
 * Gets an array of values, given an array of keys, from the data store
 *
 * @param mstore      in   pointer to the mdhim storage abstraction
 * @param keys        in   void ** for the keys to retrieve the values of
 * @param key_lens    in   lengths of the keys
 * @param data        out  void ** for the values of the keys
 * @param data_lens   out  int * for the lengths of the values 
 * @param mstore_opts in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_unqlite_bget(struct mdhim_store_t *mstore, void **keys, int *key_lens, void **data, int *data_lens, 
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
 * @param mstore      in   pointer to the mdhim storage abstraction 
 * @param mstore_opts in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_unqlite_close(struct mdhim_store_t *mstore, struct mdhim_store_opts_t *mstore_opts) {
}
