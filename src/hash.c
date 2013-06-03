#include hash.h


/*
 * is_range_server
 * Tests to see if the given rank is a range server or not
 *
 * @param rank    rank to find out if it is a range server
 * @return        0 if false, 1 if true
 */
int is_range_server(int rank) {

}

/*
 * populate_my_ranges
 *
 * sets the rangesrv_info struct in md->mdhim_rs
 * @param md      main MDHIM struct
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int populate_my_ranges(mdhim_t *md) {

}

/*
 * get_range_server
 *
 * gets the range server that handles the key given
 * @param md       main MDHIM struct
 * @param key      pointer to the key to find the range server of
 * @param key_len  length of the key
 * @return the rank of the range server or MDHIM_ERROR on error
 */
int get_range_server((mdhim_t *md, void *key, int key_len) {

}



