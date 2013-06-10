#include <math.h>
#include <stdlib.h>
#include "partitioner.h"

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
int populate_my_ranges(struct mdhim_t *md) {

}

/*
 * get_range_server
 *
 * gets the range server that handles the key given
 * @param md        main MDHIM struct
 * @param key       pointer to the key to find the range server of
 * @param key_len   length of the key
 * @param key_type  type of the key
 * @return the rank of the range server or MDHIM_ERROR on error
 */
uint32_t get_range_server(struct mdhim_t *md, void *key, int key_len, int key_type) {
	//The number of keys held per range server
	uint32_t split;
	//The number that maps a key to range server (dependent on key type)
	uint64_t key_num;
	//The range server number that we return
	uint32_t rangesrv_num;
	int i;

	/* Find the number of keys per range server
	   The last range server could have a little bit less than the others */
	split = ceil((double) md->max_keys/md->num_rangesrvs);
	
	//Perform key dependent algorithm to get the range server number
	switch(key_type) {
	case MDHIM_INT_KEY:
		//Convert the key to a signed 32 bit integer and take the absolute value
		key_num = (uint64_t) abs(*((int32_t *) key));
		break;
	case MDHIM_LONG_INT_KEY:
		//Convert the key to a signed 64 bit integer and take the absolute value
		key_num = (uint64_t) abs(*((int64_t *) key));
		break;
	case MDHIM_FLOAT_KEY:
		//Convert the key to a float and take the absolute value
		key_num = (uint64_t) fabsf(ceil(*((float *) key)));
		break;
	case MDHIM_DOUBLE_KEY:
		key_num = (uint64_t) fabs(ceil(*((float *) key)));
		break;
	case MDHIM_LONG_DOUBLE_KEY:
		key_num = (uint64_t) fabsl(*((uint32_t *) key));
		break;
	case MDHIM_STRING_KEY:
		key_num = 0;
		for (i = 0; i < key_len; i++) {
			key_num += (int) ((char *)key)[i];
		}
		break;
	default:
		return MDHIM_ERROR;
		break;
	}
	
	rangesrv_num = key_num % md->num_rangesrvs;
	return rangesrv_num;
}



