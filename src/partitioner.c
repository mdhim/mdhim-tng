#include <math.h>
#include <stdlib.h>
#include "partitioner.h"

/*
 * get_num_range_servers
 * Gets the number of range servers
 *
 * @param md      main MDHIM struct
 * @return        MDHIM_ERROR on error, otherwise the number of range servers
 */
uint32_t get_num_range_servers(struct mdhim_t *md) {
	int size;
	uint32_t num_servers = 0;
	int i = 0;

	if ((ret = MPI_Comm_size(md->comm, &size)) != MPI_SUCCESS) {
		mlog(MPI_EMERG, "Rank: %d - Couldn't get the size of the comm in get_num_range_servers", 
		     md->rank);
		return MDHIM_ERROR;
	}

	/* Get the number of range servers */
	if (size - 1 < RANGE_SERVER_FACTOR) {
		//The size of the communicator is less than the RANGE_SERVER_FACTOR
		return 1;
	} 
	
	//Figure out the number of range servers, details on the algorithm are in is_range_server
	for (i = 0; i < size; i++) {
		if (i % RANGE_SERVER_FACTOR == 0) {
			num_servers++;
		}
	}
      	
	return num_servers;
}

/*
 * is_range_server
 * Tests to see if the given rank is a range server or not
 *
 * @param md      main MDHIM struct
 * @param rank    rank to find out if it is a range server
 * @return        MDHIM_ERROR on error, 0 on false, 1 or greater to represent the range server number otherwise
 */
uint32_t is_range_server(struct mdhim_t *md, int rank) {
	int size;
	int ret;
	uint32_t rangesrv_num = 0;

	if ((ret = MPI_Comm_size(md->comm, &size)) != MPI_SUCCESS) {
		mlog(MPI_EMERG, "Rank: %d - Couldn't get the size of the comm in is_range_server", 
		     md->rank);
		return MDHIM_ERROR;
	}

	/* Get the range server number, which is just a number from 1 onward
	   It represents the ranges the server serves and is calculated with the RANGE_SERVER_FACTOR
	   
	   The RANGE_SERVER_FACTOR is a number that is divided by the rank such that if the 
	   remainder is zero, then the rank is a rank server

	   For example, if there were 8 ranks and the RANGE_SERVER_FACTOR is 2, 
	   then ranks: 2, 4, 6, 8 are range servers

	   If the size of communicator is less than the RANGE_SERVER_FACTOR, 
	   the last rank is the range server
	*/

	size -= 1;
	if (size < RANGE_SERVER_FACTOR && rank == size) {
		//The size of the communicator is less than the RANGE_SERVER_FACTOR
		rangesrv_num = 1;
	} else if (rank % RANGE_SERVER_FACTOR == 0) {
		//This is a range server, get the range server's number
		rangesrv_num = rank / RANGE_SERVER_FACTOR;
	}
      	
	return rangesrv_num;
}

/*
 * populate_my_ranges
 *
 * sets the rangesrv_info struct in md->mdhim_rs
 * @param md      main MDHIM struct
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int populate_my_ranges(struct mdhim_t *md) {
	//The number of keys held per range server
	uint32_t split;
	uint32_t rangesrv_num;
	uint32_t start_range;
	uint32_t end_range;

	//There was an error figuring out if I'm a range server
	if ((rangesrv_num = is_range_server(md, md->rank)) == MDHIM_ERROR) {
		return MDHIM_ERROR;		
	}

	//I'm not a range server
	if (!rangesrv_num) {
		return MDHIM_SUCCESS;
	}

	/* Find the number of keys per range server
	   The last range server could have a little bit less than the others */
	split = ceil((double) md->max_keys/md->num_rangesrvs);

	//Get my start range
	start_range = split * (rangesrv_num - 1);
	//Get my end range
	end_range = (split * rangesrv_num) - 1;
	//Populate the rangesrv_info structure
	md->mdhim_rs->info.rank = md->rank;
	md->mdhim_rs->info.start_range = start_range;
	md->mdhim_rs->info.end_range = end_range;
	md->mdhim_rs->info.next = NULL;
	md->mdhim_rs->info.prev = NULL;

	return MDHIM_SUCCESS;
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
	//The number that maps a key to range server (dependent on key type)
	uint64_t key_num;
	//The range server number that we return
	uint32_t rangesrv_rank = MDHIM_ERROR;
	int i;
	rangesrv_info *rp;
	
	//Perform key dependent algorithm to get the key in terms of the ranges served
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
	
	//Iterate through the range servers to find one, if any, that serves the key we have
	rp = md->rangesrvs;
	while (rp) {
		if (key_num >= rp->start_range && key_num <= rp->end_range) {
			rangesrv_rank = rp->rank;
			break;
		}
		
		rp = rp->next;
	}
       
	return rangesrv_rank;
}



