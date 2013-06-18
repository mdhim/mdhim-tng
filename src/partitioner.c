#include <math.h>
#include <stdlib.h>
#include "partitioner.h"

/*
 * partitioner_init
 * Initializes portions of the mdhim_t struct dealing with the partitioner
 *
 * @param md      main MDHIM struct
 * @return        MDHIM_ERROR on error, otherwise the number of range servers
 */

void partitioner_init(struct mdhim_t *md) {
	uint32_t num_rangesrvs;
	float reasonable_servers;

	/* Set the max and minimum keys
	   Eventually this will be configurable */
	md->max_key = MDHIM_MAX_KEY;
	md->min_key = MDHIM_MIN_KEY;
	
	//Figure out how many range servers we could have based on the range server factor
	num_rangesrvs = get_num_range_servers(md);

	//Figure what makes sense as far as the number of servers
	reasonable_servers = (md->max_key + llabs(md->min_key)) * .01f;

	//If the reasonable number of servers is exceed by num_rangesrvs 
	if (reasonable_servers > num_rangesrvs) {
		num_rangesrvs = ceil(reasonable_servers);
	}
	
	md->num_rangesrvs = num_rangesrvs;
	return;
}

/*
 * delete_alphabet
 * Deletes the alphabet hash table
 */
void delete_alphabet() {
	struct mdhim_char *cur_char, *tmp;
	HASH_ITER(hh, mdhim_alphabet, cur_char, tmp) {
		HASH_DEL(mdhim_alphabet, cur_char);  /*delete it (mdhim_alphabet advances to next)*/
		free(cur_char);            /* free it */
	}
}

/*
 * partitioner_release
 * Releases memory in use by the partitioner
 *
 */
void partitioner_release() {
	delete_alphabet();
}

/*
 * add_char
 * Adds a character to our alphabet hash table
 *
 * @param id      The id of our entry (the ascii code of the character)
 * @param pos     The value of our entry (the position of the character in our alphabet)
 */
void add_char(int id, int pos) {
	struct mdhim_char *mc;

	//Create a new mdhim_char to hold our entry
	mc = malloc(sizeof(struct mdhim_char));

	//Set the mdhim_char
	mc->id = id;
	mc->pos = pos;

	//Add it to the hash table
	HASH_ADD_INT(mdhim_alphabet, id, mc);    

	return;
}

/*
 * build_alphabet
 * Creates our ascii based alphabet and inserts each character into a uthash table
 */
void build_alphabet() {
	char c;
	int i, indx;

        /* Index of the character in the our alphabet
	   This is to number each character we care about so we can map 
	   a string to a range server 

	   0 - 9 have indexes 0 - 9
	   A - Z have indexes 10 - 35
	   a - z have indexes 36 - 61
	*/
	indx = 0;

	//Start with numbers 0 - 9
	c = '0';	
	for (i = (int) c; i <= (int) '9'; i++) {
		add_char(i, indx);
		indx++;
	}

	//Next deal with A-Z
	c = 'A';	
	for (i = (int) c; i <= (int) 'Z'; i++) {
		add_char(i, indx);
		indx++;
	}

        //Next deal with a-z
	c = 'a';	
	for (i = (int) c; i <= (int) 'z'; i++) {
		add_char(i, indx);
		indx++;
	}

	return;
}

/*
 * verify_key
 * Determines whether the given key is a valid key or not
 *
 * @param key      the key to check
 * @param key_len  the length of the key
 * @param key_type the type of the key
 *
 * @return        MDHIM_ERROR if the key is not valid, otherwise the MDHIM_SUCCESS
 */
int verify_key(void *key, int key_len, int key_type) {
	int i;
	int id;
	struct mdhim_char *mc;

	if (key_type != MDHIM_STRING_KEY) {
		if (key_len > sizeof(int64_t)) {
			return MDHIM_ERROR;
		}
	} else {
		for (i = 0; i < key_len; i++) {
			//Ignore null terminating char
			if (i == key_len - 1 && ((char *)key)[i] == '\0') {
				break;
			}

			id = (int) ((char *)key)[i];
			HASH_FIND_INT(mdhim_alphabet, &id, mc);
			if (!mc) {
				return MDHIM_ERROR;
			}
		}
	}

	return MDHIM_SUCCESS;
}

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
	int ret;

	if ((ret = MPI_Comm_size(md->mdhim_comm, &size)) != MPI_SUCCESS) {
		mlog(MPI_EMERG, "Rank: %d - Couldn't get the size of the comm in get_num_range_servers", 
		     md->mdhim_rank);
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
	uint64_t rangesrv_num = 0;

	if ((ret = MPI_Comm_size(md->mdhim_comm, &size)) != MPI_SUCCESS) {
		mlog(MPI_EMERG, "Rank: %d - Couldn't get the size of the comm in is_range_server", 
		     md->mdhim_rank);
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
      		
	if (rangesrv_num > md->num_rangesrvs) {
		rangesrv_num = 0;
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
	uint64_t split;
	uint64_t rangesrv_num;
	uint64_t start_range;
	uint64_t end_range;

	//There was an error figuring out if I'm a range server
	if ((rangesrv_num = is_range_server(md, md->mdhim_rank)) == MDHIM_ERROR) {
		return MDHIM_ERROR;		
	}

	//I'm not a range server
	if (!rangesrv_num) {
		return MDHIM_SUCCESS;
	}

	/* Find the number of keys per range server
	   The last range server could have a little bit less than the others */
	split = (uint64_t) ceil((double) (md->max_key + llabs(md->min_key))/md->num_rangesrvs);

	//Get my start range
	start_range = split * (rangesrv_num - 1);
	//Get my end range
	end_range = (split * rangesrv_num) - 1;
	//Populate the rangesrv_info structure
	md->mdhim_rs->info.rank = md->mdhim_rank;
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
	rangesrv_info *rp;
	int32_t ikey;
	int64_t likey;
	float fkey;
	double dkey;
	long double ldkey;
	int ret, i;
	int id;
	struct mdhim_char *mc;
	double str_num;
	uint64_t total_keys;
	
	//The total number of keys we can hold in all range servers combined
	total_keys = (uint64_t)MDHIM_MAX_KEY + (uint64_t)llabs(MDHIM_MIN_KEY);

	//Make sure this key is valid
	if ((ret = verify_key(key, key_len, key_type)) != MDHIM_SUCCESS) {
		mlog(MDHIM_CLIENT_INFO, "Rank: %d - Invalid key given to get_range_server()", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}

	//Perform key dependent algorithm to get the key in terms of the ranges served
	switch(key_type) {
	case MDHIM_INT_KEY:
		//Convert the key to a signed 32 bit integer
		ikey = *((int32_t *) key);
		if (ikey < 0) {
			key_num = abs(ikey);
		} else {
			key_num = ikey * 2;
		}

		break;
	case MDHIM_LONG_INT_KEY:
		//Convert the key to a signed 64 bit integer
		likey = *((int64_t *) key);
		if (likey < 0) {
			key_num = llabs(likey);
		} else {
			key_num = likey * 2;
		}

		break;
	case MDHIM_FLOAT_KEY:
		//Convert the key to a float
		fkey = *((float *) key);
		fkey = floor(fkey);
		if (fkey < 0) {
			key_num = fabsf(fkey);
		} else {
			key_num = fkey * 2;
		}

		break;
	case MDHIM_DOUBLE_KEY:
		//Convert the key to a double
		dkey = *((double *) key);
		dkey = floor(dkey);
		if (dkey < 0) {
			key_num = fabs(dkey);
		} else {
			key_num = dkey * 2;
		}

		break;
	case MDHIM_LONG_DOUBLE_KEY:
		//Convert the key to a long double
		ldkey = *((float *) key);
		ldkey = floor(ldkey);
		if (ldkey < 0) {
			key_num = fabsl(ldkey);
		} else {
			key_num = ldkey * 2;
		}

		break;
	case MDHIM_STRING_KEY:
		/* Algorithm used
		   
		   1. Iterate through each character
		   2. Transform each character into a floating point number
		   3. Add this floating point number to str_num
		   4. Multiply this number times the total number of keys to get the number 
		      that represents the position in a range

		   For #2, the transformation is as follows:
		   
		   Take the position of the character in the mdhim alphabet 
                   times 2 raised to the MDHIM_ALPHABET_EXPONENT * -(i + 1) 
		   where i is the current iteration in the loop
		*/		
 
                //Used for calculating the range server to use for this string
		str_num = 0;

		//Iterate through each character to perform the algorithm mentioned above
		for (i = 0; i < key_len; i++) {
			//Ignore null terminating char
			if (i == key_len - 1 && ((char *)key)[i] == '\0') {
				break;
			}

			id = (int) ((char *)key)[i];
			HASH_FIND_INT(mdhim_alphabet, &id, mc);
			str_num += mc->pos * pow(2, MDHIM_ALPHABET_EXPONENT * -(i + 1));			
		}

		key_num = floor(str_num * total_keys);
		break;
	default:
		return MDHIM_ERROR;
		break;
	}

	if (key_num > total_keys) {
		mlog(MPI_EMERG, "Rank: %d - Key is larger than any of the ranges served" 
		     " in get_range_server", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
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



