#include <math.h>
#include <stdlib.h>
#include "partitioner.h"

//Global hashtable for alphabet used in partitioner algorithm
struct mdhim_char *mdhim_alphabet = NULL;

/**
 * partitioner_init
 * Initializes portions of the mdhim_t struct dealing with the partitioner
 *
 * @param md      main MDHIM struct
 * @return        MDHIM_ERROR on error, otherwise the number of range servers
 */

void partitioner_init(struct mdhim_t *md) {
	uint32_t num_rangesrvs;
	uint32_t max_servers;

	//Figure out how many range servers we could have based on the range server factor
	num_rangesrvs = get_num_range_servers(md);

	//Figure what makes sense as far as the number of servers
	max_servers = MDHIM_MAX_SERVERS;

	//If the reasonable number of servers is exceed by num_rangesrvs 
	if (num_rangesrvs > max_servers) {
		num_rangesrvs = max_servers;
	}
        
        // Create the alphabet for string keys
        build_alphabet();
	
	md->num_rangesrvs = num_rangesrvs;
	return;
}

/**
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

long double get_str_num(void *key, uint32_t key_len) {
	int id, i;
	struct mdhim_char *mc;
	long double str_num;

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

	return str_num;
}

long double get_byte_num(void *key, uint32_t key_len) {
	int i, val;
	long double byte_num;

	byte_num = 0;
	//Iterate through each character to perform the algorithm mentioned above
	for (i = 0; i < key_len; i++) {
		val = ((char *) key)[i];		       
		byte_num += val * pow(2, 8 * -(i + 1));			
	}

	return byte_num;
}

/*
 * partitioner_release
 * Releases memory in use by the partitioner
 *
 */
void partitioner_release() {
	delete_alphabet();
}

/**
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

/**
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

/**
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

	if (key_len > MAX_KEY_LEN) {
		return MDHIM_ERROR;
	}
	if (key_type == MDHIM_STRING_KEY) {
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

/**
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

/**
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

	//Rank zero can't be a range server so it can do maintenance later
	if (!rank) {
		return rangesrv_num;
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

/**
 * get_range_server
 *
 * gets the range server that handles the key given
 * @param md        main MDHIM struct
 * @param key       pointer to the key to find the range server of
 * @param key_len   length of the key
 * @param key_type  type of the key
 * @return the rank of the range server or MDHIM_ERROR on error
 */
rangesrv_info *get_range_server(struct mdhim_t *md, void *key, int key_len) {
	//The number that maps a key to range server (dependent on key type)
	uint64_t key_num;
	//The range server number that we return
	rangesrv_info *rp, *ret_rp;
	uint32_t ikey;
	uint64_t likey;
	float fkey;
	double dkey;
	long double ldkey;
	int ret;
	long double map_num;
	uint64_t total_keys;
	int key_type = md->key_type;

	//The total number of keys we can hold in all range servers combined
	total_keys = (uint64_t)MDHIM_MAX_KEYS;

	//Make sure this key is valid
	if ((ret = verify_key(key, key_len, key_type)) != MDHIM_SUCCESS) {
		mlog(MDHIM_CLIENT_INFO, "Rank: %d - Invalid key given to get_range_server()", 
		     md->mdhim_rank);
		return NULL;
	}

	//Perform key dependent algorithm to get the key in terms of the ranges served
	switch(key_type) {
	case MDHIM_INT_KEY:
		//Convert the key to a signed 32 bit integer
		ikey = *((uint32_t *) key);	
		key_num = ikey;

		break;
	case MDHIM_BYTE_KEY:
		/* Algorithm used		   
		   1. Iterate through each byte
		   2. Transform each byte into a floating point number
		   3. Add this floating point number to map_num
		   4. Multiply this number times the total number of keys to get the number 
		      that represents the position in a range

		   For #2, the transformation is as follows:
		   
		   Take the position of the character in the mdhim alphabet 
                   times 2 raised to 8 * -(i + 1) 
		   where i is the current iteration in the loop
		*/		
 
                //Used for calculating the range server to use for this string
		map_num = 0;
		map_num = get_byte_num(key, key_len);	
		key_num = floorl(map_num * total_keys);

		break;
	case MDHIM_LONG_INT_KEY:
		//Convert the key to a signed 64 bit integer
		likey = *((uint64_t *) key);
		key_num = likey;

		break;
	case MDHIM_FLOAT_KEY:
		//Convert the key to a float
		fkey = *((float *) key);
		fkey = floor(fabsf(fkey));
		key_num = fkey;

		break;
	case MDHIM_DOUBLE_KEY:
		//Convert the key to a double
		dkey = *((double *) key);
		dkey = floor(fabs(dkey));
		key_num = dkey;

		break;
	case MDHIM_LONG_DOUBLE_KEY:
		//Convert the key to a long double
		ldkey = *((long double *) key);
		ldkey = floor(fabsl(ldkey));
		key_num = ldkey;

		break;
	case MDHIM_STRING_KEY:
		/* Algorithm used
		   
		   1. Iterate through each character
		   2. Transform each character into a floating point number
		   3. Add this floating point number to map_num
		   4. Multiply this number times the total number of keys to get the number 
		      that represents the position in a range

		   For #2, the transformation is as follows:
		   
		   Take the position of the character in the mdhim alphabet 
                   times 2 raised to the MDHIM_ALPHABET_EXPONENT * -(i + 1) 
		   where i is the current iteration in the loop
		*/		
 
                //Used for calculating the range server to use for this string
		map_num = 0;
		map_num = get_str_num(key, key_len);	
		key_num = floorl(map_num * total_keys);
		break;
	default:
		return NULL;
		break;
	}


	/* If we only have one range server, 
	   the range sever to return is the one with range server number 1 */
	if (md->num_rangesrvs == 1) {
		key_num = 1;
	} else {
		/* Convert the key to a range server number, which is a number 1 - N, 
		   where N is the number of range servers */
		key_num = ((uint32_t) ceil((long double) (key_num/MDHIM_MAX_RECS_PER_SLICE))) % 
			(md->num_rangesrvs - 1);
		key_num++;
	}

	//Match the range server number of the key with the range server we have in our list
	ret_rp = NULL;
	rp = md->rangesrvs;
	while (rp) {
		if (key_num == rp->rangesrv_num) {
			ret_rp = rp;
			break;
		}
		
		rp = rp->next;
	}
       
	//Return the rank
	return ret_rp;
}



