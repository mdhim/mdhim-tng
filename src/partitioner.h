#ifndef      __HASH_H
#define      __HASH_H

#include "mdhim.h"
#include "uthash.h"
#include "indexes.h"

/* Used to determine if a rank is a range server
   Works like this:
   if myrank % RANGE_SERVER_FACTOR == 0, then I'm a range server 
   if all the keys haven't been covered yet
   
   if the number of ranks is less than the RANGE_SERVER_FACTOR,
   then the last rank will be the range server
*/

#ifdef __cplusplus
extern "C"
{
#endif
//#define RANGE_SERVER_FACTOR 4 // NOW a global variable in partitioner.h
#define MDHIM_MAX_SLICES 2147483647
//32 bit unsigned integer
#define MDHIM_INT_KEY 1
#define MDHIM_LONG_INT_KEY 2
#define MDHIM_FLOAT_KEY 3
#define MDHIM_DOUBLE_KEY 4
#define MDHIM_STRING_KEY 5
//An arbitrary sized key
#define MDHIM_BYTE_KEY 6

//Maximum length of a key
#define MAX_KEY_LEN 1048576

/* The exponent used for the algorithm that determines the range server

   This exponent, should cover the number of characters in our alphabet 
   if 2 is raised to that power. If the exponent is 6, then, 64 characters are covered 
*/
#define MDHIM_ALPHABET_EXPONENT 6  

//Used for hashing strings to the appropriate range server
struct mdhim_char {
    int id;            /* we'll use this field as the key */
    int pos;             
    UT_hash_handle hh; /* makes this structure hashable */
};

typedef struct rangesrv_list rangesrv_list;
struct rangesrv_list {
	rangesrv_info *ri;
	rangesrv_list *next;
};

int max_rangesrvs;
void partitioner_init();
void partitioner_release();
rangesrv_list *get_range_servers(struct mdhim_t *md, struct index_t *index,
				 void *key, int key_len);
rangesrv_info *get_range_server_by_slice(struct mdhim_t *md, 
					 struct index_t *index, int slice);
void build_alphabet();
int verify_key(struct index_t *index, void *key, int key_len, int key_type);
long double get_str_num(void *key, uint32_t key_len);
long double get_byte_num(void *key, uint32_t key_len);
int get_slice_num(struct mdhim_t *md, struct index_t *index, void *key, int key_len);
int is_float_key(int type);
rangesrv_list *get_range_servers_from_stats(struct mdhim_t *md, struct index_t *index, 
					    void *key, int key_len, int op);

#ifdef __cplusplus
}
#endif
#endif
