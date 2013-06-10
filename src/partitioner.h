#ifndef      __HASH_H
#define      __HASH_H

#include "mdhim.h"
/* Used to determine if a rank is a range server
   Works like this:
   if myrank % RANGE_SERVER_FACTOR == 0, then I'm a range server 
   if all the keys haven't been covered yet
   
   if the number of ranks is less than the RANGE_SERVER_FACTOR,
   then the last rank will be the range server
*/

#define RANGE_SERVER_FACTOR 2
#define MDHIM_MAX_KEYS 1000000000
//32 bit signed integer
#define MDHIM_INT_KEY 1
//64 bit signed integer
#define MDHIM_LONG_INT_KEY 2
#define MDHIM_FLOAT_KEY 3
#define MDHIM_DOUBLE_KEY 4
#define MDHIM_LONG_DOUBLE_KEY 5
#define MDHIM_STRING_KEY 6

int is_range_server(int rank);
int populate_my_ranges(struct mdhim_t *md);
int get_range_server(struct mdhim_t *md, void *key, int key_len, int key_type);

#endif
