/* Used to determine if a rank is a range server
   Works like this:
       if myrank % RANGE_SERVER_FACTOR == 0, then I'm a range server

       if the number of ranks is less than the RANGE_SERVER_FACTOR, 
       then the last rank will be the range server
 */
#define RANGE_SERVER_FACTOR 4  

int is_range_server(int rank);
int populate_my_ranges(mdhim_t *md);
int get_range_server((mdhim_t *md, void *key, int key_len);

