#define PRIMARY_INDEX
#define SECONDARY_INDEX
#define SECONDARY_LOCAL_INDEX

typedef struct rangesrv_info rangesrv_info;
/* 
 * Range server info  
 * Contains information about each range server
 */
struct rangesrv_info {
	//The range server's rank in the mdhim_comm
	uint32_t rank;
	//The range server's identifier based on rank and number of servers
	uint32_t rangesrv_num;
	//The next range server in the list
	rangesrv_info *next;
	//The previous range server in the list
	rangesrv_info *prev;
};


typedef struct index index;
/* 
 * Index info  
 * Contains information about an index
 */
struct index {
	rangesrv_info *rangesrvs; /* The range servers 
				     serving this index (not used for SECONDARY_LOCAL_INDEX) */
	int type;                 /* The type of index 
				     (PRIMARY_INDEX, SECONDARY_INDEX, SECONDARY_LOCAL_INDEX) */
};
