#define PRIMARY_INDEX
#define SECONDARY_INDEX

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

/* 
 * Remote Index info  
 * Contains information about a remote index
 *
 * A remote index means that an index can be served by one or more range servers
 */
struct remote_index {
	rangesrv_info *rangesrvs; /* The range servers 
				     serving this index */
	int type;                 /* The type of index 
				     (PRIMARY_INDEX, SECONDARY_INDEX) */
	//This communicator is for range servers only to talk to each other
	MPI_Comm rs_comm;   
	//The abstracted data store layer that mdhim uses to store and retrieve records
	struct mdhim_store_t *mdhim_store;
	/* The rank of the range server master that will broadcast stat data to all clients
	   This rank is the rank in mdhim_comm not in the range server communicator */
	int rangesrv_master;
};

/* Local Index info
 * Contains information about a local index
 *
 * A local index means that an index is not served by a range server
 * The index is accessed only by the client that created it.
 */

struct local_index {
	//The abstracted data store layer that mdhim uses to store and retrieve records
	struct mdhim_store_t *mdhim_store;
};

typedef struct index_manifest_t {
	int key_type;   //The type of key 
	int index_type; /* The type of index 
			   (PRIMARY_INDEX, SECONDARY_INDEX) */
	int db_type;
	uint32_t num_rangesrvs;
	int rangesrv_factor;
	uint64_t slice_size; 
	int num_nodes;
} index_manifest_t;
