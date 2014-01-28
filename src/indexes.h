/*
 * MDHIM TNG
 * 
 * Index abstraction
 */

#ifndef      __INDEX_H
#define      __INDEX_H

#include "uthash.h"

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
	UT_hash_handle hh;         /* makes this structure hashable */
};

/* 
 * Remote Index info  
 * Contains information about a remote index
 *
 * A remote index means that an index can be served by one or more range servers
 */
struct remote_index {
	int id;
	//The abstracted data store layer that mdhim uses to store and retrieve records
	struct mdhim_store_t *mdhim_store;
	int key_type;             //The key type used in the db
	int db_type;              //The database type
	int type;                 /* The type of index 
				     (PRIMARY_INDEX, SECONDARY_INDEX, LOCAL_INDEX) */
	rangesrv_info *rangesrvs; /* The range servers 
				     serving this index */

        //Used to determine the number of range servers which is based in  
        //if myrank % RANGE_SERVER_FACTOR == 0, then myrank is a server
	int range_server_factor;
	
        //Maximum size of a slice. A range server may serve several slices.
	uint64_t mdhim_max_recs_per_slice; 

	//This communicator is for range servers only to talk to each other
	MPI_Comm rs_comm;   
	/* The rank of the range server master that will broadcast stat data to all clients
	   This rank is the rank in mdhim_comm not in the range server communicator */
	int rangesrv_master;

	//The number of range servers for this index
	uint32_t num_rangesrvs;

	//The hash table of range servers
	rangesrv_info *rangesrvs;

	//The rank's range server information, if it is a range server for this index
	rangesrv_info myinfo;

	UT_hash_handle hh;         /* makes this structure hashable */
};

/* Local Index info
 * Contains information about a local index
 *
 * A local index means that an index is not served by a range server
 * The index is accessed only by the client that created it.
 */

struct local_index {
	int id;
	//The abstracted data store layer that mdhim uses to store and retrieve records
	struct mdhim_store_t *mdhim_store;
	int key_type;             //The key type used in the db
	int db_type;              //The database type
	int type;                 /* The type of index 
				     (PRIMARY_INDEX, SECONDARY_INDEX, LOCAL_INDEX) */
};

typedef struct index_manifest_t {
	int key_type;   //The type of key 
	int index_type; /* The type of index 
			   (PRIMARY_INDEX, SECONDARY_INDEX) */
	int index_id; /* The id of the index in the hash table */
	int db_type;
	uint32_t num_rangesrvs;
	int rangesrv_factor;
	uint64_t slice_size; 
	int num_nodes;
} index_manifest_t;

#endif
