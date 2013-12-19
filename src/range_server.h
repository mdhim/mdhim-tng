#ifndef      __RANGESRV_H
#define      __RANGESRV_H

#include <pthread.h>
#include <mpi.h>
#include "data_store.h"
#include "messages.h"

struct mdhim_t;

typedef struct work_item work_item;
struct work_item {
	work_item *next;
	work_item *prev;
	void *message;
	int source;
};

typedef struct work_queue {
	work_item *head;
	work_item *tail;
} work_queue;

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

/* Outstanding requests (i.e., MPI_Req) that need to be freed later */
typedef struct out_req out_req;
struct out_req {
	out_req *next;
	out_req *prev;
	void *req;
	MPI_Request *message;
};

/* Range server specific data */
typedef struct mdhim_rs_t {
	//This communicator is for range servers only to talk to each other
	MPI_Comm rs_comm;   
	//The abstracted data store layer that mdhim uses to store and retrieve records
	struct mdhim_store_t *mdhim_store;
	work_queue *work_queue;
	pthread_mutex_t *work_queue_mutex;
	pthread_cond_t *work_ready_cv;
	pthread_t listener;
	pthread_t worker;
	rangesrv_info info;
	//Records seconds spent on putting records
	long double put_time; 
	//Records seconds spend on getting records
	long double get_time;
	long num_put;
	long num_get;
	out_req *out_req_list;
} mdhim_rs_t;


typedef struct mdhim_manifest_t {
	int key_type;
	int db_type;
	uint32_t num_rangesrvs;
	int rangesrv_factor;
	uint64_t slice_size; 
	int num_nodes;
} mdhim_manifest_t;



int range_server_add_work(struct mdhim_t *md, work_item *item);
int range_server_init(struct mdhim_t *md);
int range_server_init_comm(struct mdhim_t *md);
int range_server_stop(struct mdhim_t *md);
int im_range_server(struct mdhim_t *md);
int range_server_add_oreq(struct mdhim_t *md, MPI_Request *req, void *msg); //Add an outstanding request
int range_server_clean_oreqs(struct mdhim_t *md); //Clean outstanding reqs

#endif
