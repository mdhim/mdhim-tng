#ifndef      __RANGESRV_H
#define      __RANGESRV_H

#include <pthread.h>
#include "data_store.h"
#include "messages.h"
#include "uthash.h"

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
	//The start range this server is serving (inclusive)
	uint64_t start_range;
	//The end range this server is serving (inclusive)
	uint64_t end_range;
	//The next range server in the list
	rangesrv_info *next;
	//The previous range server in the list
	rangesrv_info *prev;
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
} mdhim_rs_t;

//Used for storing cursors for each rank
struct mdhim_cursor {
	int id;            /* key */
	void *cursor;
	UT_hash_handle hh; /* makes this structure hashable */
};

int range_server_add_work(struct mdhim_t *md, work_item *item);
int range_server_init(struct mdhim_t *md);
int range_server_stop(struct mdhim_t *md);
int im_range_server(struct mdhim_t *md);

#endif
