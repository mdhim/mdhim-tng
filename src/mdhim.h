/*
 * MDHIM TNG
 * 
 * External API and data structures
 */

#ifndef      __MDHIM_H
#define      __MDHIM_H

#include <mpi.h>
#include <stdint.h>
#include <pthread.h>
#include "data_store.h"
#include "range_server.h"
#include "messages.h"
#include "partitioner.h"
#include "Mlog/mlog.h"
#include "Mlog/mlogfacs.h"
#include "mdhim_options.h"
#include "indexes.h"

#define MDHIM_SUCCESS 0
#define MDHIM_ERROR -1
#define MDHIM_DB_ERROR -2


/* 
 * mdhim data 
 * Contains client communicator
 * Contains a list of range servers
 * Contains a pointer to mdhim_rs_t if rank is a range server
 */
struct mdhim_t {
	//This communicator will include every process in the application, but is separate from main the app
        //It is used for sending and receiving to and from the range servers
	MPI_Comm mdhim_comm;   
	//This communicator will include every process in the application, but is separate from the app
        //It is used for barriers for clients
	MPI_Comm mdhim_client_comm;
	//The rank in the mdhim_comm
	int mdhim_rank;
	//The size of mdhim_comm
	int mdhim_comm_size;

	//A pointer to the primary index
	struct index_t *primary_index;
	//A linked list of range servers
	struct index_t *remote_indexes;
	struct index_t *local_indexes;

	//Lock to allow concurrent readers and a single writer to the remote_indexes hash table
	pthread_rwlock_t *remote_indexes_lock;
	pthread_rwlock_t *local_indexes_lock;

	//The range server structure which is used only if we are a range server
	mdhim_rs_t *mdhim_rs; 
	//The mutex used if receiving from ourselves
	pthread_mutex_t *receive_msg_mutex;
	//The condition variable used if receiving from ourselves
	pthread_cond_t *receive_msg_ready_cv;
	/* The receive msg, which is sent to the client by the 
	   range server running in the same process */
	void *receive_msg;
        //Options for DB creation
        mdhim_options_t *db_opts;
};

struct mdhim_t *mdhimInit(MPI_Comm appComm, struct mdhim_options_t *opts);
int mdhimClose(struct mdhim_t *md);
int mdhimCommit(struct mdhim_t *md, struct index_t *index);
int mdhimStatFlush(struct mdhim_t *md, struct index_t *index);
struct mdhim_rm_t *mdhimPut(struct mdhim_t *md, struct index_t *index,
			    void *key, int key_len,  
			    void *value, int value_len);
struct mdhim_brm_t *mdhimBPut(struct mdhim_t *md, struct index_t *index, 
			      void **keys, int *key_lens,
			      void **values, int *value_lens, int num_records);
struct mdhim_getrm_t *mdhimGet(struct mdhim_t *md, struct index_t *index,
			       void *key, int key_len, 
			       int op);
struct mdhim_bgetrm_t *mdhimBGet(struct mdhim_t *md, struct index_t *index,
				 void **keys, int *key_lens, 
				 int num_records);
struct mdhim_bgetrm_t *mdhimBGetOp(struct mdhim_t *md, struct index_t *index,
				   void *key, int key_len, 
				   int num_records, int op);
struct mdhim_rm_t *mdhimDelete(struct mdhim_t *md, struct index_t *index,
			       void *key, int key_len);
struct mdhim_brm_t *mdhimBDelete(struct mdhim_t *md, struct index_t *index,
				 void **keys, int *key_lens,
				 int num_keys);
void mdhim_release_recv_msg(void *msg);
#endif

