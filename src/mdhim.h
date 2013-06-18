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
#include "Mlog/mlog.h"
#include "Mlog/mlogfacs.h"

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
	//This communicator will include every process in the application, but is separate from the app
	MPI_Comm mdhim_comm;   
	//The rank in the mdhim_comm
	int mdhim_rank;
	//The number of range servers in the rangesrvs list
	uint32_t num_rangesrvs;
	//A linked list of range servers
	rangesrv_info *rangesrvs;
	//The range server structure which is used only if we are a range server
	mdhim_rs_t *mdhim_rs; 
	//The max key served
	int64_t max_key;
	//The min key served
	int64_t min_key;
	//The mutex used if receiving from ourselves
	pthread_mutex_t *receive_msg_mutex;
	//The condition variable used if receiving from ourselves
	pthread_cond_t *receive_msg_ready_cv;
};

struct mdhim_t *mdhimInit(MPI_Comm appComm);
int mdimClose(struct mdhim_t *md);
int mdhimPut(struct mdhim_t *md, struct mdhim_putm_t *pm);
int mdhimBput(struct mdhim_t *md, struct mdhim_bputm_t *bpm);
int mdhimGet(struct mdhim_t *md, struct mdhim_getm_t *gm);
int mdhimBGet(struct mdhim_t *md, struct mdhim_bgetm_t *bgm);
int mdhimDelete(struct mdhim_t *md, struct mdhim_delm_t *dm);
int mdhimBdelete(struct mdhim_t *md, struct mdhim_bdelm_t *dm);

#endif

