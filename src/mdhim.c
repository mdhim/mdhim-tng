/*
 * MDHIM TNG
 * 
 * API implementation that calls functions in client.c and local_client.c
 */

#include <stdlib.h>
#include "mdhim.h"
#include "range_server.h"
#include "client.h"
#include "partitioner.h"


/*
 * mdhimInit
 * Initializes MDHIM
 *
 * @param appComm  the communicator that was passed in from the application (e.g., MPI_COMM_WORLD)
 * @return mdhim_t* that contains info about this instance or NULL if there was an error
 */
struct mdhim_t *mdhimInit(MPI_Comm appComm) {
	int ret;
	struct mdhim_t *md;
	struct rangesrv_info *rangesrvs;

	//Allocate memory for the main MDHIM structure
	md = malloc(sizeof(struct mdhim_t));
	if (!md) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM - Error while allocating memory while initializing");
		return NULL;
	}

	//Dup the communicator passed in 
	if ((ret = MPI_Comm_dup(appComm, &md->mdhim_comm)) != MPI_SUCCESS) {
		mlog(MDHIM_CLIENT_CRIT, "Error while initializing the MDHIM communicator");
		return NULL;
	}
  
	//Get our rank in the main MDHIM communicator
	if ((ret = MPI_Comm_rank(md->mdhim_comm, &md->mdhim_rank)) != MPI_SUCCESS) {
		mlog(MDHIM_CLIENT_CRIT, "Error getting our rank while initializing MDHIM");
		return NULL;
	}
  
        //Get the size of the main MDHIM communicator
	if ((ret = MPI_Comm_size(md->mdhim_comm, &md->mdhim_comm_size)) != MPI_SUCCESS) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error getting the size of the " 
		     "comm while initializing", 
		     md->mdhim_rank);
		return NULL;
	}

	//Initialize receive msg mutex - used for receiving a message from myself
	md->receive_msg_mutex = malloc(sizeof(pthread_mutex_t));
	if (!md->receive_msg_mutex) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - " 
		     "Error while allocating memory for client", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}
	if ((ret = pthread_mutex_init(md->receive_msg_mutex, NULL)) != 0) {    
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - " 
		     "Error while initializing receive queue mutex", md->mdhim_rank);
		return MDHIM_ERROR;
	}
	//Initialize the receive condition variable - used for receiving a message from myself
	md->receive_msg_ready_cv = malloc(sizeof(pthread_cond_t));
	if (!md->receive_msg_ready_cv) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - " 
		     "Error while allocating memory for client", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}
	if ((ret = pthread_cond_init(md->receive_msg_ready_cv, NULL)) != 0) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - " 
		     "Error while initializing client receive condition variable", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}

	//Initialize the partitioner
	partitioner_init(md);

	//Start range server if I'm a range server
	if ((ret = range_server_init(md)) != MDHIM_SUCCESS) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - Error initializing MDHIM range server", 
		     md->mdhim_rank);
		return NULL;
	}

	//Get a list of all the range servers including myself if I am one
	if (!(rangesrvs = get_rangesrvs(md))) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - Did not receive any range servers" 
		     "while initializing", md->mdhim_rank);
		return NULL;
	}

	//Set the range server list in the md struct
	md->rangesrvs = rangesrvs;

	//Set the receive queue to NULL 
	md->receive_msg = NULL;

	return md;
}

/*
 * Quits the MDHIM instance
 * @param md main MDHIM struct
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int mdhimClose(struct mdhim_t *md) {
	int ret;
	struct rangesrv_info *rsrv, *trsrv;

	//Stop range server if I'm a range server	
	if ((ret = range_server_stop(md)) != MDHIM_SUCCESS) {
		return MDHIM_ERROR;
	}

	//Free up memory used by the partitioner
	partitioner_release();

	//Free up the range server list
	rsrv = md->rangesrvs;
	while (rsrv) {
		trsrv = rsrv->next;
		free(rsrv);
		rsrv = trsrv;
	}

	//Destroy the receive mutex
	if ((ret = pthread_mutex_destroy(md->receive_msg_mutex)) != 0) {
		return MDHIM_ERROR;
	}
	free(md->receive_msg_mutex);

	//Destroy the receive condition variable
	if ((ret = pthread_cond_destroy(md->receive_msg_ready_cv)) != 0) {
		return MDHIM_ERROR;
	}
	free(md->receive_msg_ready_cv);

	return MDHIM_SUCCESS;
}

/*
 * Put into MDHIM
 *
 * @param md main MDHIM struct
 * @param pm pointer to put message to be sent or inserted into the range server's work queue
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int mdhimPut(struct mdhim_t *md, struct mdhim_putm_t *pm) {
	//If I'm a range server this is to be sent to, create a message_item and call add_work to add this 
	//Otherwise, call client_put
}

/*
 * Bulk put into MDHIM
 * 
 * @param md main MDHIM struct
 * @param bpm pointer to bulk put message to be sent or inserted into the range server's work queue
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int mdhimBput(struct mdhim_t *md, struct mdhim_bputm_t *bpm) {
	//If I'm a range server this is to be sent to, create a message_item and call add_work to add this 
	//Otherwise, call client_bput
}

/*
 * Get a record from MDHIM
 *
 * @param md main MDHIM struct
 * @param gm pointer to get message to be sent or inserted into the range server's work queue
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int mdhimGet(struct mdhim_t *md, struct mdhim_getm_t *gm) {
	//If I'm a range server this is to be sent to, create a message_item and call add_work to add this 
	//Otherwise, call client_get
}

/*
 * Bulk Get from MDHIM
 *
 * @param md main MDHIM struct
 * @param bgm pointer to get message to be sent or inserted into the range server's work queue
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int mdhimBGet(struct mdhim_t *md, struct mdhim_bgetm_t *bgm) {
	//If I'm a range server this is to be sent to, create a message_item and call add_work to add this 
	//Otherwise, call client_bget
}

/*
 * Delete from MDHIM
 *
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int mdhimDelete(struct mdhim_t *md, struct mdhim_delm_t *dm) {
	//If I'm a range server this is to be sent to, create a message_item and call add_work to add this 
	//Otherwise, call client_delete
}

/*
 * Bulk delete from MDHIM
 *
 * @param md main MDHIM struct
 * @param bgm pointer to get message to be sent or inserted into the range server's work queue
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int mdhimBdelete(struct mdhim_t *md, struct mdhim_bdelm_t *dm) {
	//If I'm a range server this is to be sent to, create a message_item and call add_work to add this 
	//Otherwise, call client_bdelete
}


