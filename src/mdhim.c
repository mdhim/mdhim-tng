/*
 * MDHIM TNG
 * 
 * API implementation that calls functions in client.c and range_server.c
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
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error while initializing the MDHIM communicator", 
		     md->mdhim_rank);
		return NULL;
	}
  
	//Get our rank in the main MDHIM communicator
	if ((ret = MPI_Comm_rank(md->mdhim_comm, &md->mdhim_rank)) != MPI_SUCCESS) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error getting our rank while initializing MDHIM", 
		     md->mdhim_rank);
		return NULL;
	}
  
	//Initialize the partitioner
	partitioner_init(md);

	//Start range server if I'm a range server
	if ((ret = range_server_init(md)) != MDHIM_SUCCESS) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - Error initializing MDHIM range server", 
		     md->mdhim_rank);
		return NULL;
	}

	if (!(rangesrvs = get_rangesrvs(md))) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - Did not receive any range servers", 
		     md->mdhim_rank);
		return NULL;
	}

	md->rangesrvs = rangesrvs;
	return md;
}

/*
 * Quits the MDHIM instance
 * @param md main MDHIM struct
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int mdhimClose(struct mdhim_t *md) {
	int ret;
	//Stop range server if I'm a range server
	
	if ((ret = range_server_stop(md)) != MDHIM_SUCCESS) {
		return MDHIM_ERROR;
	}

	partitioner_release();
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

