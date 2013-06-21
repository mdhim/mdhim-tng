#include "mdhim.h"
#include "messages.h"

/**
 * send_message
 * Sends a message to the given destination
 *
 * @param md      main MDHIM struct
 * @param dest    destination to send to 
 * @param message pointer to message to send
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int send_message(struct mdhim_t *md, int dest, void *message) {

	MPI_Status status;
	int return_code;

	// Send a message to the dest (destination) server
	return_code = MPI_Send(message, 1, MPI_CHAR, dest, RANGESRV_WORK, md->mdhim_comm);

	// If the send did not succeed then return the error code back
	if ( return_code != MPI_SUCCESS )
		return MDHIM_ERROR;

	return MDHIM_SUCCESS;
}

/**
 * receive_message
 * Receives a message from the given source
 *
 * @param md      in   main MDHIM struct
 * @param src     in   source to receive from 
 * @message       out  pointer for message received
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int receive_message(struct mdhim_t *md, int src, void *message) {

	int max_size = 2147483647; //At most receive 2GB
	MPI_Status status;
	int return_code;

	// Receive a message from src (source) server
	return_code = MPI_Recv(message, max_size, MPI_CHAR, src, RANGESRV_WORK, md->mdhim_comm, 
		 &status);

	// If the receive did not succed then return the error code back
	if ( return_code != MPI_SUCCESS )
		return MDHIM_ERROR;

	return MDHIM_SUCCESS;
}

/**
 * get_rangesrvs
 * Receives range server info from every process and constructs a rangesrv_info list
 *
 * @param md      in   main MDHIM struct
 * @return a list of range servers
 */
struct rangesrv_info *get_rangesrvs(struct mdhim_t *md) {
	int rank;
	struct mdhim_rsi_t rsi;
	char *sendbuf;
	int sendsize;
	int sendidx = 0;
	int recvidx = 0;
	char *recvbuf;
	int recvsize;
	int ret = 0;
	int i = 0;
	rangesrv_info *rs_info, *rs_head, *rs_tail;

	//The range server info list head and tail that we are populating
	rs_head = rs_tail = NULL;
	//Allocate buffers
	sendsize = sizeof(struct mdhim_rsi_t);
	sendbuf = malloc(sendsize);
	recvsize = sendsize * md->mdhim_comm_size;
	recvbuf = malloc(recvsize);
	//Populate range server info to send
	if (md->mdhim_rs) {
		rsi.start_range = md->mdhim_rs->info.start_range;
		rsi.end_range = md->mdhim_rs->info.end_range;
	} else {
		//I'm not a range server
		rsi.start_range = -1;
		rsi.end_range = -1;
	}
	
	//Pack the range server info
	if ((ret = MPI_Pack(&rsi, sizeof(struct mdhim_rsi_t), MPI_CHAR, sendbuf, sendsize, &sendidx, 
			    md->mdhim_comm)) != MPI_SUCCESS) {
		mlog(MPI_CRIT, "Rank: %d - " 
		     "Error packing buffer when sending range server info", 
		     md->mdhim_rank);
		return NULL;
	}

	//Receive the range server info from each rank
	if ((ret = MPI_Allgather(sendbuf, sendsize, MPI_PACKED, recvbuf, recvsize,
				 MPI_PACKED, md->mdhim_comm)) != MPI_SUCCESS) {
		mlog(MPI_CRIT, "Rank: %d - " 
		     "Error while receiving range server info", 
		     md->mdhim_rank);
		return NULL;
	}

	//Unpack the receive buffer and construct a linked list of range servers
	for (i = 0; i < md->mdhim_comm_size; i++) {
		if ((ret = MPI_Unpack(recvbuf, recvsize, &recvidx, &rsi, sizeof(struct mdhim_rsi_t), 
				      MPI_CHAR, md->mdhim_comm)) != MPI_SUCCESS) {
			mlog(MPI_CRIT, "Rank: %d - " 
			     "Error while unpacking range server info", 
			     md->mdhim_rank);
			return NULL;
		}

		if (rsi.start_range == -1 || rsi.end_range == -1) {
			mlog(MDHIM_CLIENT_DBG, "Rank: %d - " 
			     "Rank: %d is not a range server", 
			     md->mdhim_rank, i);
			continue;
		}

		rs_info = malloc(sizeof(struct rangesrv_info));
		rs_info->rank = i;
		rs_info->start_range = rsi.start_range;
		rs_info->end_range = rsi.end_range;
		rs_info->next = NULL;
		if (!rs_head) {
			rs_head = rs_info;
			rs_tail = rs_info;
		} else {
			rs_tail->next = rs_info;
			rs_info->prev = rs_tail;
			rs_tail = rs_info;
		}
	}

	return rs_head;
}
