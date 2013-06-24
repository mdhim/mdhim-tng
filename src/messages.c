#include "mdhim.h"
#include "messages.h"

/**
 * send_rangesrv_work
 * Sends a message to the range server at the given destination
 *
 * @param md      main MDHIM struct
 * @param dest    destination to send to 
 * @param message pointer to message struct to send
 * @param size    size of the message
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int send_rangesrv_work(struct mdhim_t *md, int dest, void *message) {
	MPI_Status status;
	int return_code;
	char *sendbuf;
	int sendsize;

	//Pack the work message in into sendbuf and set sendsize

	//Send the size
	return_code = MPI_Send(&sendsize, 1, MPI_INT, dest, RANGESRV_WORK_SIZE, md->mdhim_comm);
	if (return_code != MPI_SUCCESS) {
		mlog(MPI_CRIT, "Rank: %d - " 
		     "Error sending work size in send_rangesrv_work", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}

	return_code = MPI_Send(sendbuf, sendsize, MPI_PACKED, dest, RANGESRV_WORK_MSG, 
			       md->mdhim_comm);
	if (return_code != MPI_SUCCESS) {
		mlog(MPI_CRIT, "Rank: %d - " 
		     "Error sending work message in send_rangesrv_work", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}

	return MDHIM_SUCCESS;
}

/**
 * receive_rangesrv_work message
 * Receives a message from the given source
 *
 * @param md      in   main MDHIM struct
 * @param message out  double pointer for message received
 * @param source  out  pointer to source of message received
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int receive_rangesrv_work(struct mdhim_t *md, void **message, int *source) {
	MPI_Status status;
	int return_code;
	int msg_size;
	int msg_source;
	// Receive the message size from any client
	return_code = MPI_Recv(&msg_size, 1, MPI_INT, MPI_ANY_SOURCE, RANGESRV_WORK_SIZE, 
			       md->mdhim_comm, &status);
	if (return_code != MPI_SUCCESS) {
		mlog(MPI_CRIT, "Rank: %d - " 
		     "Error receiving work size in receive_rangesrv_work", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}

	if (msg_size <= 0) {
		mlog(MPI_CRIT, "Rank: %d - " 
		     "Received work size of 0 or less in receive_rangesrv_work", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}

	// Receive a message from the client that sent the previous message
	msg_source = status.MPI_SOURCE;
	return_code = MPI_Recv(message, msg_size, MPI_PACKED, msg_source, RANGESRV_WORK_MSG, 
			       md->mdhim_comm, &status);

	// If the receive did not succed then return the error code back
	if ( return_code != MPI_SUCCESS ) {
		mlog(MPI_CRIT, "Rank: %d - " 
		     "Error receiving message in receive_rangesrv_work", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}

	//Unpack buffer to produce a message struct and place result in message pointer
	*source = msg_source;

	return MDHIM_SUCCESS;
}

/**
 * send_client_response
 * Sends a message to a client
 *
 * @param md      main MDHIM struct
 * @param dest    destination to send to 
 * @param message pointer to message to send
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int send_client_response(struct mdhim_t *md, int dest, void *message) {
	MPI_Status status;
	int return_code;
	char *sendbuf;
	int sendsize;

	//Pack the client response in the message pointer into sendbuf and set sendsize

	//Send the size
	return_code = MPI_Send(&sendsize, 1, MPI_INT, dest, CLIENT_RESPONSE_SIZE, md->mdhim_comm);
	if (return_code != MPI_SUCCESS) {
		mlog(MPI_CRIT, "Rank: %d - " 
		     "Error sending work size in send_client_response", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}

	return_code = MPI_Send(sendbuf, sendsize, MPI_PACKED, dest, CLIENT_RESPONSE_MSG, 
			       md->mdhim_comm);
	if (return_code != MPI_SUCCESS) {
		mlog(MPI_CRIT, "Rank: %d - " 
		     "Error sending work message in send_client_response", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}

	return MDHIM_SUCCESS;
}


/**
 * receive_client_response message
 * Receives a message from the given source
 *
 * @param md      in   main MDHIM struct
 * @param src     in   source to receive from 
 * @param message out  double pointer for message received
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int receive_client_response(struct mdhim_t *md, int src, void **message) {
	MPI_Status status;
	int return_code;
	int msg_size;
	int msg_source;

	// Receive the message size from src
	return_code = MPI_Recv(&msg_size, 1, MPI_INT, src, CLIENT_RESPONSE_SIZE, 
			       md->mdhim_comm, &status);
	if (return_code != MPI_SUCCESS) {
		mlog(MPI_CRIT, "Rank: %d - " 
		     "Error receiving work size in receive_client_response", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}
	if (msg_size <= 0) {
		mlog(MPI_CRIT, "Rank: %d - " 
		     "Received work size of 0 or less in receive_client_response", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}

	// Receive a message from the client that sent the previous message
	msg_source = status.MPI_SOURCE;
	return_code = MPI_Recv(message, msg_size, MPI_PACKED, msg_source, CLIENT_RESPONSE_MSG, 
			       md->mdhim_comm, &status);

	// If the receive did not succeed then return the error code back
	if ( return_code != MPI_SUCCESS ) {
		mlog(MPI_CRIT, "Rank: %d - " 
		     "Error receiving message in receive_client_response", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}

	//Unpack buffer to produce a message struct and place result in message pointer

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
