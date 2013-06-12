#include "mdhim.h"
#include "messages.h"

/*
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

/*
 * receive_message
 * Receives a message from the given source
 *
 * @param md      in   main MDHIM struct
 * @param src     in   source to receive from 
 * @message       out  pointer for message received
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int receive_message(struct mdhim_t *md, int src, void *message) {

	int max_size = 1048576; //At most receive 1MB
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
