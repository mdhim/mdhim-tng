#include <unistd.h>
#include <math.h>
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
	int return_code = MDHIM_ERROR;
	void *sendbuf = NULL;
	int sendsize = 0;
	int mtype;

	//Pack the work message in into sendbuf and set sendsize
	mtype = ((struct mdhim_basem_t *) message)->mtype;
	switch(mtype) {
	case MDHIM_PUT:
		return_code = pack_put_message(md, (struct mdhim_putm_t *)message, &sendbuf, 
					       &sendsize);
		break;
	case MDHIM_BULK_PUT:
		return_code = pack_bput_message(md, (struct mdhim_bputm_t *)message, &sendbuf, 
						&sendsize);
		break;
	case MDHIM_GET:
		return_code = pack_get_message(md, (struct mdhim_getm_t *)message, &sendbuf, 
					       &sendsize);
		break;
	case MDHIM_BULK_GET:
		return_code = pack_bget_message(md, (struct mdhim_bgetm_t *)message, &sendbuf, 
						&sendsize);
		break;
	case MDHIM_DEL:
		return_code = pack_del_message(md, (struct mdhim_delm_t *)message, &sendbuf, 
					       &sendsize);
		break;
	case MDHIM_BULK_DEL:
		return_code = pack_bdel_message(md, (struct mdhim_bdelm_t *)message, &sendbuf, 
						&sendsize);
		break;
	case MDHIM_COMMIT:
		return_code = pack_base_message(md, (struct mdhim_basem_t *)message, &sendbuf, 
						&sendsize);
		break;
	case MDHIM_CLOSE:
		return_code = pack_base_message(md, (struct mdhim_basem_t *)message, &sendbuf, 
						&sendsize);
		break;
	default:
		break;
	}

	if (return_code != MDHIM_SUCCESS) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: Packing message "
                     "failed before sending.", md->mdhim_rank);
		return MDHIM_ERROR;
	}

	//Send the message
	return_code = MPI_Send(sendbuf, sendsize, MPI_PACKED, dest, RANGESRV_WORK_MSG, 
			       md->mdhim_comm);
	if (return_code != MPI_SUCCESS) {
		mlog(MPI_CRIT, "Rank: %d - " 
		     "Error sending work message in send_rangesrv_work", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}

	free(sendbuf);
	return MDHIM_SUCCESS;
}

/**
 * send_all_rangesrv_work
 * Sends multiple messages simultaneously and waits for them to all complete
 *
 * @param md      main MDHIM struct
 * @param dest    destination to send to 
 * @param message pointer to message struct to send
 * @param size    size of the message
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int send_all_rangesrv_work(struct mdhim_t *md, void **messages) {
	int return_code = MDHIM_ERROR;
	void *sendbuf = NULL;
	void **sendbufs;
	int sendsize = 0;
	int mtype;
	MPI_Request **reqs;
	MPI_Request *req;
	int num_msgs;
	int i, ret, flag, done, num_sent;
	void *mesg;
	MPI_Status status;
	int dest;

	ret = MDHIM_SUCCESS;
	num_msgs = md->num_rangesrvs;
	reqs = malloc(sizeof(MPI_Request *) * num_msgs);
	memset(reqs, 0, sizeof(MPI_Request *) * num_msgs);
	sendbufs = malloc(sizeof(void *) * num_msgs);
	memset(sendbufs, 0, sizeof(void *) * num_msgs);
	num_sent = 0;
	done = 0;

	//Send all messages at once
	for (i = 0; i < num_msgs; i++) {
		mesg = *(messages + i);
		if (!mesg) {
			continue;
		}
		mtype = ((struct mdhim_basem_t *) mesg)->mtype;
		dest = ((struct mdhim_basem_t *) mesg)->server_rank;
		//Pack the work message in into sendbuf and set sendsize
		switch(mtype) {
		case MDHIM_BULK_PUT:
			return_code = pack_bput_message(md, (struct mdhim_bputm_t *)mesg, &sendbuf, 
							&sendsize);
			break;
		case MDHIM_BULK_GET:
			return_code = pack_bget_message(md, (struct mdhim_bgetm_t *)mesg, &sendbuf, 
							&sendsize);
			break;
		case MDHIM_BULK_DEL:
			return_code = pack_bdel_message(md, (struct mdhim_bdelm_t *)mesg, &sendbuf, 
							&sendsize);
			break;
		default:
			break;
		}
		
		if (return_code != MDHIM_SUCCESS) {
			mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: Packing message "
			     "failed before sending.", md->mdhim_rank);
			ret = MDHIM_ERROR;
		}
				
		sendbufs[i] = sendbuf;
		req = malloc(sizeof(MPI_Request));
		return_code = MPI_Isend(sendbuf, sendsize, MPI_PACKED, dest, RANGESRV_WORK_MSG, 
					md->mdhim_comm, req);
		if (return_code != MPI_SUCCESS) {
			free(req);
			reqs[i] = NULL;
			mlog(MPI_CRIT, "Rank: %d - " 
			     "Error sending work message in send_rangesrv_work", 
			     md->mdhim_rank);
			ret = MDHIM_ERROR;
		} else {
			reqs[i] = req;
			num_sent++;
		}
	}
	
	//Wait for messages to complete
	while (done != num_sent) {
		for (i = 0; i < num_msgs; i++) {
			req = reqs[i];
			if (!req) {
				continue;
			}
			
			MPI_Test(req, &flag, &status);
			if (flag) {
				free(req);
				reqs[i] = NULL;
				done++;
			}		
		}
		
		usleep(200);
	}

	for (i = 0; i < num_msgs; i++) {
		if (!sendbufs[i]) {
			continue;
		}
		free(sendbufs[i]);
	}

	free(sendbufs);
	free(reqs);

	return ret;
}

/**
 * receive_rangesrv_work message
 * Receives a message from the given source
 *
 * @param md      in   main MDHIM struct
 * @param message out  double pointer for message received
 * @param src     out  pointer to source of message received
 * @return MDHIM_SUCCESS, MDHIM_CLOSE, MDHIM_COMMIT, or MDHIM_ERROR on error
 */
int receive_rangesrv_work(struct mdhim_t *md, int *src, void **message) {
	MPI_Status status;
	int return_code;
	int msg_size;
	int msg_source;
	void *recvbuf;
	int mtype;
	struct mdhim_basem_t *bm;
	int mesg_idx = 0;
	MPI_Request req;
	int flag = 0;
	int ret = MDHIM_SUCCESS;

	// Receive a message from any client
	recvbuf = (void *) malloc(MDHIM_MAX_MSG_SIZE);
	flag = 0;
	return_code = MPI_Irecv(recvbuf, MDHIM_MAX_MSG_SIZE, MPI_PACKED, MPI_ANY_SOURCE, RANGESRV_WORK_MSG, 
			       md->mdhim_comm, &req);
	// If the receive did not succeed then return the error code back
	if ( return_code != MPI_SUCCESS ) {
             	mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - Error: %d "
                     "receive message failed.", md->mdhim_rank, return_code);
		free(recvbuf);
		return MDHIM_ERROR;
	}
	while (!flag) {
		MPI_Test(&req, &flag, &status);	
		usleep(100);
	}

	msg_source = status.MPI_SOURCE;
	*src = msg_source;
	*message = NULL;
	//Unpack buffer to get the message type
	bm = malloc(sizeof(struct mdhim_basem_t));
	return_code = MPI_Unpack(recvbuf, MDHIM_MAX_MSG_SIZE, &mesg_idx, bm, 
				 sizeof(struct mdhim_basem_t), MPI_CHAR, 
				 md->mdhim_comm);
	mtype = bm->mtype;
	msg_size = bm->size;
	free(bm);
	mlog(MDHIM_SERVER_DBG, "MDHIM Rank: %d - "
	     "Received message with size: %d and type: %d from rank: %d.", md->mdhim_rank, msg_size, 
	     mtype, msg_source);

	switch(mtype) {
	case MDHIM_PUT:
		return_code = unpack_put_message(md, recvbuf, msg_size, message);
		break;
	case MDHIM_BULK_PUT:
		return_code = unpack_bput_message(md, recvbuf, msg_size, message);
		break;
	case MDHIM_GET:
		return_code = unpack_get_message(md, recvbuf, msg_size, message);
		break;
	case MDHIM_BULK_GET:
		return_code = unpack_bget_message(md, recvbuf, msg_size, message);
		break;
	case MDHIM_DEL:
		return_code = unpack_del_message(md, recvbuf, msg_size, message);
		break;
	case MDHIM_BULK_DEL:
		return_code = unpack_bdel_message(md, recvbuf, msg_size, message);			
		break;
	case MDHIM_COMMIT:
		ret = MDHIM_COMMIT;
		break;
	case MDHIM_CLOSE:
		ret = MDHIM_CLOSE;
		break;
	default:
		break;
	}

	if (return_code != MPI_SUCCESS) {
		mlog(MPI_CRIT, "Rank: %d - " 
		     "Error unpacking message in receive_rangesrv_work", 
		     md->mdhim_rank);
		ret = MDHIM_ERROR;
	}

	free(recvbuf);

	return ret;
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
	int return_code;
	void *sendbuf = NULL;
	int sendsize = 0;
	int mtype;
	int ret = MDHIM_SUCCESS;

	//Pack the client response in the message pointer into sendbuf and set sendsize
	mtype = ((struct mdhim_basem_t *) message)->mtype;
	switch(mtype) {
	case MDHIM_RECV:
		return_code = pack_return_message(md, (struct mdhim_rm_t *)message, &sendbuf, 
						  &sendsize);
		break;
	case MDHIM_RECV_GET:
		return_code = pack_getrm_message(md, (struct mdhim_getrm_t *)message, &sendbuf, 
						 &sendsize);
		break;
	case MDHIM_RECV_BULK_GET:
		return_code = pack_bgetrm_message(md, (struct mdhim_bgetrm_t *)message, &sendbuf, 
						  &sendsize);
		break;
	default:
		break;
	}

	if (return_code != MDHIM_SUCCESS) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to pack "
                     "the message while sending.", md->mdhim_rank);
		ret = MDHIM_ERROR;
	}

	mlog(MPI_DBG, "Rank: %d - " 
	     "Sending response message to: %d", 
	     md->mdhim_rank, dest);
	//Send the actual message
	return_code = MPI_Send(sendbuf, sendsize, MPI_PACKED, dest, CLIENT_RESPONSE_MSG, 
				md->mdhim_comm);
	if (return_code != MPI_SUCCESS) {
		mlog(MPI_CRIT, "Rank: %d - " 
		     "Error sending work message in send_client_response", 
		     md->mdhim_rank);
		ret = MDHIM_ERROR;
	}
	free(sendbuf);

	return ret;
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
	int mtype;
	int mesg_idx = 0;
	void *recvbuf;
	struct mdhim_basem_t *bm;

	recvbuf = malloc(MDHIM_MAX_MSG_SIZE);
	return_code = MPI_Recv(recvbuf, MDHIM_MAX_MSG_SIZE, MPI_PACKED, src, CLIENT_RESPONSE_MSG, 
			       md->mdhim_comm, &status);

	// If the receive did not succeed then return the error code back
	if ( return_code != MPI_SUCCESS ) {
		mlog(MPI_CRIT, "Rank: %d - " 
		     "Error receiving message in receive_client_response", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}

	//Received the message
	mlog(MPI_DBG, "Rank: %d - " 
	     "Received the message from server: %d", 
	     md->mdhim_rank, src);

	*message = NULL;
	bm = malloc(sizeof(struct mdhim_basem_t));
	//Unpack buffer to get the message type
	return_code = MPI_Unpack(recvbuf, MDHIM_MAX_MSG_SIZE, &mesg_idx, bm, 
				 sizeof(struct mdhim_basem_t), MPI_CHAR, 
				 md->mdhim_comm);
	mtype = bm->mtype;
	msg_size = bm->size;
	free(bm);
	switch(mtype) {
	case MDHIM_RECV:
		return_code = unpack_return_message(md, recvbuf, message);
		break;
	case MDHIM_RECV_GET:
		return_code = unpack_getrm_message(md, recvbuf, msg_size, message);
		break;
	case MDHIM_RECV_BULK_GET:
		return_code = unpack_bgetrm_message(md, recvbuf, msg_size, message);
		break;
	default:
		break;
	}

	if (return_code != MDHIM_SUCCESS) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to unpack "
                     "the message while receiving from client.", md->mdhim_rank);
		return MDHIM_ERROR;
	}

	free(recvbuf);

	return MDHIM_SUCCESS;
}

/**
 * receive_all_client_responses
 * Receives messages from multiple sources sources
 *
 * @param md       in  main MDHIM struct
 * @param srcs     in  sources to receive from 
 * @param srcs     in  number of sources to receive from 
 * @param message out  list of messages to receive
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int receive_all_client_responses(struct mdhim_t *md, int *srcs, int nsrcs, 
				 void ***messages) {
	MPI_Status status;
	int return_code;
	int mtype;
	int mesg_idx = 0;
	void *recvbuf, **recvbufs;
	struct mdhim_basem_t *bm;
	int i;
	int ret = MDHIM_SUCCESS;
	MPI_Request **reqs, *req;
	int num_sent = 0;
	int done = 0;
	int flag = 0;
	int msg_size;

	reqs = malloc(nsrcs * sizeof(MPI_Request *));
	memset(reqs, 0, nsrcs * sizeof(MPI_Request *));
	recvbufs = malloc(nsrcs * sizeof(void *));
	memset(recvbufs, 0, nsrcs * sizeof(void *));
	done = num_sent = 0;
	for (i = 0; i < nsrcs; i++) {
	// Receive a message from the servers in the list	
		recvbuf = malloc(MDHIM_MAX_MSG_SIZE);
		recvbufs[i] = recvbuf;
		req = malloc(sizeof(MPI_Request));
		reqs[i] = req;
		return_code = MPI_Irecv(recvbuf, MDHIM_MAX_MSG_SIZE, MPI_PACKED, 
					srcs[i], CLIENT_RESPONSE_MSG, 
				       md->mdhim_comm, req);
		
		// If the receive did not succeed then return the error code back
		if ( return_code != MPI_SUCCESS ) {
			mlog(MPI_CRIT, "Rank: %d - " 
			     "Error receiving message in receive_client_response", 
			     md->mdhim_rank);
			ret = MDHIM_ERROR;
		}

		num_sent++;				
	}

	//Wait for messages to complete
	while (done != num_sent) {
		for (i = 0; i < nsrcs; i++) {
			req = reqs[i];
			if (!req) {
				continue;
			}
			
			MPI_Test(req, &flag, &status);				
			if (!flag) {
			  continue;
			}
			free(req);
			reqs[i] = NULL;
			done++;		
		}

		if (done != num_sent) {
			usleep(200);
		}
	}

	free(reqs);
	for (i = 0; i < nsrcs; i++) {			
		recvbuf = recvbufs[i];
		//Received the message
		*(*messages + i) = NULL;
		bm = malloc(sizeof(struct mdhim_basem_t));
		//Unpack buffer to get the message type
		mesg_idx = 0;
		return_code = MPI_Unpack(recvbuf, MDHIM_MAX_MSG_SIZE, &mesg_idx, bm, 
					 sizeof(struct mdhim_basem_t), MPI_CHAR, 
					 md->mdhim_comm);
		mtype = bm->mtype;
		msg_size = bm->size;
		free(bm);
		switch(mtype) {
		case MDHIM_RECV:
			return_code = unpack_return_message(md, recvbuf, 
							    (*messages + i));
			break;
		case MDHIM_RECV_GET:
			return_code = unpack_getrm_message(md, recvbuf, msg_size, 
							   (*messages + i));
			break;
		case MDHIM_RECV_BULK_GET:
			return_code = unpack_bgetrm_message(md, recvbuf, msg_size, 
							    (*messages + i));
			break;
		default:
			break;
		}
		
		if (return_code != MDHIM_SUCCESS) {
			mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to unpack "
			     "the message while receiving from client.", md->mdhim_rank);
			ret = MDHIM_ERROR;
		}

		free(recvbuf);
	}

	free(recvbufs);

	return ret;
}

///------------------------

/**
 * pack_put_message
 * Packs a put message structure into contiguous memory for message passing
 *
 * @param md       in   main MDHIM struct
 * @param pm       in   structure put_message which will be packed into the sendbuf 
 * @param sendbuf  out  double pointer for packed message to send
 * @param sendsize out  pointer to size of sendbuf
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 * 
 * struct mdhim_putm_t {
 int mtype;
 void *key;
 int key_len;
 void *data;
 int data_len;
 int server_rank;
 };
*/
int pack_put_message(struct mdhim_t *md, struct mdhim_putm_t *pm, void **sendbuf, int *sendsize) {
	int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
        int64_t m_size = sizeof(struct mdhim_putm_t); // Generous variable for size calculation
        int mesg_size;  // Variable to be used as parameter for MPI_pack of safe size
    	int mesg_idx = 0;  // Variable for incremental pack
        void *outbuf;

        // Add to size the length of the key and data fields
        m_size += pm->key_len + pm->value_len;	
	mlog(MDHIM_CLIENT_DBG, "MDHIM Rank: %d - Packing put message with key len: %d," 
	     " with value len: %d.", md->mdhim_rank, pm->key_len, pm->value_len);

        if (m_size > MDHIM_MAX_MSG_SIZE) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: put message too large."
                     " Put is over Maximum size allowed of %d.", md->mdhim_rank, MDHIM_MAX_MSG_SIZE);
		return MDHIM_ERROR; 
        }

	//Set output variable for the size to send
	mesg_size = (int) m_size;
        *sendsize = mesg_size;	
	pm->size = mesg_size;

	mlog(MDHIM_CLIENT_DBG, "MDHIM Rank: %d - Packing put message has size: %d.", md->mdhim_rank,
	     mesg_size);
        // Is the computed message size of a safe value? (less than a max message size?)
        if ((*sendbuf = malloc(mesg_size * sizeof(char))) == NULL) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to pack put message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }
        
	outbuf = *sendbuf;
        // pack the message first with the structure and then followed by key and data values.
	return_code = MPI_Pack(pm, sizeof(struct mdhim_putm_t), MPI_CHAR, outbuf, mesg_size, 
			       &mesg_idx, md->mdhim_comm);
        return_code += MPI_Pack(pm->key, pm->key_len, MPI_CHAR, outbuf, mesg_size, &mesg_idx,
				md->mdhim_comm);
        return_code += MPI_Pack(pm->value, pm->value_len, MPI_CHAR, outbuf, mesg_size, &mesg_idx, 
				md->mdhim_comm);

	// If the pack did not succeed then log the error and return the error code
	if ( return_code != MPI_SUCCESS ) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to pack "
                     "the put message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }

	return MDHIM_SUCCESS;
}

/**
 * pack_bput_message
 * Packs a bulk put message structure into contiguous memory for message passing
 *
 * @param md        in   main MDHIM struct
 * @param bpm       in   structure bput_message which will be packed into the sendbuf 
 * @param sendsize  out  double pointer for packed message to send
 * @param sendsize  out  pointer for packed message size
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 * 
 * struct mdhim_bputm_t {
 int mtype;
 void **keys;
 int *key_lens;
 void **data;
 int *data_lens;
 int num_records;
 int server_rank;
 };
*/
int pack_bput_message(struct mdhim_t *md, struct mdhim_bputm_t *bpm, void **sendbuf, int *sendsize) {
	int return_code = MPI_SUCCESS; // MPI_SUCCESS = 0
        int64_t m_size = sizeof(struct mdhim_bputm_t);  // Generous variable for size calc
        int mesg_size;   // Variable to be used as parameter for MPI_pack of safe size
    	int mesg_idx = 0;
        int i;

        // Add the sizes of the length arrays (key_lens and data_lens)
        m_size += 2 * bpm->num_records * sizeof(int);
        
        // For the each of the keys and data add enough chars.
        for (i=0; i < bpm->num_records; i++)
                m_size += bpm->key_lens[i] + bpm->value_lens[i];
        
        // Is the computed message size of a safe value? (less than a max message size?)
        if (m_size > MDHIM_MAX_MSG_SIZE) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: bulk put message too large."
                     " Bput is over Maximum size allowed of %d.", md->mdhim_rank, MDHIM_MAX_MSG_SIZE);
		return MDHIM_ERROR; 
        }
        mesg_size = m_size;  // Safe size to use in MPI_pack     
	*sendsize = mesg_size;
	bpm->size = mesg_size;

        if ((*sendbuf = malloc(mesg_size * sizeof(char))) == NULL) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to pack bulk put message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }

        // pack the message first with the structure and then followed by key and data values (plus lengths).
	return_code = MPI_Pack(bpm, sizeof(struct mdhim_bputm_t), MPI_CHAR, *sendbuf, 
			       mesg_size, &mesg_idx, md->mdhim_comm);
         
        // For the each of the keys and data pack the chars plus two ints for key_len and data_len.
        for (i=0; i < bpm->num_records; i++) {
                return_code += MPI_Pack(&bpm->key_lens[i], 1, MPI_INT, 
					*sendbuf, mesg_size, &mesg_idx, md->mdhim_comm);
                return_code += MPI_Pack(bpm->keys[i], bpm->key_lens[i], MPI_CHAR, 
					*sendbuf, mesg_size, &mesg_idx, md->mdhim_comm);
                return_code += MPI_Pack(&bpm->value_lens[i], 1, MPI_INT, 
					*sendbuf, mesg_size, &mesg_idx, md->mdhim_comm);
                return_code += MPI_Pack(bpm->values[i], bpm->value_lens[i], MPI_CHAR, 
					*sendbuf, mesg_size, &mesg_idx, md->mdhim_comm);
        }

	// If the pack did not succeed then log the error and return the error code
	if ( return_code != MPI_SUCCESS ) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to pack "
                     "the bulk put message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }

	return MDHIM_SUCCESS;
}

/**
 * unpack_put_message
 * Unpacks a put message structure into contiguous memory for message passing
 *
 * @param md         in   main MDHIM struct
 * @param message    in   pointer for packed message we received
 * @param mesg_size  in   size of the incoming message
 * @param pm         out  structure put_message which will be unpacked from the message 
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 * 
 * struct mdhim_putm_t {
 int mtype;
 void *key;
 int key_len;
 void *data;
 int data_len;
 int server_rank;
 };
*/
int unpack_put_message(struct mdhim_t *md, void *message, int mesg_size,  void **putm) {

	int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
    	int mesg_idx = 0;  // Variable for incremental unpack
        struct mdhim_putm_t *pm;

	mlog(MDHIM_SERVER_DBG, "MDHIM Rank: %d - Unpacking put message with size: %d",
	     md->mdhim_rank, mesg_size);
        if ((*((struct mdhim_putm_t **) putm) = malloc(sizeof(struct mdhim_putm_t))) == NULL) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack put message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }
        
	pm = *((struct mdhim_putm_t **) putm);
        // Unpack the message first with the structure and then followed by key and data values.
        return_code = MPI_Unpack(message, mesg_size, &mesg_idx, pm, 
				 sizeof(struct mdhim_putm_t), MPI_CHAR, 
				 md->mdhim_comm);
        
        // Unpack key by first allocating memory and then extracting the values from message
        if ((pm->key = malloc(pm->key_len * sizeof(char))) == NULL) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack put message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }
        return_code += MPI_Unpack(message, mesg_size, &mesg_idx, pm->key, pm->key_len, 
				  MPI_CHAR, md->mdhim_comm);
        
        // Unpack data by first allocating memory and then extracting the values from message
        if ((pm->value = malloc(pm->value_len * sizeof(char))) == NULL) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack put message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }
        return_code += MPI_Unpack(message, mesg_size, &mesg_idx, pm->value, pm->value_len, 
				  MPI_CHAR, md->mdhim_comm);

	// If the unpack did not succeed then log the error and return the error code
	if ( return_code != MPI_SUCCESS ) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to unpack "
                     "the put message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }

	return MDHIM_SUCCESS;
}

/**
 * unpack_bput_message
 * Unpacks a bulk put message structure into contiguous memory for message passing
 *
 * @param md         in   main MDHIM struct
 * @param message    in   pointer for packed message we received
 * @param mesg_size  in   size of the incoming message
 * @param bpm        out  structure bulk_put_message which will be unpacked from the message 
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 * 
 * struct mdhim_bputm_t {
 int mtype;
 void **keys;
 int *key_lens;
 void **data;
 int *data_lens;
 int num_records;
 int server_rank;
 };
*/
int unpack_bput_message(struct mdhim_t *md, void *message, int mesg_size, void **bput) {

	int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
    	int mesg_idx = 0;  // Variable for incremental unpack
        int i;
	int num_records;

        if ((*((struct mdhim_bputm_t **) bput) = malloc(sizeof(struct mdhim_bputm_t))) == NULL) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack bput message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }
        
        // Unpack the message first with the structure and then followed by key and data values.
        return_code = MPI_Unpack(message, mesg_size, &mesg_idx, *((struct mdhim_bputm_t **) bput), 
				 sizeof(struct mdhim_bputm_t), 
				 MPI_CHAR, md->mdhim_comm);
        
	num_records = (*((struct mdhim_bputm_t **) bput))->num_records;
	// Allocate memory for key pointers, to be populated later.
        if (((*((struct mdhim_bputm_t **) bput))->keys = 
	     malloc(num_records * sizeof(void *))) == NULL) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
		     "memory to unpack bput message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }

	// Allocate memory for value pointers, to be populated later.
        if (((*((struct mdhim_bputm_t **) bput))->values = 
	     malloc(num_records * sizeof(void *))) == NULL) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
		     "memory to unpack bput message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }

        // Allocate memory for key_lens, to be populated later.
        if (((*((struct mdhim_bputm_t **) bput))->key_lens = 
	     (int *)malloc(num_records * sizeof(int))) == NULL) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack bput message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }
        
        // Allocate memory for value_lens, to be populated later.
        if (((*((struct mdhim_bputm_t **) bput))->value_lens = 
	     (int *)malloc(num_records * sizeof(int))) == NULL) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack bput message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }
        
        // For the each of the keys and data unpack the chars plus two ints for key_lens[i] and data_lens[i].
        for (i=0; i < num_records; i++) {
		// Unpack the key_lens[i]
		return_code += MPI_Unpack(message, mesg_size, &mesg_idx, 
					  &(*((struct mdhim_bputm_t **) bput))->key_lens[i], 1, MPI_INT, 
					  md->mdhim_comm);
            
		// Unpack key by first allocating memory and then extracting the values from message
		if (((*((struct mdhim_bputm_t **) bput))->keys[i] = 
		     malloc((*((struct mdhim_bputm_t **) bput))->key_lens[i] * 
			    sizeof(char))) == NULL) {
			mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
			     "memory to unpack bput message.", md->mdhim_rank);
			return MDHIM_ERROR; 
		}
		return_code += MPI_Unpack(message, mesg_size, &mesg_idx, 
					  (*((struct mdhim_bputm_t **) bput))->keys[i], 
					  (*((struct mdhim_bputm_t **) bput))->key_lens[i], 
					  MPI_CHAR, md->mdhim_comm);
            
		// Unpack the data_lens[i]
		return_code += MPI_Unpack(message, mesg_size, &mesg_idx, 
					  &(*((struct mdhim_bputm_t **) bput))->value_lens[i], 1, 
					  MPI_INT, 
					  md->mdhim_comm);
	    
		// Unpack data by first allocating memory and then extracting the values from message
		if (((*((struct mdhim_bputm_t **) bput))->values[i] = 
		     malloc((*((struct mdhim_bputm_t **) bput))->value_lens[i] * 
			    sizeof(char))) == NULL) {
			mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
			     "memory to unpack bput message.", md->mdhim_rank);
			return MDHIM_ERROR; 
		}

		return_code += MPI_Unpack(message, mesg_size, &mesg_idx, 
					  (*((struct mdhim_bputm_t **) bput))->values[i], 
					  (*((struct mdhim_bputm_t **) bput))->value_lens[i], 
					  MPI_CHAR, md->mdhim_comm);
        }

	// If the unpack did not succeed then log the error and return the error code
	if ( return_code != MPI_SUCCESS ) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to unpack "
                     "the bput message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }

	return MDHIM_SUCCESS;
}

///------------------------

/**
 * pack_get_message
 * Packs a get message structure into contiguous memory for message passing
 *
 * @param md        in   main MDHIM struct
 * @param gm        in   structure get_message which will be packed into the sendbuf 
 * @param sendbuf   out  double pointer for packed message to send
 * @param sendsize  out  pointer to sendbuf's size
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 * 
 * struct mdhim_getm_t {
 int mtype;  
 int op;  
 void *key;
 int key_len;
 int server_rank;
 };
*/
int pack_get_message(struct mdhim_t *md, struct mdhim_getm_t *gm, void **sendbuf, int *sendsize) {

	int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
        int64_t m_size = sizeof(struct mdhim_getm_t); // Generous variable for size calculation
        int mesg_size;  // Variable to be used as parameter for MPI_pack of safe size
    	int mesg_idx = 0;  // Variable for incremental pack
        void *outbuf;
        
        // Add to size the length of the key and data fields
        m_size += gm->key_len;
        
        if (m_size > MDHIM_MAX_MSG_SIZE) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: get message too large."
                     " Get is over Maximum size allowed of %d.", md->mdhim_rank, MDHIM_MAX_MSG_SIZE);
		return MDHIM_ERROR; 
        }
        mesg_size = m_size;
        *sendsize = mesg_size;
	gm->size = mesg_size;

        // Is the computed message size of a safe value? (less than a max message size?)
        if ((*sendbuf = malloc(mesg_size * sizeof(char))) == NULL) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to pack get message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }
        
        outbuf = *sendbuf;
        // pack the message first with the structure and then followed by key and data values.
	return_code = MPI_Pack(gm, sizeof(struct mdhim_getm_t), MPI_CHAR, outbuf, mesg_size, 
                               &mesg_idx, md->mdhim_comm);
        return_code += MPI_Pack(gm->key, gm->key_len, MPI_CHAR, outbuf, mesg_size, 
                                &mesg_idx, md->mdhim_comm);

	// If the pack did not succeed then log the error and return the error code
	if ( return_code != MPI_SUCCESS ) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to pack "
                     "the get message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }

	return MDHIM_SUCCESS;
}

/**
 * pack_bget_message
 * Packs a bget message structure into contiguous memory for message passing
 *
 * @param md       in   main MDHIM struct
 * @param bgm      in   structure bget_message which will be packed into the sendbuf
 * @param sendbuf  out  double pointer for packed message to send
 * @param sendsize out  pointer for sendbuf size
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 * 
 * struct mdhim_bgetm_t {
 int mtype;
 int op;
 void **keys;
 int *key_lens;
 int num_records;
 int server_rank;
 };
*/
int pack_bget_message(struct mdhim_t *md, struct mdhim_bgetm_t *bgm, void **sendbuf, int *sendsize) {
	int return_code = MPI_SUCCESS; // MPI_SUCCESS = 0
        int64_t m_size = sizeof(struct mdhim_bgetm_t);  // Generous variable for size calc
        int mesg_size;   // Variable to be used as parameter for MPI_pack of safe size
    	int mesg_idx = 0;
        int i;
        
        // Calculate the size of the message to send
        m_size += bgm->num_records * sizeof(int) * 2;
        
        // For the each of the keys add the size to the length
        for (i=0; i < bgm->num_records; i++)
                m_size += bgm->key_lens[i];
        
        // Is the computed message size of a safe value? (less than a max message size?)
        if (m_size > MDHIM_MAX_MSG_SIZE) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: bulk get message too large."
                     " Bget is over Maximum size allowed of %d.", md->mdhim_rank, MDHIM_MAX_MSG_SIZE);
		return MDHIM_ERROR; 
        }
        mesg_size = m_size;  // Safe size to use in MPI_pack     
	*sendsize = mesg_size;
	bgm->size = mesg_size;

        if ((*sendbuf = malloc(mesg_size * sizeof(char))) == NULL) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to pack bulk get message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }
        
        // pack the message first with the structure and then followed by key and 
	// data values (plus lengths).
	return_code = MPI_Pack(bgm, sizeof(struct mdhim_bgetm_t), MPI_CHAR, 
			       *sendbuf, mesg_size, 
                               &mesg_idx, md->mdhim_comm);
         
        // For the each of the keys and data pack the chars plus one int for key_len.
        for (i=0; i < bgm->num_records; i++) {	
		return_code += MPI_Pack(&bgm->key_lens[i], 1, MPI_INT, 
					*sendbuf, mesg_size, 
                                        &mesg_idx, md->mdhim_comm);
                return_code += MPI_Pack(bgm->keys[i], bgm->key_lens[i], MPI_CHAR, 
					*sendbuf, mesg_size, 
                                        &mesg_idx, md->mdhim_comm);
        }

	// If the pack did not succeed then log the error and return the error code
	if ( return_code != MPI_SUCCESS ) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to pack "
                     "the bulk get message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }

	return MDHIM_SUCCESS;
}

/**
 * unpack_get_message
 * Unpacks a get message structure into contiguous memory for message passing
 *
 * @param md         in   main MDHIM struct
 * @param message    in   pointer for packed message we received
 * @param mesg_size  in   size of the incoming message
 * @param pm         out  structure get_message which will be unpacked from the message 
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 * 
 * struct mdhim_getm_t {
 int mtype;  
 //Operation type e.g., MDHIM_GET_VAL, MDHIM_GET_NEXT, MDHIM_GET_PREV
 int op;  
 void *key;
 int key_len;
 int server_rank;
 };
*/
int unpack_get_message(struct mdhim_t *md, void *message, int mesg_size, void **getm) {
	int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
    	int mesg_idx = 0;  // Variable for incremental unpack

        mlog(MDHIM_SERVER_DBG, "MDHIM Rank: %d - Unpacking get message with size: %d",
	     md->mdhim_rank, mesg_size);
        if ((*((struct mdhim_getm_t **) getm) = malloc(sizeof(struct mdhim_getm_t))) == NULL) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack get message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }
        
        // Unpack the message first with the structure and then followed by key and data values.
        return_code = MPI_Unpack(message, mesg_size, &mesg_idx, *((struct mdhim_getm_t **) getm), 
				 sizeof(struct mdhim_getm_t), MPI_CHAR, md->mdhim_comm);
        
        // Unpack key by first allocating memory and then extracting the values from message
        if (((*((struct mdhim_getm_t **) getm))->key = 
	     malloc((*((struct mdhim_getm_t **) getm))->key_len * sizeof(char))) == NULL) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack get message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }
        return_code += MPI_Unpack(message, mesg_size, &mesg_idx, 
				  (*((struct mdhim_getm_t **) getm))->key, 
				  (*((struct mdhim_getm_t **) getm))->key_len, 
				  MPI_CHAR, md->mdhim_comm);

	// If the unpack did not succeed then log the error and return the error code
	if ( return_code != MPI_SUCCESS ) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to unpack "
                     "the get message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }

	return MDHIM_SUCCESS;
}

/**
 * unpack_bget_message
 * Unpacks a bulk get message structure into contiguous memory for message passing
 *
 * @param md         in   main MDHIM struct
 * @param message    in   pointer for packed message we received
 * @param mesg_size  in   size of the incoming message
 * @param bgm        out  structure bulk_get_message which will be unpacked from the message 
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 * 
 * struct mdhim_bgetm_t {
 int mtype;
 int op;
 void **keys;
 int *key_lens;
 int num_records;
 int server_rank;
 };
*/
int unpack_bget_message(struct mdhim_t *md, void *message, int mesg_size, void **bgetm) {
	int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
    	int mesg_idx = 0;  // Variable for incremental unpack
        int i;
	int num_records;

        mlog(MDHIM_SERVER_DBG, "MDHIM Rank: %d - Unpacking bget message with size: %d",
	     md->mdhim_rank, mesg_size);
        if ((*((struct mdhim_bgetm_t **) bgetm) = malloc(sizeof(struct mdhim_bgetm_t))) == NULL) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack bget message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }
        
        // Unpack the message first with the structure and then followed by key and data values.
        return_code = MPI_Unpack(message, mesg_size, &mesg_idx, *((struct mdhim_bgetm_t **) bgetm),
				 sizeof(struct mdhim_bgetm_t), 
				 MPI_CHAR, md->mdhim_comm);
        
	num_records = (*((struct mdhim_bgetm_t **) bgetm))->num_records;			 
	// Allocate memory for key pointers, to be populated later.
        if (((*((struct mdhim_bgetm_t **) bgetm))->keys = 
	     malloc(num_records * sizeof(void *))) == NULL) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
		     "memory to unpack bget message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }	
        // Allocate memory for key_lens, to be populated later.
        if (((*((struct mdhim_bgetm_t **) bgetm))->key_lens = 
	     (int *)malloc(num_records * sizeof(int))) == NULL) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack bget message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }
        
	memset((*((struct mdhim_bgetm_t **) bgetm))->key_lens, 0, num_records * sizeof(int));
        // For the each of the keys and data unpack the chars plus an int for key_lens[i].
        for (i=0; i < num_records; i++) {	
		// Unpack the key_lens[i]
		return_code += MPI_Unpack(message, mesg_size, &mesg_idx, 
					  &(*((struct mdhim_bgetm_t **) bgetm))->key_lens[i], 
					  1, MPI_INT, md->mdhim_comm);
		// Unpack key by first allocating memory and then extracting the values from message
		if (((*((struct mdhim_bgetm_t **) bgetm))->keys[i] = 
		     malloc((*((struct mdhim_bgetm_t **) bgetm))->key_lens[i] * 
			    sizeof(char))) == NULL) {
			mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
			     "memory to unpack bget message.", md->mdhim_rank);
			return MDHIM_ERROR; 
		}

		return_code += MPI_Unpack(message, mesg_size, &mesg_idx, 
					  (*((struct mdhim_bgetm_t **) bgetm))->keys[i], 
					  (*((struct mdhim_bgetm_t **) bgetm))->key_lens[i], 
					  MPI_CHAR, md->mdhim_comm);            
        }

	// If the unpack did not succeed then log the error and return the error code
	if ( return_code != MPI_SUCCESS ) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to unpack "
                     "the bget message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }

	return MDHIM_SUCCESS;
}

///------------------------

/**
 * pack_getrm_message
 * Packs a get return message structure into contiguous memory for message passing
 *
 * @param md       in   main MDHIM struct
 * @param grm      in   structure get_return_message which will be packed into the message 
 * @param sendbuf  out  double pointer for packed message to send
 * @param sendsize out  pointer to sendbuf's size
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 * 
 * struct mdhim_getrm_t {
 int mtype;
 int error;
 void *key;
 int key_len;
 void *value;
 int value_len;
 };
*/
int pack_getrm_message(struct mdhim_t *md, struct mdhim_getrm_t *grm, void **sendbuf, int *sendsize) {

	int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
        int64_t m_size = sizeof(struct mdhim_getrm_t); // Generous variable for size calculation
        int mesg_size;  // Variable to be used as parameter for MPI_pack of safe size
    	int mesg_idx = 0;  // Variable for incremental pack
        void *outbuf;

        // Add to size the length of the key and data fields
        m_size += grm->key_len + grm->value_len;
        
        if (m_size > MDHIM_MAX_MSG_SIZE) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: getrm message too large. Get return "
                     "message is over Maximum size allowed of %d.", md->mdhim_rank, MDHIM_MAX_MSG_SIZE);
		return MDHIM_ERROR; 
        }
        mesg_size = m_size;
        *sendsize = mesg_size;
	grm->size = mesg_size;

        // Is the computed message size of a safe value? (less than a max message size?)
        if ((*sendbuf = malloc(mesg_size * sizeof(char))) == NULL) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to pack get return message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }
        
	outbuf = *sendbuf;
        // pack the message first with the structure and then followed by key and data values.
	return_code = MPI_Pack(grm, sizeof(struct mdhim_getrm_t), MPI_CHAR, outbuf, mesg_size, 
			       &mesg_idx, md->mdhim_comm);
	if (grm->key_len) {
		return_code += MPI_Pack(grm->key, grm->key_len, MPI_CHAR, outbuf, mesg_size, 
					&mesg_idx, md->mdhim_comm);
	}
	if (grm->value_len) {
		return_code += MPI_Pack(grm->value, grm->value_len, MPI_CHAR, outbuf, mesg_size, 
					&mesg_idx, md->mdhim_comm);
	}

	// If the pack did not succeed then log the error and return the error code
	if ( return_code != MPI_SUCCESS ) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to pack "
                     "the get return message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }

	return MDHIM_SUCCESS;
}

/**
 * pack_bgetrm_message
 * Packs a bulk get return message structure into contiguous memory for message passing
 *
 * @param md       in   main MDHIM struct
 * @param bgrm     in   structure bget_return_message which will be packed into the message 
 * @param sendbuf  out  double pointer for packed message to send
 * @param sendsize out  pointer to sendbuf's size
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 * 
 * struct mdhim_bgetrm_t {
 int mtype;
 int error;
 void **keys;
 int *key_lens;
 void **values;
 int *value_lens;
 int num_records;
 };
*/
int pack_bgetrm_message(struct mdhim_t *md, struct mdhim_bgetrm_t *bgrm, void **sendbuf, int *sendsize) {
	int return_code = MPI_SUCCESS; // MPI_SUCCESS = 0
        int64_t m_size = sizeof(struct mdhim_bgetrm_t);  // Generous variable for size calc
        int mesg_size;   // Variable to be used as parameter for MPI_pack of safe size
    	int mesg_idx = 0;
        int i;
        void *outbuf;

        // For each of the lens (key_lens and data_lens)
        // WARNING We are treating ints as the same size as char for packing purposes
        m_size += 2 * bgrm->num_records * sizeof(int);
        
        // For the each of the keys and data add enough chars.
        for (i=0; i < bgrm->num_records; i++)
                m_size += bgrm->key_lens[i] + bgrm->value_lens[i];
        
        // Is the computed message size of a safe value? (less than a max message size?)
        if (m_size > MDHIM_MAX_MSG_SIZE) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: bulk get return message too large."
		     " Bget return message is over Maximum size allowed of %d.", md->mdhim_rank, 
		     MDHIM_MAX_MSG_SIZE);
		return MDHIM_ERROR; 
        }
        mesg_size = m_size;  // Safe size to use in MPI_pack     
	*sendsize = mesg_size;
	bgrm->size = mesg_size;

        if ((*sendbuf = malloc(mesg_size * sizeof(char))) == NULL) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to pack bulk get return message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }
        
	outbuf = *sendbuf;
        // pack the message first with the structure and then followed by key and data values (plus lengths).
	return_code = MPI_Pack(bgrm, sizeof(struct mdhim_bgetrm_t), MPI_CHAR, outbuf, mesg_size, 
			       &mesg_idx, md->mdhim_comm);
         
        // For the each of the keys and data pack the chars plus two ints for key_len and data_len.
        for (i=0; i < bgrm->num_records; i++) {
                return_code += MPI_Pack(&bgrm->key_lens[i], 1, MPI_INT, outbuf, mesg_size, 
					&mesg_idx, md->mdhim_comm);
		if (bgrm->key_lens[i] > 0) {
			return_code += MPI_Pack(bgrm->keys[i], bgrm->key_lens[i], MPI_CHAR, outbuf, 
						mesg_size, &mesg_idx, md->mdhim_comm);
		}

                return_code += MPI_Pack(&bgrm->value_lens[i], 1, MPI_INT, outbuf, mesg_size, 
					&mesg_idx, md->mdhim_comm);
		/* Pack the value retrieved from the db
		   There is a chance that the key didn't exist in the db */
		if (bgrm->value_lens[i] > 0) {
			return_code += MPI_Pack(bgrm->values[i], bgrm->value_lens[i], 
						MPI_CHAR, outbuf, mesg_size, 
						&mesg_idx, md->mdhim_comm);
		} 
        }

	// If the pack did not succeed then log the error and return the error code
	if ( return_code != MPI_SUCCESS ) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to pack "
                     "the bulk get return message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }

	return MDHIM_SUCCESS;
}

/**
 * unpack_getrm_message
 * Unpacks a get return message structure into contiguous memory for message passing
 *
 * @param md         in   main MDHIM struct
 * @param message    in   pointer for packed message we received
 * @param mesg_size  in   size of the incoming message
 * @param getrm      out  structure getrm_message which will be unpacked from the message 
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 * 
 * struct mdhim_getrm_t {
 int mtype;
 int error;
 void *key;
 int key_len;
 void *value;
 int value_len;
 };
*/
int unpack_getrm_message(struct mdhim_t *md, void *message, int mesg_size, void **getrm) {

	int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
    	int mesg_idx = 0;  // Variable for incremental unpack
        struct mdhim_getrm_t *grm;

        if ((*((struct mdhim_getrm_t **) getrm) = malloc(sizeof(struct mdhim_getrm_t))) == NULL) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack put message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }
        
        grm = *((struct mdhim_getrm_t **) getrm);
        // Unpack the message first with the structure and then followed by key and data values.
        return_code = MPI_Unpack(message, mesg_size, &mesg_idx, grm, sizeof(struct mdhim_getrm_t), 
				 MPI_CHAR, md->mdhim_comm);
        
        // Unpack key by first allocating memory and then extracting the values from message
	grm->key = NULL;
        if (grm->key_len && (grm->key = (char *)malloc(grm->key_len * sizeof(char))) == NULL) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack get return message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }
	if (grm->key) {
		return_code += MPI_Unpack(message, mesg_size, &mesg_idx, grm->key, grm->key_len, 
					  MPI_CHAR, md->mdhim_comm);
	}
        
        // Unpack data by first allocating memory and then extracting the values from message
	grm->value = NULL;
        if (grm->value_len && 
	    (grm->value = (char *)malloc(grm->value_len * sizeof(char))) == NULL) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack get return message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }
	if (grm->value) {
		return_code += MPI_Unpack(message, mesg_size, &mesg_idx, grm->value, grm->value_len,
					  MPI_CHAR, md->mdhim_comm);
	}

	// If the unpack did not succeed then log the error and return the error code
	if ( return_code != MPI_SUCCESS ) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to unpack "
                     "the get return message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }

	return MDHIM_SUCCESS;
}

/**
 * unpack_bgetrm_message
 * Unpacks a bulk get return message structure into contiguous memory for message passing
 *
 * @param md         in   main MDHIM struct
 * @param message    in   pointer for packed message
 * @param mesg_size  in   size of the incoming message
 * @param bgtrm      out  structure bulk_get_return_message which will be unpacked from the message 
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 * 
 * struct mdhim_bgetrm_t {
 int mtype;
 int error;
 void **keys;
 int *key_lens;
 void **values;
 int *value_lens;
 int num_records;
 };
*/
int unpack_bgetrm_message(struct mdhim_t *md, void *message, int mesg_size, void **bgetrm) {

	int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
    	int mesg_idx = 0;  // Variable for incremental unpack
        int i;
        struct mdhim_bgetrm_t *bgrm;

        if ((*((struct mdhim_bgetrm_t **) bgetrm) = malloc(sizeof(struct mdhim_bgetrm_t))) == NULL) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack bget return message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }
        
        bgrm = *((struct mdhim_bgetrm_t **) bgetrm);
        // Unpack the message first with the structure and then followed by key and data values.
        return_code = MPI_Unpack(message, mesg_size, &mesg_idx, bgrm, sizeof(struct mdhim_bgetrm_t), 
				 MPI_CHAR, md->mdhim_comm);
        
	// Allocate memory for key pointers, to be populated later.
        if ((bgrm->keys = malloc(bgrm->num_records * sizeof(void *))) == NULL) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
		     "memory to unpack bgetrm message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }	

        // Allocate memory for key_lens, to be populated later.
        if ((bgrm->key_lens = malloc(bgrm->num_records * sizeof(int))) == NULL) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack bget return message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }
        
	// Allocate memory for value pointers, to be populated later.
        if ((bgrm->values = malloc(bgrm->num_records * sizeof(void *))) == NULL) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
		     "memory to unpack bgetrm message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }

        // Allocate memory for value_lens, to be populated later.
        if ((bgrm->value_lens = (int *)malloc(bgrm->num_records * sizeof(int))) == NULL) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack bget return message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }
        
        // For the each of the keys and data unpack the chars plus two ints for key_lens[i] and data_lens[i].
        for (i=0; i < bgrm->num_records; i++) {
		// Unpack the key_lens[i]
		return_code += MPI_Unpack(message, mesg_size, &mesg_idx, &bgrm->key_lens[i], 1, 
					  MPI_INT, md->mdhim_comm);
            
		// Unpack key by first allocating memory and then extracting the values from message
		bgrm->keys[i] = NULL;
		if (bgrm->key_lens[i] && 
		    (bgrm->keys[i] = (char *)malloc(bgrm->key_lens[i] * sizeof(char))) == NULL) {
			mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
			     "memory to unpack bget return message.", md->mdhim_rank);
			return MDHIM_ERROR; 
		}
		if (bgrm->keys[i]) {
			return_code += MPI_Unpack(message, mesg_size, &mesg_idx, bgrm->keys[i], bgrm->key_lens[i], 
						  MPI_CHAR, md->mdhim_comm);
		}
            
		// Unpack the value_lens[i]
		return_code += MPI_Unpack(message, mesg_size, &mesg_idx, &bgrm->value_lens[i], 1, 
					  MPI_INT, md->mdhim_comm);

		//There wasn't a value found for this key
		if (!bgrm->value_lens[i]) {
			bgrm->values[i] = NULL;
			continue;
		} 

		// Unpack data by first allocating memory and then extracting the values from message
		bgrm->values[i] = NULL;
		if (bgrm->value_lens[i] && 
		    (bgrm->values[i] = (char *)malloc(bgrm->value_lens[i] * sizeof(char))) == NULL) {
			mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
			     "memory to unpack bget return message.", md->mdhim_rank);
			return MDHIM_ERROR; 
		}
		if (bgrm->values[i]) {
			return_code += MPI_Unpack(message, mesg_size, &mesg_idx, bgrm->values[i], 
						  bgrm->value_lens[i], 
						  MPI_CHAR, md->mdhim_comm);
		}
	    
        }

	// If the unpack did not succeed then log the error and return the error code
	if ( return_code != MPI_SUCCESS ) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to unpack "
                     "the bget return message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }

	return MDHIM_SUCCESS;
}

///------------------------

/**
 * pack_base_message
 * Packs a base message structure into contiguous memory for message passing
 *
 * @param md       in   main MDHIM struct
 * @param cm       in   structure base message which will be packed into the sendbuf 
 * @param sendbuf  out  double pointer for packed message to send
 * @param sendsize out  pointer for packed message size
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 * 
 * struct mdhim_basem_t {
 int mtype;
 };
*/
int pack_base_message(struct mdhim_t *md, struct mdhim_basem_t *cm, void **sendbuf, int *sendsize) {

	int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
        int64_t m_size = sizeof(struct mdhim_basem_t); // Generous variable for size calculation
        int mesg_size;  // Variable to be used as parameter for MPI_pack of safe size
    	int mesg_idx = 0;  // Variable for incremental pack
        void *outbuf;
      
        mesg_size = m_size;
        *sendsize = mesg_size;
	cm->size = mesg_size;

        if ((*sendbuf = malloc(mesg_size * sizeof(char))) == NULL) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to pack del message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }
        
	outbuf = *sendbuf;
        // pack the message first with the structure and then followed by key and data values.
	return_code = MPI_Pack(cm, sizeof(struct mdhim_basem_t), MPI_CHAR, outbuf, mesg_size, 
			       &mesg_idx, md->mdhim_comm);

	// If the pack did not succeed then log the error and return the error code
	if ( return_code != MPI_SUCCESS ) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to pack "
                     "the del message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }

	return MDHIM_SUCCESS;
}

///------------------------

/**
 * pack_del_message
 * Packs a delete message structure into contiguous memory for message passing
 *
 * @param md       in   main MDHIM struct
 * @param dm       in   structure del_message which will be packed into the sendbuf 
 * @param sendbuf  out  double pointer for packed message to send
 * @param sendsize out  pointer for packed message size
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 * 
 * struct mdhim_delm_t {
 int mtype;
 void *key;
 int key_len; 
 int server_rank;
 };
*/
int pack_del_message(struct mdhim_t *md, struct mdhim_delm_t *dm, void **sendbuf, int *sendsize) {

	int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
        int64_t m_size = sizeof(struct mdhim_delm_t); // Generous variable for size calculation
        int mesg_size;  // Variable to be used as parameter for MPI_pack of safe size
    	int mesg_idx = 0;  // Variable for incremental pack

        // Add to size the length of the key and data fields
        m_size += dm->key_len;
        
        // Is the computed message size of a safe value? (less than a max message size?)
        if (m_size > MDHIM_MAX_MSG_SIZE) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: del message too large."
                     " Del is over Maximum size allowed of %d.", md->mdhim_rank, MDHIM_MAX_MSG_SIZE);
		return MDHIM_ERROR; 
        }
        mesg_size = m_size;
        *sendsize = mesg_size;
	dm->size = mesg_size;

        if ((*sendbuf = malloc(mesg_size * sizeof(char))) == NULL) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to pack del message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }
        
        // pack the message first with the structure and then followed by key and data values.
	return_code = MPI_Pack(dm, sizeof(struct mdhim_delm_t), MPI_CHAR, *sendbuf, 
			       mesg_size, &mesg_idx, md->mdhim_comm);
        return_code += MPI_Pack(dm->key, dm->key_len, MPI_CHAR, *sendbuf, 
				mesg_size, &mesg_idx, md->mdhim_comm);

	// If the pack did not succeed then log the error and return the error code
	if ( return_code != MPI_SUCCESS ) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to pack "
                     "the del message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }

	return MDHIM_SUCCESS;
}

/**
 * pack_bdel_message
 * Packs a bdel message structure into contiguous memory for message passing
 *
 * @param md       in   main MDHIM struct
 * @param bdm      in   structure bdel_message which will be packed into the message 
 * @param sendbuf  out  double pointer for packed message to send
 * @param sendsize out  pointer for packed message size
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 * 
 * struct mdhim_bdelm_t {
 int mtype;  
 void **keys;
 int *key_lens;
 int num_records;
 int server_rank;
 };
*/
int pack_bdel_message(struct mdhim_t *md, struct mdhim_bdelm_t *bdm, void **sendbuf, 
		      int *sendsize) {

	int return_code = MPI_SUCCESS; // MPI_SUCCESS = 0
        int64_t m_size = sizeof(struct mdhim_bdelm_t);  // Generous variable for size calc
        int mesg_size;   // Variable to be used as parameter for MPI_pack of safe size
    	int mesg_idx = 0;
        int i;
        void *outbuf;

        // Add up the size of message
        m_size += bdm->num_records * sizeof(int);
        
        // For the each of the keys add enough chars.
        for (i=0; i < bdm->num_records; i++)
                m_size += bdm->key_lens[i];
        
        // Is the computed message size of a safe value? (less than a max message size?)
        if (m_size > MDHIM_MAX_MSG_SIZE) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: bulk del message too large."
                     " Bdel is over Maximum size allowed of %d.", md->mdhim_rank, MDHIM_MAX_MSG_SIZE);
		return MDHIM_ERROR; 
        }
        mesg_size = m_size;  // Safe size to use in MPI_pack     
	*sendsize = mesg_size;
	bdm->size = mesg_size;

        if ((*sendbuf = malloc(mesg_size * sizeof(char))) == NULL) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to pack bulk del message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }
        
	outbuf = *sendbuf;
        // pack the message first with the structure and then followed by key (plus lengths).
	return_code = MPI_Pack(bdm, sizeof(struct mdhim_bdelm_t), MPI_CHAR, *sendbuf, 
			       mesg_size, &mesg_idx, md->mdhim_comm);
         
        // For the each of the keys and data pack the chars plus one int for key_len.
        for (i=0; i < bdm->num_records; i++) {
                return_code += MPI_Pack(&bdm->key_lens[i], 1, MPI_INT, *sendbuf, 
					mesg_size, &mesg_idx, md->mdhim_comm);
                return_code += MPI_Pack(bdm->keys[i], bdm->key_lens[i], MPI_CHAR, 
					*sendbuf, mesg_size, &mesg_idx, 
					md->mdhim_comm);
        }

	// If the pack did not succeed then log the error and return the error code
	if ( return_code != MPI_SUCCESS ) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to pack "
                     "the bulk del message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }

	return MDHIM_SUCCESS;
}

/**
 * unpack_del_message
 * Unpacks a del message structure into contiguous memory for message passing
 *
 * @param md         in   main MDHIM struct
 * @param message    in   pointer for packed message
 * @param mesg_size  in   size of the incoming message
 * @param delm         out  structure get_message which will be unpacked from the message 
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 * 
 * struct mdhim_delm_t {
 int mtype;
 void *key;
 int key_len; 
 int server_rank;
 };
*/
int unpack_del_message(struct mdhim_t *md, void *message, int mesg_size, void **delm) {

	int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
    	int mesg_idx = 0;  // Variable for incremental unpack
        struct mdhim_delm_t *dm;

        if ((*((struct mdhim_delm_t **) delm) = malloc(sizeof(struct mdhim_delm_t))) == NULL) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack del message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }
        
	dm = *((struct mdhim_delm_t **) delm);
        // Unpack the message first with the structure and then followed by key and data values.
        return_code = MPI_Unpack(message, mesg_size, &mesg_idx, dm, sizeof(struct mdhim_delm_t), 
				 MPI_CHAR, md->mdhim_comm);
        
        // Unpack key by first allocating memory and then extracting the values from message
        if ((dm->key = (char *)malloc(dm->key_len * sizeof(char))) == NULL) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack del message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }
        return_code += MPI_Unpack(message, mesg_size, &mesg_idx, dm->key, dm->key_len, 
				  MPI_CHAR, md->mdhim_comm);

	// If the unpack did not succeed then log the error and return the error code
	if ( return_code != MPI_SUCCESS ) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to unpack "
                     "the del message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }

	return MDHIM_SUCCESS;
}

/**
 * unpack_bdel_message
 * Unpacks a bulk del message structure into contiguous memory for message passing
 *
 * @param md         in   main MDHIM struct
 * @param message    in   pointer for packed message
 * @param mesg_size  in   size of the incoming message
 * @param bdelm        out  structure bulk_del_message which will be unpacked from the message 
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 * 
 * struct mdhim_bdelm_t {
 int mtype;  
 void **keys;
 int *key_lens;
 int num_records;
 int server_rank;
 };
*/
int unpack_bdel_message(struct mdhim_t *md, void *message, int mesg_size, void **bdelm) {

	int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
    	int mesg_idx = 0;  // Variable for incremental unpack
        int i;
	int num_records;

        if ((*((struct mdhim_bdelm_t **) bdelm) = malloc(sizeof(struct mdhim_bdelm_t))) == NULL) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
		     "memory to unpack bdel message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }
        
        // Unpack the message first with the structure and then followed by key and data values.
        return_code = MPI_Unpack(message, mesg_size, &mesg_idx, 
				 (*((struct mdhim_bdelm_t **) bdelm)), sizeof(struct mdhim_bdelm_t), 
				 MPI_CHAR, md->mdhim_comm);
        
	num_records = (*((struct mdhim_bdelm_t **) bdelm))->num_records;
	// Allocate memory for keys, to be populated later.
        if (((*((struct mdhim_bdelm_t **) bdelm))->keys = 
	     malloc(num_records * sizeof(void *))) == NULL) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack bdel message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }
        // Allocate memory for key_lens, to be populated later.
        if (((*((struct mdhim_bdelm_t **) bdelm))->key_lens = 
	     (int *)malloc(num_records * sizeof(int))) == NULL) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack bdel message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }
        
        // For the each of the keys and data unpack the chars plus an int for key_lens[i].
        for (i=0; i < num_records; i++) {
		// Unpack the key_lens[i]
		return_code += MPI_Unpack(message, mesg_size, &mesg_idx, 
					  &(*((struct mdhim_bdelm_t **) bdelm))->key_lens[i], 1,
					  MPI_INT, md->mdhim_comm);
		
		// Unpack key by first allocating memory and then extracting the values from message
		if (((*((struct mdhim_bdelm_t **) bdelm))->keys[i] = 
		     (char *)malloc((*((struct mdhim_bdelm_t **) bdelm))->key_lens[i] * 
				    sizeof(char))) == NULL) {
			mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
			     "memory to unpack bdel message.", md->mdhim_rank);
			return MDHIM_ERROR; 
		}
		return_code += MPI_Unpack(message, mesg_size, &mesg_idx, 
					  (*((struct mdhim_bdelm_t **) bdelm))->keys[i], 
					  (*((struct mdhim_bdelm_t **) bdelm))->key_lens[i], 
					  MPI_CHAR, md->mdhim_comm);            
        }
	
	// If the unpack did not succeed then log the error and return the error code
	if ( return_code != MPI_SUCCESS ) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - Error: unable to unpack "
                     "the bdel message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }
	
	return MDHIM_SUCCESS;
}

///------------------------


/**
 * pack_return_message
 * Packs a return message structure into contiguous memory for message passing
 *
 * @param md       in   main MDHIM struct
 * @param pm       in   structure which will be packed into the sendbuf 
 * @param sendbuf  out  double pointer for packed message to send
 * @param sendsize out  pointer for packed message size
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 * 
 * struct mdhim_rm_t {
 int mtype;  
 int error;
 };
*/
int pack_return_message(struct mdhim_t *md, struct mdhim_rm_t *rm, void **sendbuf, int *sendsize) {

	int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
        int mesg_size = sizeof(struct mdhim_rm_t); 
        int mesg_idx = 0;
        void *outbuf;
	
	*sendsize = mesg_size;
	rm->size = mesg_size;

        if ((*sendbuf = malloc(mesg_size * sizeof(char))) == NULL) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to pack return message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }
        
	outbuf = *sendbuf;
        // Pack the message from the structure
	return_code = MPI_Pack(rm, sizeof(struct mdhim_rm_t), MPI_CHAR, outbuf, mesg_size, &mesg_idx, 
			       md->mdhim_comm);

	// If the pack did not succeed then log the error and return the error code
	if ( return_code != MPI_SUCCESS ) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to pack "
                     "the return message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }

	return MDHIM_SUCCESS;
}

/**
 * unpack_return_message
 * unpacks a return message structure into contiguous memory for message passing
 *
 * @param md      in   main MDHIM struct
 * @param pm      in   structure return_message which will be packed into the message 
 * @param message out  pointer for packed message
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 * 
 * struct mdhim_rm_t {
 int mtype;  
 int error;
 };
*/
int unpack_return_message(struct mdhim_t *md, void *message, void **retm) {

	int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
        int mesg_size = sizeof(struct mdhim_rm_t); 
        int mesg_idx = 0;
        struct mdhim_rm_t *rm;

        if (((*(struct mdhim_rm_t **) retm) = malloc(sizeof(struct mdhim_rm_t))) == NULL) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack return message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }
        
        rm = *((struct mdhim_rm_t **) retm);

        // Unpack the structure from the message
        return_code = MPI_Unpack(message, mesg_size, &mesg_idx, rm, sizeof(struct mdhim_rm_t), 
				 MPI_CHAR, md->mdhim_comm);

	// If the pack did not succeed then log the error and return the error code
	if ( return_code != MPI_SUCCESS ) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to unpack "
                     "the return message.", md->mdhim_rank);
		return MDHIM_ERROR; 
        }

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
		rsi.rangesrv_num = md->mdhim_rs->info.rangesrv_num;
		mlog(MDHIM_SERVER_DBG, "Rank: %d - " 
		     "I'm a range server with range server number: %u", 
		     md->mdhim_rank, rsi.rangesrv_num);
	} else {
		//I'm not a range server
		rsi.rangesrv_num = 0;
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
	if ((ret = MPI_Allgather(sendbuf, sendsize, MPI_PACKED, recvbuf, sendsize,
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

		if (rsi.rangesrv_num != 0) {
			mlog(MDHIM_CLIENT_DBG, "Rank: %d - " 
			     "Received range server with rank: %d, range server number: %u", 
			     md->mdhim_rank, i, rsi.rangesrv_num);
		} else {
			continue;
		}

		rs_info = malloc(sizeof(struct rangesrv_info));
		rs_info->rank = i;
		rs_info->rangesrv_num = rsi.rangesrv_num;
		rs_info->next = NULL;
		if (!rs_head) {
			rs_head = rs_info;
			rs_tail = rs_info;
		} else {
			rs_tail->next = rs_info;
			rs_info->prev = rs_tail;
			rs_tail = rs_info;
		}

		//Set the master range server to be the server with the largest rank
		if (rs_info->rank > md->rangesrv_master) {
			md->rangesrv_master = rs_info->rank;
		}
	}

	//Set the range server list in the md struct
	md->rangesrvs = rs_head;

	return rs_head;
}

/**
 * Frees all memory taken up by messages - including keys and values
 *
 * @param msg          pointer to the message to free
 */
void mdhim_full_release_msg(void *msg) {
	int mtype;
	int i;

	if (!msg) {
		return;
	}

	//Determine the message type and free accordingly
	mtype = ((struct mdhim_basem_t *) msg)->mtype;
	switch(mtype) {
	case MDHIM_RECV:
		free((struct mdhim_rm_t *) msg);
		break;
	case MDHIM_RECV_GET:
		if (((struct mdhim_getrm_t *) msg)->key) {
			free(((struct mdhim_getrm_t *) msg)->key);
		}	

		if (((struct mdhim_getrm_t *) msg)->value) {
			free(((struct mdhim_getrm_t *) msg)->value);
		}	

		free((struct mdhim_getrm_t *) msg);
		break;
	case MDHIM_RECV_BULK_GET:
		for (i = 0; i < ((struct mdhim_bgetrm_t *) msg)->num_records; i++) {
			if (((struct mdhim_bgetrm_t *) msg)->keys[i]) {
				free(((struct mdhim_bgetrm_t *) msg)->keys[i]);
			}
			if (((struct mdhim_bgetrm_t *) msg)->value_lens[i] && 
			    ((struct mdhim_bgetrm_t *) msg)->values[i]) {
				free(((struct mdhim_bgetrm_t *) msg)->values[i]);
			}
		}
		
		if (((struct mdhim_bgetrm_t *) msg)->key_lens) {
			free(((struct mdhim_bgetrm_t *) msg)->key_lens);	
		}
		if (((struct mdhim_bgetrm_t *) msg)->keys) {
			free(((struct mdhim_bgetrm_t *) msg)->keys);	
		}
		if (((struct mdhim_bgetrm_t *) msg)->value_lens) {
			free(((struct mdhim_bgetrm_t *) msg)->value_lens);	
		}
		if (((struct mdhim_bgetrm_t *) msg)->values) {
			free(((struct mdhim_bgetrm_t *) msg)->values);	
		}

		free((struct mdhim_bgetrm_t *) msg);
		break;
	case MDHIM_BULK_PUT:
		for (i = 0; i < ((struct mdhim_bputm_t *) msg)->num_records; i++) {
			if (((struct mdhim_bputm_t *) msg)->keys[i]) {
				free(((struct mdhim_bputm_t *) msg)->keys[i]);
			}
			if (((struct mdhim_bputm_t *) msg)->values[i]) {
				free(((struct mdhim_bputm_t *) msg)->values[i]);
			}
		}
		
		if (((struct mdhim_bputm_t *) msg)->key_lens) {
			free(((struct mdhim_bputm_t *) msg)->key_lens);	
		}
		if (((struct mdhim_bputm_t *) msg)->keys) {
			free(((struct mdhim_bputm_t *) msg)->keys);	
		}
		if (((struct mdhim_bputm_t *) msg)->value_lens) {
			free(((struct mdhim_bputm_t *) msg)->value_lens);	
		}
		if (((struct mdhim_bputm_t *) msg)->values) {
			free(((struct mdhim_bputm_t *) msg)->values);	
		}

		free((struct mdhim_bputm_t *) msg);
		break;
	default:
		break;
	}
}


/**
 * Frees memory taken up by messages except for the keys and values
 *
 * @param msg          pointer to the message to free
 */
void mdhim_partial_release_msg(void *msg) {
	int mtype;

	if (!msg) {
		return;
	}

	//Determine the message type and free accordingly
	mtype = ((struct mdhim_basem_t *) msg)->mtype;
	switch(mtype) {
	case MDHIM_RECV:
		free((struct mdhim_rm_t *) msg);
		break;
	case MDHIM_RECV_GET:
		free((struct mdhim_getrm_t *) msg);
		break;
	case MDHIM_RECV_BULK_GET:			
		if (((struct mdhim_bgetrm_t *) msg)->key_lens) {
			free(((struct mdhim_bgetrm_t *) msg)->key_lens);	
		}
		if (((struct mdhim_bgetrm_t *) msg)->keys) {
			free(((struct mdhim_bgetrm_t *) msg)->keys);	
		}
		if (((struct mdhim_bgetrm_t *) msg)->value_lens) {
			free(((struct mdhim_bgetrm_t *) msg)->value_lens);	
		}
		if (((struct mdhim_bgetrm_t *) msg)->values) {
			free(((struct mdhim_bgetrm_t *) msg)->values);	
		}

		free((struct mdhim_bgetrm_t *) msg);
		break;
	case MDHIM_BULK_PUT:	
		if (((struct mdhim_bputm_t *) msg)->key_lens) {
			free(((struct mdhim_bputm_t *) msg)->key_lens);	
		}
		if (((struct mdhim_bputm_t *) msg)->keys) {
			free(((struct mdhim_bputm_t *) msg)->keys);	
		}
		if (((struct mdhim_bputm_t *) msg)->value_lens) {
			free(((struct mdhim_bputm_t *) msg)->value_lens);	
		}
		if (((struct mdhim_bputm_t *) msg)->values) {
			free(((struct mdhim_bputm_t *) msg)->values);	
		}

		free((struct mdhim_bputm_t *) msg);
		break;
	default:
		break;
	}
}

/**
 * get_stat_flush
 * Receives stat data from all the range servers and populates md->stats
 *
 * @param md      in   main MDHIM struct
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int get_stat_flush(struct mdhim_t *md) {
	char *sendbuf;
	int sendsize;
	int sendidx = 0;
	int recvidx = 0;
	char *recvbuf;
	int recvsize;
	int ret = 0;
	int i = 0;
	int float_type = 0;
	struct mdhim_stat *stat, *tmp;
	void *tstat;
	int stat_size;
	struct mdhim_db_istat istat;
	struct mdhim_db_fstat fstat;
	int master;
	uint64_t num_items;

	//Get the sizes for the buffers and allocate them
	num_items = ceil(((double) MDHIM_MAX_RANGE_KEY/MDHIM_MAX_RECS_PER_SLICE)/md->num_rangesrvs);
	if ((ret = is_float_key(md->key_type)) == 1) {
		float_type = 1;
		sendsize = num_items * sizeof(struct mdhim_db_fstat);
	} else {
		float_type = 0;
		sendsize = num_items * sizeof(struct mdhim_db_istat);
	}
	recvsize = md->num_rangesrvs * sendsize;
	recvbuf = malloc(recvsize);
	memset(recvbuf, 0, recvsize);
	if (md->mdhim_rs) {
		//Acquire the work mutex to make sure the stats are not updated
		pthread_mutex_lock(md->mdhim_rs->work_queue_mutex);
	
		//Get the master range server rank according the range server comm
		if ((ret = MPI_Comm_size(md->mdhim_rs->rs_comm, &master)) != MPI_SUCCESS) {
			mlog(MPI_CRIT, "Rank: %d - " 
			     "Error packing buffer when sending stat info to master range server", 
			     md->mdhim_rank);
		}		
		//The master rank is the last rank in range server comm
		master--;

		//Allocate send buffer
		sendbuf = malloc(sendsize);		  
		//Pack the stat data I have by iterating through the stats hash
		HASH_ITER(hh, md->mdhim_rs->mdhim_store->mdhim_store_stats, stat, tmp) {
			//Get the appropriate struct to send
			if (float_type) {
				fstat.slice = stat->key;
				fstat.num = stat->num;
				fstat.dmin = *(long double *) stat->min;
				fstat.dmax = *(long double *) stat->max;
				tstat = &fstat;
				stat_size = sizeof(struct mdhim_db_fstat);
			} else {
				istat.slice = stat->key;
				istat.num = stat->num;
				istat.imin = *(uint64_t  *) stat->min;
				istat.imax = *(uint64_t  *) stat->max;
				tstat = &istat;
				stat_size = sizeof(struct mdhim_db_istat);
			}
		  
			//Pack the struct
			if ((ret = MPI_Pack(&tstat, stat_size, MPI_CHAR, sendbuf, sendsize, &sendidx, 
					    md->mdhim_rs->rs_comm)) != MPI_SUCCESS) {
				mlog(MPI_CRIT, "Rank: %d - " 
				     "Error packing buffer when sending stat info to master range server", 
				     md->mdhim_rank);
				return MDHIM_ERROR;
			}
		}

		pthread_mutex_unlock(md->mdhim_rs->work_queue_mutex);

		//The master server will receive the stat info from each rank in the range server comm
		if ((ret = MPI_Gather(sendbuf, sendsize, MPI_PACKED, recvbuf, sendsize,
				      MPI_PACKED, master, md->mdhim_rs->rs_comm)) != MPI_SUCCESS) {
			mlog(MPI_CRIT, "Rank: %d - " 
			     "Error while receiving range server info", 
			     md->mdhim_rank);
			return MDHIM_ERROR;
		}
	}

	//The master range server broadcasts the receive buffer to the mdhim_comm
	if ((ret = MPI_Bcast(recvbuf, recvsize, MPI_PACKED, md->rangesrv_master,
			     md->mdhim_comm)) != MPI_SUCCESS) {
		mlog(MPI_CRIT, "Rank: %d - " 
		     "Error while receiving range server info", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}

	//Unpack the receive buffer and populate our md->stats hash table
	for (i = 0; i < recvsize; i+=stat_size) {
		tstat = malloc(stat_size);
		if ((ret = MPI_Unpack(recvbuf, recvsize, &recvidx, tstat, stat_size, 
				      MPI_CHAR, md->mdhim_comm)) != MPI_SUCCESS) {
			mlog(MPI_CRIT, "Rank: %d - " 
			     "Error while unpacking stat data", 
			     md->mdhim_rank);
			return MDHIM_ERROR;
		}

		//Skip empty slices
		if (!((struct mdhim_db_fstat *)tstat)->slice) {
			continue;
		}

		stat = malloc(sizeof(struct mdhim_stat));
		if (float_type) {
			stat->min = (void *) malloc(sizeof(long double));
			stat->max = (void *) malloc(sizeof(long double));
			*(long double *)stat->min = ((struct mdhim_db_fstat *)tstat)->dmin;
			*(long double *)stat->max = ((struct mdhim_db_fstat *)tstat)->dmax;
			stat->key = ((struct mdhim_db_fstat *)tstat)->slice;
			stat->num = ((struct mdhim_db_fstat *)tstat)->num;
			mlog(MPI_CRIT, "Rank: %d - " 
			     "Retrieved stat for slice: %lu, min: %Lf, max: %Lf, num: %lu", 
			     md->mdhim_rank, stat->key, ((struct mdhim_db_fstat *)tstat)->dmin, 
			     ((struct mdhim_db_fstat *)tstat)->dmax, stat->num);
		} else {
			stat->min = (void *) malloc(sizeof(uint64_t));
			stat->max = (void *) malloc(sizeof(uint64_t));
			*(uint64_t *)stat->min = ((struct mdhim_db_istat *)tstat)->imin;
			*(uint64_t *)stat->max = ((struct mdhim_db_istat *)tstat)->imax;
			stat->key = ((struct mdhim_db_istat *)tstat)->slice;
			stat->num = ((struct mdhim_db_istat *)tstat)->num;
			mlog(MPI_CRIT, "Rank: %d - " 
			     "Retrieved stat for slice: %lu, min: %lu, max: %lu, num: %lu", 
			     md->mdhim_rank, stat->key, ((struct mdhim_db_istat *)tstat)->imin, 
			     ((struct mdhim_db_istat *)tstat)->imax, stat->num);
		}
		  
		//Add the stat to our hash table
		HASH_ADD_ULINT(md->stats, key, stat); 
		free(tstat);
	}
	
	free(sendbuf);
	free(recvbuf);
	return MDHIM_SUCCESS;
}
