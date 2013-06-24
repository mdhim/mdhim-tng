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
	int return_code;
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
	default:
		break;
	}

	if (return_code != MDHIM_SUCCESS || !sendbuf || !sendsize) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to pack "
                     "the message while sending.", md->mdhim_rank);
		return MDHIM_ERROR;
	}

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
 * @param src     out  pointer to source of message received
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int receive_rangesrv_work(struct mdhim_t *md, int *src, void **message) {
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
             	mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d "
                     "send message failed.", md->mdhim_rank, return_code);
		return MDHIM_ERROR;
	}

	//Unpack buffer to produce a message struct and place result in message pointer
	*src = msg_source;

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
        
        // Add to size the length of the key and data fields
        m_size += pm->key_len + pm->value_len;
        
        if (m_size > MDHIM_MAX_MSG_SIZE) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: put message too large."
                     " Put is over Maximum size allowed of %d.", md->mdhim_rank, MDHIM_MAX_MSG_SIZE);
             return MDHIM_ERROR; 
        }
        mesg_size = m_size;
        *sendsize = mesg_size;

        // Is the computed message size of a safe value? (less than a max message size?)
        if ((sendbuf = malloc(mesg_size * sizeof(char))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to pack put message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        
        // pack the message first with the structure and then followed by key and data values.
	return_code = MPI_Pack(pm, sizeof(struct mdhim_putm_t), MPI_CHAR, sendbuf, mesg_size, &mesg_idx, md->mdhim_comm);
        return_code += MPI_Pack(pm->key, pm->key_len, MPI_CHAR, sendbuf, mesg_size, &mesg_idx, md->mdhim_comm);
        return_code += MPI_Pack(pm->value, pm->value_len, MPI_CHAR, sendbuf, mesg_size, &mesg_idx, md->mdhim_comm);

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
	int num_keys;
	int server_rank;
};
 */
int pack_bput_message(struct mdhim_t *md, struct mdhim_bputm_t *bpm, void **sendbuf, int *sendsize) {
	int return_code = MPI_SUCCESS; // MPI_SUCCESS = 0
        int64_t m_size = sizeof(struct mdhim_bputm_t);  // Generous variable for size calc
        int mesg_size;   // Variable to be used as parameter for MPI_pack of safe size
    	int mesg_idx = 0;
        int i;
        
        // For each of the lens (key_lens and data_lens)
        // WARNING We are treating ints as the same size as char for packing purposes
        m_size += 2 * bpm->num_records;
        
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

        if ((sendbuf = malloc(mesg_size * sizeof(char))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to pack bulk put message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        
        // pack the message first with the structure and then followed by key and data values (plus lengths).
	return_code = MPI_Pack(bpm, sizeof(struct mdhim_bputm_t), MPI_CHAR, sendbuf, mesg_size, &mesg_idx, md->mdhim_comm);
         
        // For the each of the keys and data pack the chars plus two ints for key_len and data_len.
        for (i=0; i < bpm->num_records; i++) {
                return_code += MPI_Pack(&bpm->key_lens[i], 1, MPI_INT, sendbuf, mesg_size, &mesg_idx, md->mdhim_comm);
                return_code += MPI_Pack(bpm->keys[i], bpm->key_lens[i], MPI_CHAR, sendbuf, mesg_size, &mesg_idx, md->mdhim_comm);
                return_code += MPI_Pack(&bpm->value_lens[i], 1, MPI_INT, sendbuf, mesg_size, &mesg_idx, md->mdhim_comm);
                return_code += MPI_Pack(bpm->values[i], bpm->value_lens[i], MPI_CHAR, sendbuf, mesg_size, &mesg_idx, md->mdhim_comm);
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
 * @param message    in   pointer for packed message
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
int unpack_put_message(struct mdhim_t *md, void *message, int mesg_size, struct mdhim_putm_t *pm) {

	int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
    	int mesg_idx = 0;  // Variable for incremental unpack
        
        if ((pm = malloc(sizeof(struct mdhim_putm_t))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack put message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        
        // Unpack the message first with the structure and then followed by key and data values.
        return_code = MPI_Unpack(message, mesg_size, &mesg_idx, pm, sizeof(struct mdhim_putm_t), MPI_CHAR, md->mdhim_comm);
        
        // Unpack key by first allocating memory and then extracting the values from message
        if ((pm->key = (char *)malloc(pm->key_len * sizeof(char))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack put message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        return_code += MPI_Unpack(message, mesg_size, &mesg_idx, pm->key, pm->key_len, MPI_CHAR, md->mdhim_comm);
        
        // Unpack data by first allocating memory and then extracting the values from message
        if ((pm->value = (char *)malloc(pm->value_len * sizeof(char))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack put message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        return_code += MPI_Unpack(message, mesg_size, &mesg_idx, pm->value, pm->value_len, MPI_CHAR, md->mdhim_comm);

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
 * @param message    in   pointer for packed message
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
	int num_keys;
	int server_rank;
};
 */
int unpack_bput_message(struct mdhim_t *md, void *message, int mesg_size, struct mdhim_bputm_t *bpm) {

	int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
    	int mesg_idx = 0;  // Variable for incremental unpack
        int i;
        
        if ((bpm = malloc(sizeof(struct mdhim_bputm_t))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack bput message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        
        // Unpack the message first with the structure and then followed by key and data values.
        return_code = MPI_Unpack(message, mesg_size, &mesg_idx, bpm, sizeof(struct mdhim_bputm_t), MPI_CHAR, md->mdhim_comm);
        
        // Allocate memory for key_lens first, to be populated later.
        if ((bpm->key_lens = (int *)malloc(bpm->num_records * sizeof(int))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack bput message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        
        // Allocate memory for key_lens first, to be populated later.
        if ((bpm->value_lens = (int *)malloc(bpm->num_records * sizeof(int))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack bput message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        
        // For the each of the keys and data unpack the chars plus two ints for key_lens[i] and data_lens[i].
        for (i=0; i < bpm->num_records; i++) {
            // Unpack the key_lens[i]
            return_code += MPI_Unpack(message, mesg_size, &mesg_idx, &bpm->key_lens[i], 1, MPI_INT, md->mdhim_comm);
            
            // Unpack key by first allocating memory and then extracting the values from message
            if ((bpm->keys[i] = (char *)malloc(bpm->key_lens[i] * sizeof(char))) == NULL) {
                 mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                         "memory to unpack bput message.", md->mdhim_rank);
                 return MDHIM_ERROR; 
            }
            return_code += MPI_Unpack(message, mesg_size, &mesg_idx, bpm->keys[i], bpm->key_lens[i], MPI_CHAR, md->mdhim_comm);
            
            // Unpack the data_lens[i]
            return_code += MPI_Unpack(message, mesg_size, &mesg_idx, &bpm->value_lens[i], 1, MPI_INT, md->mdhim_comm);

            // Unpack data by first allocating memory and then extracting the values from message
            if ((bpm->values[i] = (char *)malloc(bpm->value_lens[i] * sizeof(char))) == NULL) {
                 mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                         "memory to unpack bput message.", md->mdhim_rank);
                 return MDHIM_ERROR; 
            }
            return_code += MPI_Unpack(message, mesg_size, &mesg_idx, bpm->values[i], bpm->value_lens[i], MPI_CHAR, md->mdhim_comm);
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
        
        // Add to size the length of the key and data fields
        m_size += gm->key_len;
        
        if (m_size > MDHIM_MAX_MSG_SIZE) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: get message too large."
                     " Get is over Maximum size allowed of %d.", md->mdhim_rank, MDHIM_MAX_MSG_SIZE);
             return MDHIM_ERROR; 
        }
        mesg_size = m_size;
        *sendsize = mesg_size;

        // Is the computed message size of a safe value? (less than a max message size?)
        if ((sendbuf = malloc(mesg_size * sizeof(char))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to pack get message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        
        // pack the message first with the structure and then followed by key and data values.
	return_code = MPI_Pack(gm, sizeof(struct mdhim_getm_t), MPI_CHAR, sendbuf, mesg_size, &mesg_idx, md->mdhim_comm);
        return_code += MPI_Pack(gm->key, gm->key_len, MPI_CHAR, sendbuf, mesg_size, &mesg_idx, md->mdhim_comm);

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
	int num_keys;
	int server_rank;
};
 */
int pack_bget_message(struct mdhim_t *md, struct mdhim_bgetm_t *bgm, void **sendbuf, int *sendsize) {
	int return_code = MPI_SUCCESS; // MPI_SUCCESS = 0
        int64_t m_size = sizeof(struct mdhim_bgetm_t);  // Generous variable for size calc
        int mesg_size;   // Variable to be used as parameter for MPI_pack of safe size
    	int mesg_idx = 0;
        int i;
        
        // For each of the key_lens
        // WARNING We are treating ints as the same size as char for packing purposes
        m_size += bgm->num_records;
        
        // For the each of the keys add enough chars.
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

        if ((sendbuf = malloc(mesg_size * sizeof(char))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to pack bulk get message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        
        // pack the message first with the structure and then followed by key and data values (plus lengths).
	return_code = MPI_Pack(bgm, sizeof(struct mdhim_bgetm_t), MPI_CHAR, sendbuf, mesg_size, &mesg_idx, md->mdhim_comm);
         
        // For the each of the keys and data pack the chars plus one int for key_len.
        for (i=0; i < bgm->num_records; i++) {
                return_code += MPI_Pack(&bgm->key_lens[i], 1, MPI_INT, sendbuf, mesg_size, &mesg_idx, md->mdhim_comm);
                return_code += MPI_Pack(bgm->keys[i], bgm->key_lens[i], MPI_CHAR, sendbuf, mesg_size, &mesg_idx, md->mdhim_comm);
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
 * @param message    in   pointer for packed message
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
int unpack_get_message(struct mdhim_t *md, void *message, int mesg_size, struct mdhim_getm_t *gm) {

	int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
    	int mesg_idx = 0;  // Variable for incremental unpack
        
        if ((gm = malloc(sizeof(struct mdhim_getm_t))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack get message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        
        // Unpack the message first with the structure and then followed by key and data values.
        return_code = MPI_Unpack(message, mesg_size, &mesg_idx, gm, sizeof(struct mdhim_getm_t), MPI_CHAR, md->mdhim_comm);
        
        // Unpack key by first allocating memory and then extracting the values from message
        if ((gm->key = (char *)malloc(gm->key_len * sizeof(char))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack get message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        return_code += MPI_Unpack(message, mesg_size, &mesg_idx, gm->key, gm->key_len, MPI_CHAR, md->mdhim_comm);

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
 * @param message    in   pointer for packed message
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
int unpack_bget_message(struct mdhim_t *md, void *message, int mesg_size, struct mdhim_bgetm_t *bgm) {

	int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
    	int mesg_idx = 0;  // Variable for incremental unpack
        int i;
        
        if ((bgm = malloc(sizeof(struct mdhim_bgetm_t))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack bget message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        
        // Unpack the message first with the structure and then followed by key and data values.
        return_code = MPI_Unpack(message, mesg_size, &mesg_idx, bgm, sizeof(struct mdhim_bgetm_t), MPI_CHAR, md->mdhim_comm);
        
        // Allocate memory for key_lens first, to be populated later.
        if ((bgm->key_lens = (int *)malloc(bgm->num_records * sizeof(int))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack bget message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        
        // For the each of the keys and data unpack the chars plus an int for key_lens[i].
        for (i=0; i < bgm->num_records; i++) {
            // Unpack the key_lens[i]
            return_code += MPI_Unpack(message, mesg_size, &mesg_idx, &bgm->key_lens[i], 1, MPI_INT, md->mdhim_comm);
            
            // Unpack key by first allocating memory and then extracting the values from message
            if ((bgm->keys[i] = (char *)malloc(bgm->key_lens[i] * sizeof(char))) == NULL) {
                 mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                         "memory to unpack bget message.", md->mdhim_rank);
                 return MDHIM_ERROR; 
            }
            return_code += MPI_Unpack(message, mesg_size, &mesg_idx, bgm->keys[i], bgm->key_lens[i], MPI_CHAR, md->mdhim_comm);
            
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
        
        // Add to size the length of the key and data fields
        m_size += grm->key_len + grm->value_len;
        
        if (m_size > MDHIM_MAX_MSG_SIZE) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: getrm message too large. Get return "
                     "message is over Maximum size allowed of %d.", md->mdhim_rank, MDHIM_MAX_MSG_SIZE);
             return MDHIM_ERROR; 
        }
        mesg_size = m_size;
        *sendsize = mesg_size;

        // Is the computed message size of a safe value? (less than a max message size?)
        if ((sendbuf = malloc(mesg_size * sizeof(char))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to pack get return message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        
        // pack the message first with the structure and then followed by key and data values.
	return_code = MPI_Pack(grm, sizeof(struct mdhim_getrm_t), MPI_CHAR, sendbuf, mesg_size, &mesg_idx, md->mdhim_comm);
        return_code += MPI_Pack(grm->key, grm->key_len, MPI_CHAR, sendbuf, mesg_size, &mesg_idx, md->mdhim_comm);
        return_code += MPI_Pack(grm->value, grm->value_len, MPI_CHAR, sendbuf, mesg_size, &mesg_idx, md->mdhim_comm);

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
        
        // For each of the lens (key_lens and data_lens)
        // WARNING We are treating ints as the same size as char for packing purposes
        m_size += 2 * bgrm->num_records;
        
        // For the each of the keys and data add enough chars.
        for (i=0; i < bgrm->num_records; i++)
                m_size += bgrm->key_lens[i] + bgrm->value_lens[i];
        
        // Is the computed message size of a safe value? (less than a max message size?)
        if (m_size > MDHIM_MAX_MSG_SIZE) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: bulk get return message too large."
                     " Bget return message is over Maximum size allowed of %d.", md->mdhim_rank, MDHIM_MAX_MSG_SIZE);
             return MDHIM_ERROR; 
        }
        mesg_size = m_size;  // Safe size to use in MPI_pack     
	*sendsize = mesg_size;

        if ((sendbuf = malloc(mesg_size * sizeof(char))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to pack bulk get return message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        
        // pack the message first with the structure and then followed by key and data values (plus lengths).
	return_code = MPI_Pack(bgrm, sizeof(struct mdhim_bgetrm_t), MPI_CHAR, sendbuf, mesg_size, &mesg_idx, md->mdhim_comm);
         
        // For the each of the keys and data pack the chars plus two ints for key_len and data_len.
        for (i=0; i < bgrm->num_records; i++) {
                return_code += MPI_Pack(&bgrm->key_lens[i], 1, MPI_INT, sendbuf, mesg_size, &mesg_idx, md->mdhim_comm);
                return_code += MPI_Pack(bgrm->keys[i], bgrm->key_lens[i], MPI_CHAR, sendbuf, mesg_size, &mesg_idx, md->mdhim_comm);
                return_code += MPI_Pack(&bgrm->value_lens[i], 1, MPI_INT, sendbuf, mesg_size, &mesg_idx, md->mdhim_comm);
                return_code += MPI_Pack(bgrm->values[i], bgrm->value_lens[i], MPI_CHAR, sendbuf, mesg_size, &mesg_idx, md->mdhim_comm);
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
 * @param message    in   pointer for packed message
 * @param mesg_size  in   size of the incoming message
 * @param bgrm       out  structure getrm_message which will be unpacked from the message 
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
int unpack_getrm_message(struct mdhim_t *md, void *message, int mesg_size, struct mdhim_getrm_t *grm) {

	int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
    	int mesg_idx = 0;  // Variable for incremental unpack
        
        if ((grm = malloc(sizeof(struct mdhim_getrm_t))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack put message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        
        // Unpack the message first with the structure and then followed by key and data values.
        return_code = MPI_Unpack(message, mesg_size, &mesg_idx, grm, sizeof(struct mdhim_getrm_t), MPI_CHAR, md->mdhim_comm);
        
        // Unpack key by first allocating memory and then extracting the values from message
        if ((grm->key = (char *)malloc(grm->key_len * sizeof(char))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack get return message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        return_code += MPI_Unpack(message, mesg_size, &mesg_idx, grm->key, grm->key_len, MPI_CHAR, md->mdhim_comm);
        
        // Unpack data by first allocating memory and then extracting the values from message
        if ((grm->value = (char *)malloc(grm->value_len * sizeof(char))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack get return message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        return_code += MPI_Unpack(message, mesg_size, &mesg_idx, grm->value, grm->value_len, MPI_CHAR, md->mdhim_comm);

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
int unpack_bgetrm_message(struct mdhim_t *md, void *message, int mesg_size, struct mdhim_bgetrm_t *bgrm) {

	int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
    	int mesg_idx = 0;  // Variable for incremental unpack
        int i;
        
        if ((bgrm = malloc(sizeof(struct mdhim_bgetrm_t))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack bget return message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        
        // Unpack the message first with the structure and then followed by key and data values.
        return_code = MPI_Unpack(message, mesg_size, &mesg_idx, bgrm, sizeof(struct mdhim_bgetrm_t), MPI_CHAR, md->mdhim_comm);
        
        // Allocate memory for key_lens first, to be populated later.
        if ((bgrm->key_lens = (int *)malloc(bgrm->num_records * sizeof(int))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack bget return message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        
        // Allocate memory for key_lens first, to be populated later.
        if ((bgrm->value_lens = (int *)malloc(bgrm->num_records * sizeof(int))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack bget return message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        
        // For the each of the keys and data unpack the chars plus two ints for key_lens[i] and data_lens[i].
        for (i=0; i < bgrm->num_records; i++) {
            // Unpack the key_lens[i]
            return_code += MPI_Unpack(message, mesg_size, &mesg_idx, &bgrm->key_lens[i], 1, MPI_INT, md->mdhim_comm);
            
            // Unpack key by first allocating memory and then extracting the values from message
            if ((bgrm->keys[i] = (char *)malloc(bgrm->key_lens[i] * sizeof(char))) == NULL) {
                 mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                         "memory to unpack bget return message.", md->mdhim_rank);
                 return MDHIM_ERROR; 
            }
            return_code += MPI_Unpack(message, mesg_size, &mesg_idx, bgrm->keys[i], bgrm->key_lens[i], MPI_CHAR, md->mdhim_comm);
            
            // Unpack the data_lens[i]
            return_code += MPI_Unpack(message, mesg_size, &mesg_idx, &bgrm->value_lens[i], 1, MPI_INT, md->mdhim_comm);

            // Unpack data by first allocating memory and then extracting the values from message
            if ((bgrm->values[i] = (char *)malloc(bgrm->value_lens[i] * sizeof(char))) == NULL) {
                 mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                         "memory to unpack bget return message.", md->mdhim_rank);
                 return MDHIM_ERROR; 
            }
            return_code += MPI_Unpack(message, mesg_size, &mesg_idx, bgrm->values[i], bgrm->value_lens[i], MPI_CHAR, md->mdhim_comm);
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

        if ((sendbuf = malloc(mesg_size * sizeof(char))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to pack del message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        
        // pack the message first with the structure and then followed by key and data values.
	return_code = MPI_Pack(dm, sizeof(struct mdhim_delm_t), MPI_CHAR, sendbuf, mesg_size, &mesg_idx, md->mdhim_comm);
        return_code += MPI_Pack(dm->key, dm->key_len, MPI_CHAR, sendbuf, mesg_size, &mesg_idx, md->mdhim_comm);

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
	int num_keys;
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
        
        // For each of the key_lens
        // WARNING We are treating ints as the same size as char for packing purposes
        m_size += bdm->num_keys;
        
        // For the each of the keys add enough chars.
        for (i=0; i < bdm->num_keys; i++)
                m_size += bdm->key_lens[i];
        
        // Is the computed message size of a safe value? (less than a max message size?)
        if (m_size > MDHIM_MAX_MSG_SIZE) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: bulk del message too large."
                     " Bdel is over Maximum size allowed of %d.", md->mdhim_rank, MDHIM_MAX_MSG_SIZE);
             return MDHIM_ERROR; 
        }
        mesg_size = m_size;  // Safe size to use in MPI_pack     
	*sendsize = mesg_size;

        if ((sendbuf = malloc(mesg_size * sizeof(char))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to pack bulk del message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        
        // pack the message first with the structure and then followed by key (plus lengths).
	return_code = MPI_Pack(bdm, sizeof(struct mdhim_bdelm_t), MPI_CHAR, sendbuf, mesg_size, &mesg_idx, md->mdhim_comm);
         
        // For the each of the keys and data pack the chars plus one int for key_len.
        for (i=0; i < bdm->num_keys; i++) {
                return_code += MPI_Pack(&bdm->key_lens[i], 1, MPI_INT, sendbuf, mesg_size, &mesg_idx, md->mdhim_comm);
                return_code += MPI_Pack(bdm->keys[i], bdm->key_lens[i], MPI_CHAR, sendbuf, mesg_size, &mesg_idx, md->mdhim_comm);
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
 * @param dm         out  structure get_message which will be unpacked from the message 
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 * 
 * struct mdhim_delm_t {
	int mtype;
	void *key;
	int key_len; 
	int server_rank;
};
 */
int unpack_del_message(struct mdhim_t *md, void *message, int mesg_size, struct mdhim_delm_t *dm) {

	int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
    	int mesg_idx = 0;  // Variable for incremental unpack
        
        if ((dm = malloc(sizeof(struct mdhim_delm_t))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack del message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        
        // Unpack the message first with the structure and then followed by key and data values.
        return_code = MPI_Unpack(message, mesg_size, &mesg_idx, dm, sizeof(struct mdhim_delm_t), MPI_CHAR, md->mdhim_comm);
        
        // Unpack key by first allocating memory and then extracting the values from message
        if ((dm->key = (char *)malloc(dm->key_len * sizeof(char))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack del message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        return_code += MPI_Unpack(message, mesg_size, &mesg_idx, dm->key, dm->key_len, MPI_CHAR, md->mdhim_comm);

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
 * @param bdm        out  structure bulk_del_message which will be unpacked from the message 
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 * 
 * struct mdhim_bdelm_t {
	int mtype;  
	void **keys;
	int *key_lens;
	int num_keys;
	int server_rank;
};
 */
int unpack_bdel_message(struct mdhim_t *md, void *message, int mesg_size, struct mdhim_bdelm_t *bdm) {

	int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
    	int mesg_idx = 0;  // Variable for incremental unpack
        int i;
        
        if ((bdm = malloc(sizeof(struct mdhim_bdelm_t))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack bdel message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        
        // Unpack the message first with the structure and then followed by key and data values.
        return_code = MPI_Unpack(message, mesg_size, &mesg_idx, bdm, sizeof(struct mdhim_bdelm_t), MPI_CHAR, md->mdhim_comm);
        
        // Allocate memory for key_lens first, to be populated later.
        if ((bdm->key_lens = (int *)malloc(bdm->num_keys * sizeof(int))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack bdel message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        
        // For the each of the keys and data unpack the chars plus an int for key_lens[i].
        for (i=0; i < bdm->num_keys; i++) {
            // Unpack the key_lens[i]
            return_code += MPI_Unpack(message, mesg_size, &mesg_idx, &bdm->key_lens[i], 1, MPI_INT, md->mdhim_comm);
            
            // Unpack key by first allocating memory and then extracting the values from message
            if ((bdm->keys[i] = (char *)malloc(bdm->key_lens[i] * sizeof(char))) == NULL) {
                 mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                         "memory to unpack bdel message.", md->mdhim_rank);
                 return MDHIM_ERROR; 
            }
            return_code += MPI_Unpack(message, mesg_size, &mesg_idx, bdm->keys[i], bdm->key_lens[i], MPI_CHAR, md->mdhim_comm);
            
        }

	// If the unpack did not succeed then log the error and return the error code
	if ( return_code != MPI_SUCCESS ) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to unpack "
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
        
	*sendsize = mesg_size;
        if ((sendbuf = malloc(mesg_size * sizeof(char))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to pack return message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        
        // Pack the message from the structure
	return_code = MPI_Pack(rm, sizeof(struct mdhim_rm_t), MPI_CHAR, sendbuf, mesg_size, &mesg_idx, md->mdhim_comm);

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
int unpack_return_message(struct mdhim_t *md, void *message, struct mdhim_rm_t *rm) {

	int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
        int mesg_size = sizeof(struct mdhim_rm_t); 
        int mesg_idx = 0;
        
        if ((rm = malloc(sizeof(struct mdhim_rm_t))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack return message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        
        // Unpack the structure from the message
        return_code = MPI_Unpack(message, mesg_size, &mesg_idx, rm, sizeof(struct mdhim_rm_t), MPI_CHAR, md->mdhim_comm);

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
