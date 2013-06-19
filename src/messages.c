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

	// If the send did not succeed then log the error and return the error code 
	if ( return_code != MPI_SUCCESS ) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d "
                     "send message failed.", md->mdhim_rank, return_code);
             return MDHIM_ERROR; 
        }

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

	MPI_Status status;
	int return_code;
        
        if ((message = malloc(MDHIM_MAX_MSG_SIZE)) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to receive a message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }

	// Receive a message from src (source) server
	return_code = MPI_Recv(message, MDHIM_MAX_MSG_SIZE, MPI_CHAR, src, RANGESRV_WORK, md->mdhim_comm, 
		 &status);

	// If the receive did not succeed then log the error and return the error code
	if ( return_code != MPI_SUCCESS ) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d "
                     "receive message failed.", md->mdhim_rank, return_code);
             return MDHIM_ERROR; 
        }

	return MDHIM_SUCCESS;
}

///------------------------

/*
 * pack_put_message
 * Packs a put message structure into contiguous memory for message passing
 *
 * @param md      in   main MDHIM struct
 * @param pm      in   structure put_message which will be packed into the message 
 * @param message out  pointer for packed message
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
int pack_put_message(struct mdhim_t *md, struct mdhim_putm_t *pm, void *message) {

	int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
        int64_t m_size = sizeof(struct mdhim_putm_t); // Generous variable for size calculation
        int mesg_size;  // Variable to be used as parameter for MPI_pack of safe size
    	int mesg_idx = 0;  // Variable for incremental pack
        
        // Add to size the length of the key and data fields
        m_size += pm->key_len + pm->data_len;
        
        if (m_size > MDHIM_MAX_MSG_SIZE) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: put message too large."
                     " Put is over Maximum size allowed of %d.", md->mdhim_rank, MDHIM_MAX_MSG_SIZE);
             return MDHIM_ERROR; 
        }
        mesg_size = m_size;
        
        // Is the computed message size of a safe value? (less than a max message size?)
        if ((message = malloc(mesg_size * sizeof(char))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to pack put message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        
        // pack the message first with the structure and then followed by key and data values.
	return_code = MPI_Pack(pm, sizeof(struct mdhim_putm_t), MPI_CHAR, message, mesg_size, &mesg_idx, md->mdhim_comm);
        return_code += MPI_Pack(pm->key, pm->key_len, MPI_CHAR, message, mesg_size, &mesg_idx, md->mdhim_comm);
        return_code += MPI_Pack(pm->data, pm->data_len, MPI_CHAR, message, mesg_size, &mesg_idx, md->mdhim_comm);

	// If the pack did not succeed then log the error and return the error code
	if ( return_code != MPI_SUCCESS ) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to pack "
                     "the put message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }

	return MDHIM_SUCCESS;
}

/*
 * pack_bput_message
 * Packs a bulk put message structure into contiguous memory for message passing
 *
 * @param md      in   main MDHIM struct
 * @param bpm     in   structure bput_message which will be packed into the message 
 * @param message out  pointer for packed message
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
int pack_bput_message(struct mdhim_t *md, struct mdhim_bputm_t *bpm, void *message) {

	int return_code = MPI_SUCCESS; // MPI_SUCCESS = 0
        int64_t m_size = sizeof(struct mdhim_bputm_t);  // Generous variable for size calc
        int mesg_size;   // Variable to be used as parameter for MPI_pack of safe size
    	int mesg_idx = 0;
        int i;
        
        // For each of the lens (key_lens and data_lens)
        // WARNING We are treating ints as the same size as char for packing purposes
        m_size += 2 * bpm->num_keys;
        
        // For the each of the keys and data add enough chars.
        for (i=0; i < bpm->num_keys; i++)
                m_size += bpm->key_lens[i] + bpm->data_lens[i];
        
        // Is the computed message size of a safe value? (less than a max message size?)
        if (m_size > MDHIM_MAX_MSG_SIZE) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: bulk put message too large."
                     " Bput is over Maximum size allowed of %d.", md->mdhim_rank, MDHIM_MAX_MSG_SIZE);
             return MDHIM_ERROR; 
        }
        mesg_size = m_size;  // Safe size to use in MPI_pack     
  
        if ((message = malloc(mesg_size * sizeof(char))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to pack bulk put message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        
        // pack the message first with the structure and then followed by key and data values (plus lengths).
	return_code = MPI_Pack(bpm, sizeof(struct mdhim_bputm_t), MPI_CHAR, message, mesg_size, &mesg_idx, md->mdhim_comm);
         
        // For the each of the keys and data pack the chars plus two ints for key_len and data_len.
        for (i=0; i < bpm->num_keys; i++) {
                return_code += MPI_Pack(bpm->key_lens[i], 1, MPI_INT, message, mesg_size, &mesg_idx, md->mdhim_comm);
                return_code += MPI_Pack(bpm->keys[i], bpm->key_lens[i], MPI_CHAR, message, mesg_size, &mesg_idx, md->mdhim_comm);
                return_code += MPI_Pack(bpm->data_lens[i], 1, MPI_INT, message, mesg_size, &mesg_idx, md->mdhim_comm);
                return_code += MPI_Pack(bpm->data[i], bpm->data_lens[i], MPI_CHAR, message, mesg_size, &mesg_idx, md->mdhim_comm);
        }

	// If the pack did not succeed then log the error and return the error code
	if ( return_code != MPI_SUCCESS ) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to pack "
                     "the bulk put message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }

	return MDHIM_SUCCESS;
}

/*
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
        if ((pm->data = (char *)malloc(pm->data_len * sizeof(char))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack put message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        return_code += MPI_Unpack(message, mesg_size, &mesg_idx, pm->data, pm->data_len, MPI_CHAR, md->mdhim_comm);

	// If the unpack did not succeed then log the error and return the error code
	if ( return_code != MPI_SUCCESS ) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to unpack "
                     "the put message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }

	return MDHIM_SUCCESS;
}

/*
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
        if ((bpm->key_lens = (int *)malloc(bpm->num_keys * sizeof(int))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack bput message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        
        // Allocate memory for key_lens first, to be populated later.
        if ((bpm->data_lens = (int *)malloc(bpm->num_keys * sizeof(int))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack bput message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        
        // For the each of the keys and data unpack the chars plus two ints for key_lens[i] and data_lens[i].
        for (i=0; i < bpm->num_keys; i++) {
            // Unpack the key_lens[i]
            return_code += MPI_Unpack(message, mesg_size, &mesg_idx, bpm->key_lens[i], 1, MPI_INT, md->mdhim_comm);
            
            // Unpack key by first allocating memory and then extracting the values from message
            if ((bpm->keys[i] = (char *)malloc(bpm->key_lens[i] * sizeof(char))) == NULL) {
                 mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                         "memory to unpack bput message.", md->mdhim_rank);
                 return MDHIM_ERROR; 
            }
            return_code += MPI_Unpack(message, mesg_size, &mesg_idx, bpm->keys[i], bpm->key_lens[i], MPI_CHAR, md->mdhim_comm);
            
            // Unpack the data_lens[i]
            return_code += MPI_Unpack(message, mesg_size, &mesg_idx, bpm->data_lens[i], 1, MPI_INT, md->mdhim_comm);

            // Unpack data by first allocating memory and then extracting the values from message
            if ((bpm->data[i] = (char *)malloc(bpm->data_lens[i] * sizeof(char))) == NULL) {
                 mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                         "memory to unpack bput message.", md->mdhim_rank);
                 return MDHIM_ERROR; 
            }
            return_code += MPI_Unpack(message, mesg_size, &mesg_idx, bpm->data[i], bpm->data_lens[i], MPI_CHAR, md->mdhim_comm);
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

/*
 * pack_get_message
 * Packs a get message structure into contiguous memory for message passing
 *
 * @param md      in   main MDHIM struct
 * @param gm      in   structure get_message which will be packed into the message 
 * @param message out  pointer for packed message
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
int pack_get_message(struct mdhim_t *md, struct mdhim_getm_t *gm, void *message) {

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
        
        // Is the computed message size of a safe value? (less than a max message size?)
        if ((message = malloc(mesg_size * sizeof(char))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to pack get message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        
        // pack the message first with the structure and then followed by key and data values.
	return_code = MPI_Pack(gm, sizeof(struct mdhim_getm_t), MPI_CHAR, message, mesg_size, &mesg_idx, md->mdhim_comm);
        return_code += MPI_Pack(gm->key, gm->key_len, MPI_CHAR, message, mesg_size, &mesg_idx, md->mdhim_comm);

	// If the pack did not succeed then log the error and return the error code
	if ( return_code != MPI_SUCCESS ) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to pack "
                     "the get message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }

	return MDHIM_SUCCESS;
}

/*
 * pack_bget_message
 * Packs a bget message structure into contiguous memory for message passing
 *
 * @param md      in   main MDHIM struct
 * @param bgm     in   structure bget_message which will be packed into the message 
 * @param message out  pointer for packed message
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
int pack_bget_message(struct mdhim_t *md, struct mdhim_bgetm_t *bgm, void *message) {

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
  
        if ((message = malloc(mesg_size * sizeof(char))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to pack bulk get message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        
        // pack the message first with the structure and then followed by key and data values (plus lengths).
	return_code = MPI_Pack(bgm, sizeof(struct mdhim_bgetm_t), MPI_CHAR, message, mesg_size, &mesg_idx, md->mdhim_comm);
         
        // For the each of the keys and data pack the chars plus one int for key_len.
        for (i=0; i < bgm->num_records; i++) {
                return_code += MPI_Pack(bgm->key_lens[i], 1, MPI_INT, message, mesg_size, &mesg_idx, md->mdhim_comm);
                return_code += MPI_Pack(bgm->keys[i], bgm->key_lens[i], MPI_CHAR, message, mesg_size, &mesg_idx, md->mdhim_comm);
        }

	// If the pack did not succeed then log the error and return the error code
	if ( return_code != MPI_SUCCESS ) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to pack "
                     "the bulk get message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }

	return MDHIM_SUCCESS;
}

/*
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

/*
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
            return_code += MPI_Unpack(message, mesg_size, &mesg_idx, bgm->key_lens[i], 1, MPI_INT, md->mdhim_comm);
            
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

/*
 * pack_getrm_message
 * Packs a get return message structure into contiguous memory for message passing
 *
 * @param md      in   main MDHIM struct
 * @param grm     in   structure get_return_message which will be packed into the message 
 * @param message out  pointer for packed message
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
int pack_getrm_message(struct mdhim_t *md, struct mdhim_getrm_t *grm, void *message) {

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
        
        // Is the computed message size of a safe value? (less than a max message size?)
        if ((message = malloc(mesg_size * sizeof(char))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to pack get return message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        
        // pack the message first with the structure and then followed by key and data values.
	return_code = MPI_Pack(grm, sizeof(struct mdhim_getrm_t), MPI_CHAR, message, mesg_size, &mesg_idx, md->mdhim_comm);
        return_code += MPI_Pack(grm->key, grm->key_len, MPI_CHAR, message, mesg_size, &mesg_idx, md->mdhim_comm);
        return_code += MPI_Pack(grm->value, grm->value_len, MPI_CHAR, message, mesg_size, &mesg_idx, md->mdhim_comm);

	// If the pack did not succeed then log the error and return the error code
	if ( return_code != MPI_SUCCESS ) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to pack "
                     "the get return message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }

	return MDHIM_SUCCESS;
}

/*
 * pack_bgetrm_message
 * Packs a bulk get return message structure into contiguous memory for message passing
 *
 * @param md      in   main MDHIM struct
 * @param bgrm    in   structure bget_return_message which will be packed into the message 
 * @param message out  pointer for packed message
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
int pack_bgetrm_message(struct mdhim_t *md, struct mdhim_bgetrm_t *bgrm, void *message) {

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
  
        if ((message = malloc(mesg_size * sizeof(char))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to pack bulk get return message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        
        // pack the message first with the structure and then followed by key and data values (plus lengths).
	return_code = MPI_Pack(bgrm, sizeof(struct mdhim_bgetrm_t), MPI_CHAR, message, mesg_size, &mesg_idx, md->mdhim_comm);
         
        // For the each of the keys and data pack the chars plus two ints for key_len and data_len.
        for (i=0; i < bgrm->num_records; i++) {
                return_code += MPI_Pack(bgrm->key_lens[i], 1, MPI_INT, message, mesg_size, &mesg_idx, md->mdhim_comm);
                return_code += MPI_Pack(bgrm->keys[i], bgrm->key_lens[i], MPI_CHAR, message, mesg_size, &mesg_idx, md->mdhim_comm);
                return_code += MPI_Pack(bgrm->value_lens[i], 1, MPI_INT, message, mesg_size, &mesg_idx, md->mdhim_comm);
                return_code += MPI_Pack(bgrm->values[i], bgrm->value_lens[i], MPI_CHAR, message, mesg_size, &mesg_idx, md->mdhim_comm);
        }

	// If the pack did not succeed then log the error and return the error code
	if ( return_code != MPI_SUCCESS ) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to pack "
                     "the bulk get return message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }

	return MDHIM_SUCCESS;
}

/*
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

/*
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
            return_code += MPI_Unpack(message, mesg_size, &mesg_idx, bgrm->key_lens[i], 1, MPI_INT, md->mdhim_comm);
            
            // Unpack key by first allocating memory and then extracting the values from message
            if ((bgrm->keys[i] = (char *)malloc(bgrm->key_lens[i] * sizeof(char))) == NULL) {
                 mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                         "memory to unpack bget return message.", md->mdhim_rank);
                 return MDHIM_ERROR; 
            }
            return_code += MPI_Unpack(message, mesg_size, &mesg_idx, bgrm->keys[i], bgrm->key_lens[i], MPI_CHAR, md->mdhim_comm);
            
            // Unpack the data_lens[i]
            return_code += MPI_Unpack(message, mesg_size, &mesg_idx, bgrm->value_lens[i], 1, MPI_INT, md->mdhim_comm);

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

/*
 * pack_del_message
 * Packs a delete message structure into contiguous memory for message passing
 *
 * @param md      in   main MDHIM struct
 * @param dm      in   structure del_message which will be packed into the message 
 * @param message out  pointer for packed message
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 * 
 * struct mdhim_delm_t {
	int mtype;
	void *key;
	int key_len; 
	int server_rank;
};
 */
int pack_del_message(struct mdhim_t *md, struct mdhim_delm_t *dm, void *message) {

	int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
        int64_t m_size = sizeof(struct mdhim_delm_t); // Generous variable for size calculation
        int mesg_size;  // Variable to be used as parameter for MPI_pack of safe size
    	int mesg_idx = 0;  // Variable for incremental pack
        
        // Add to size the length of the key and data fields
        m_size += dm->key_len;
        
        if (m_size > MDHIM_MAX_MSG_SIZE) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: del message too large."
                     " Del is over Maximum size allowed of %d.", md->mdhim_rank, MDHIM_MAX_MSG_SIZE);
             return MDHIM_ERROR; 
        }
        mesg_size = m_size;
        
        // Is the computed message size of a safe value? (less than a max message size?)
        if ((message = malloc(mesg_size * sizeof(char))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to pack del message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        
        // pack the message first with the structure and then followed by key and data values.
	return_code = MPI_Pack(dm, sizeof(struct mdhim_delm_t), MPI_CHAR, message, mesg_size, &mesg_idx, md->mdhim_comm);
        return_code += MPI_Pack(dm->key, dm->key_len, MPI_CHAR, message, mesg_size, &mesg_idx, md->mdhim_comm);

	// If the pack did not succeed then log the error and return the error code
	if ( return_code != MPI_SUCCESS ) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to pack "
                     "the del message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }

	return MDHIM_SUCCESS;
}

/*
 * pack_bdel_message
 * Packs a bdel message structure into contiguous memory for message passing
 *
 * @param md      in   main MDHIM struct
 * @param bgm     in   structure bdel_message which will be packed into the message 
 * @param message out  pointer for packed message
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
int pack_bdel_message(struct mdhim_t *md, struct mdhim_bdelm_t *bdm, void *message) {

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
  
        if ((message = malloc(mesg_size * sizeof(char))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to pack bulk del message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        
        // pack the message first with the structure and then followed by key (plus lengths).
	return_code = MPI_Pack(bdm, sizeof(struct mdhim_bdelm_t), MPI_CHAR, message, mesg_size, &mesg_idx, md->mdhim_comm);
         
        // For the each of the keys and data pack the chars plus one int for key_len.
        for (i=0; i < bdm->num_keys; i++) {
                return_code += MPI_Pack(bdm->key_lens[i], 1, MPI_INT, message, mesg_size, &mesg_idx, md->mdhim_comm);
                return_code += MPI_Pack(bdm->keys[i], bdm->key_lens[i], MPI_CHAR, message, mesg_size, &mesg_idx, md->mdhim_comm);
        }

	// If the pack did not succeed then log the error and return the error code
	if ( return_code != MPI_SUCCESS ) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to pack "
                     "the bulk del message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }

	return MDHIM_SUCCESS;
}

/*
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
        return_code = MPI_Unpack(message, mesg_size, &mesg_idx, gm, sizeof(struct mdhim_delm_t), MPI_CHAR, md->mdhim_comm);
        
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

/*
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
        return_code = MPI_Unpack(message, mesg_size, &mesg_idx, bgm, sizeof(struct mdhim_bdelm_t), MPI_CHAR, md->mdhim_comm);
        
        // Allocate memory for key_lens first, to be populated later.
        if ((bdm->key_lens = (int *)malloc(bdm->num_keys * sizeof(int))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to unpack bdel message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        
        // For the each of the keys and data unpack the chars plus an int for key_lens[i].
        for (i=0; i < bdm->num_keys; i++) {
            // Unpack the key_lens[i]
            return_code += MPI_Unpack(message, mesg_size, &mesg_idx, bdm->key_lens[i], 1, MPI_INT, md->mdhim_comm);
            
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


/*
 * pack_return_message
 * Packs a return message structure into contiguous memory for message passing
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
int pack_return_message(struct mdhim_t *md, struct mdhim_rm_t *rm, void *message) {

	int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
        int mesg_size = sizeof(struct mdhim_rm_t); 
        int mesg_idx = 0;
        
        if ((message = malloc(mesg_size * sizeof(char))) == NULL) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to allocate "
                     "memory to pack return message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }
        
        // Pack the message from the structure
	return_code = MPI_Pack(rm, sizeof(struct mdhim_rm_t), MPI_CHAR, message, mesg_size, &mesg_idx, md->mdhim_comm);

	// If the pack did not succeed then log the error and return the error code
	if ( return_code != MPI_SUCCESS ) {
             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: unable to pack "
                     "the return message.", md->mdhim_rank);
             return MDHIM_ERROR; 
        }

	return MDHIM_SUCCESS;
}

/*
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

