/*
 * MDHIM TNG
 * 
 * Client specific implementation for sending to range server that is not yourself
 */

#include <stdlib.h>
#include "mdhim.h"
#include "client.h"
#include "partitioner.h"

/**
 * Send put to range server
 *
 * @param md main MDHIM struct
 * @param pm pointer to put message to be sent or inserted into the range server's work queue
 * @return return_message structure with ->error = MDHIM_SUCCESS or MDHIM_ERROR
 */
struct mdhim_rm_t *client_put(struct mdhim_t *md, struct mdhim_putm_t *pm) {

	int return_code;
	struct mdhim_rm_t *rm;
        void *message;
	
	rm = malloc(sizeof(struct mdhim_rm_t));
	rm->error = MDHIM_SUCCESS;
	rm->server_rank = pm->server_rank;
	rm->mtype = MDHIM_RECV;

        // Pack pm ==> message and then send message
        return_code = pack_put_message(md, pm, message);
        // If the pack did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d while packing "
	       "put message",  md->mdhim_rank, return_code);
	  rm->error = MDHIM_ERROR;
          return rm;
	}
        
	return_code = send_message(md, pm->server_rank, message);
	// If the send did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d from server while sending "
	       "put message request",  md->mdhim_rank, return_code);
	  rm->error = MDHIM_ERROR;
          return rm;
	}

	return_code = receive_message(md, pm->server_rank, message);
	// If the receive did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d from server while receiving "
	       "put message request",  md->mdhim_rank, return_code);
	  rm->error = MDHIM_ERROR;
          return rm;
	}
        
        // Unpack rm from the received message
        return_code = unpack_return_message(md, message, rm);
        // If the pack did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d while unpacking "
	       "return message",  md->mdhim_rank, return_code);
	  rm->error = MDHIM_ERROR;
          return rm;
	}

	// Return response message
	return rm;
}

/**
 * Send bulk put to range server
 * 
 * @param md main MDHIM struct
 * @param bpm pointer to bulk put message to be sent or inserted into the range server's work queue
 * @return return_message structure with ->error = MDHIM_SUCCESS or MDHIM_ERROR
*/
struct mdhim_brm_t *client_bput(struct mdhim_t *md, struct mdhim_bputm_t *bpm) {

	int return_code;
        void *message;
        struct mdhim_brm_t *brm;
	
	brm = malloc(sizeof(struct mdhim_brm_t));
	brm->error = MDHIM_SUCCESS ;
	brm->server_rank = bpm->server_rank;
	brm->mtype = MDHIM_RECV_BULK;
      
        // Pack bpm ==> message and then send message
        return_code = pack_bput_message(md, bpm, message);
        // If the pack did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d while packing "
	       "bput message",  md->mdhim_rank, return_code);
	  brm->error = MDHIM_ERROR;
          return brm;
	}
        
	return_code = send_message(md, bpm->server_rank, message);
	// If the send did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d from server while sending "
	       "bput message request",  md->mdhim_rank, return_code);
	  brm->error = MDHIM_ERROR;
          return brm;
	}

	return_code = receive_message(md, bpm->server_rank, message);
	// If the receive did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d from server while receiving "
	       "bput message request",  md->mdhim_rank, return_code);
	  brm->error = MDHIM_ERROR;
          return brm;
	}
        
        // Unpack brm from the received message
        return_code = unpack_return_message(md, message, brm);
        // If the pack did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d while unpacking "
	       "return message",  md->mdhim_rank, return_code);
	  brm->error = MDHIM_ERROR;
          return brm;
	}

	// Return response message
	return brm;
}


//TODO//
/**
 * Send get to range server
 *
 * @param md main MDHIM struct
 * @param gm pointer to get message to be sent or inserted into the range server's work queue
 * @return return_message structure with ->error = MDHIM_SUCCESS or MDHIM_ERROR
*/
struct mdhim_getrm_t *client_get(struct mdhim_t *md, struct mdhim_getm_t *gm) {

	int return_code;
	struct mdhim_getrm_t *rm;
	char *message;

	rm = malloc(sizeof(struct mdhim_getrm_t));
	rm->error = MDHIM_SUCCESS;
	rm->server_rank = gm->server_rank;
	rm->mtype = MDHIM_RECV;
        
        // Pack gm ==> message and then send message
        return_code = pack_get_message(md, gm->server_rank, message);
        // If the pack did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d from server while packing "
	       "get message request",  md->mdhim_rank, return_code);
	  rm->error = MDHIM_ERROR;
	}

	return_code = receive_message(md, gm->server_rank, message);
	// If the receive did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d from server while receiving "
	       "get message request",  md->mdhim_rank, return_code);
	  rm->error = MDHIM_ERROR;
	}
        
        // Unpack getrm ==> message
        return_code = unpack_getrm_message(md, message, MDHIM_MAX_MSG_SIZE, gm);
        // If the pack did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d while unpacking "
	       "get message",  md->mdhim_rank, return_code);
	  rm->error = MDHIM_ERROR;
	}

	// Return response message
	return rm;
}


//TODO//

/**
 * Send bulk get to range server
 *
 * @param md main MDHIM struct
 * @param bgm pointer to get message to be sent or inserted into the range server's work queue
 * @return return_message structure with ->error = MDHIM_SUCCESS or MDHIM_ERROR
 */
struct mdhim_bgetrm_t *client_bget(struct mdhim_t *md, struct mdhim_bgetm_t *bgm) {

	int return_code;
	struct mdhim_bgetrm_t *bgrm;

	bgrm = malloc(sizeof(struct mdhim_bgetrm_t));
	bgrm->error = MDHIM_SUCCESS;
	bgrm->server_rank = bgm->server_rank;
	bgrm->mtype = MDHIM_RECV_BULK_GET;

	return_code = send_message(md, bgm->server_rank, bgm);
	// If the send did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d from server while sending "
	       "bget record request",  md->mdhim_rank, return_code);
	  bgrm->error = MDHIM_ERROR;
	}

	return_code = receive_message(md, bgm->server_rank, bgrm);
	// If the receive did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d from server while receiving "
	       "bget record request",  md->mdhim_rank, return_code);
	  bgrm->error = MDHIM_ERROR;
	}

	// Return response message
	return bgrm;
}

/**
 * Send delete to range server
 *
 * @param md main MDHIM struct
 * @param dm pointer to del message to be sent or inserted into the range server's work queue
 * @return return_message structure with ->error = MDHIM_SUCCESS or MDHIM_ERROR
 */
struct mdhim_rm_t *client_delete(struct mdhim_t *md, struct mdhim_delm_t *dm) {

	int return_code;
	struct mdhim_rm_t *rm;
        void *message;
	
	rm = malloc(sizeof(struct mdhim_rm_t));
        rm->error = MDHIM_SUCCESS;
	rm->server_rank = dm->server_rank;
	rm->mtype = MDHIM_RECV;
        
        // Pack dm ==> message and then send message
        return_code = pack_del_message(md, dm->server_rank, message);
        // If the pack did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d while packing "
	       "del message request",  md->mdhim_rank, return_code);
	  rm->error = MDHIM_ERROR;
	}

	return_code = receive_message(md, dm->server_rank, message);
	// If the receive did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d from server while receiving "
	       "del message request",  md->mdhim_rank, return_code);
	  rm->error = MDHIM_ERROR;
          return rm;
	}
        
        // Unpack rm from the received message
        return_code = unpack_return_message(md, message, rm);
        // If the pack did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d while unpacking "
	       "return message",  md->mdhim_rank, return_code);
	  rm->error = MDHIM_ERROR;
          return rm;
	}

	// Return any error code if an error is encountered
	return rm;
}

/**
 * Send bulk delete to range server
 *
 * @param md main MDHIM struct
 * @param bdm pointer to bulk del message to be sent or inserted into the range server's work queue
 * @return return_message structure with ->error = MDHIM_SUCCESS or MDHIM_ERROR
 */
struct mdhim_brm_t *client_bdelete(struct mdhim_t *md, struct mdhim_bdelm_t *bdm) {

	int return_code;
        void *message;
	struct mdhim_brm_t *brm;
	
	brm = malloc(sizeof(struct mdhim_brm_t));
	brm->error = MDHIM_SUCCESS;
	brm->server_rank = bdm->server_rank;
	brm->mtype =  MDHIM_RECV_BULK;
        
        // Pack bdm ==> message and then send message
        return_code = pack_bdel_message(md, bdm, message);
        // If the pack did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d while packing "
	       "bdel message",  md->mdhim_rank, return_code);
	  brm->error = MDHIM_ERROR;
	}

	return_code = send_message(md, bdm->server_rank, message);
	// If the send did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d from server while sending "
	       "bdel record request",  md->mdhim_rank, return_code);
	  brm->error = MDHIM_ERROR;
          return brm;
	}

	return_code = receive_message(md, bdm->server_rank, message);
	// If the receive did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d from server while receiving "
	       "bdel record request",  md->mdhim_rank, return_code);
	  brm->error = MDHIM_ERROR;
          return brm;
	}
        
        // Unpack rm from the received message
        return_code = unpack_return_message(md, message, brm);
        // If the pack did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d while unpacking "
	       "return message",  md->mdhim_rank, return_code);
	  brm->error = MDHIM_ERROR;
          return brm;
	}

	// Return any error code if an error is encountered
	return brm;
}
