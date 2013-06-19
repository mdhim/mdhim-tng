/*
 * MDHIM TNG
 * 
 * Client specific implementation
 */

#include <stdlib.h>
#include "mdhim.h"
#include "client.h"
#include "partitioner.h"

/*
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
	rm->error = MDHIM_SUCCESS ;

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

	// Return any error code if an error is encountered
	return rm;
}

/*
 * Send bulk put to range server
 * 
 * @param md main MDHIM struct
 * @param bpm pointer to bulk put message to be sent or inserted into the range server's work queue
 * @return return_message structure with ->error = MDHIM_SUCCESS or MDHIM_ERROR
*/
struct mdhim_rm_t *client_bput(struct mdhim_t *md, struct mdhim_bputm_t *bpm) {

	int return_code;
	struct mdhim_rm_t *rm;
        void *message;
	
	rm = malloc(sizeof(struct mdhim_rm_t));
	rm->error = MDHIM_SUCCESS ;
        
        // Pack bpm ==> message and then send message
        return_code = pack_bput_message(md, bpm, message);
        // If the pack did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d while packing "
	       "bput message",  md->mdhim_rank, return_code);
	  rm->error = MDHIM_ERROR;
          return rm;
	}
        
	return_code = send_message(md, bpm->server_rank, message);
	// If the send did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d from server while sending "
	       "bput message request",  md->mdhim_rank, return_code);
	  rm->error = MDHIM_ERROR;
          return rm;
	}

	return_code = receive_message(md, bpm->server_rank, message);
	// If the receive did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d from server while receiving "
	       "bput message request",  md->mdhim_rank, return_code);
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


//TODO//
/*
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

	rm = malloc(sizeof(struct mdhim_rm_t));
	rm->error = MDHIM_SUCCESS ;
        
        // Pack gm ==> message and then send message
        return_code = pack_get_message(md, gm, message);
        // If the pack did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d while packing "
	       "get message",  md->mdhim_rank, return_code);
	  rm->error = MDHIM_ERROR;
	}

	return_code = send_message(md, gm->server_rank, message);
	// If the send did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d from server while sending "
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

	// Return any error code if an error is encountered
	return rm;
}

//TODO//

/*
 * Send bulk get to range server
 *
 * @param md main MDHIM struct
 * @param bgm pointer to get message to be sent or inserted into the range server's work queue
 * @return return_message structure with ->error = MDHIM_SUCCESS or MDHIM_ERROR
 */
struct mdhim_bgetrm_t *client_bget(struct mdhim_t *md, struct mdhim_bgetm_t *bgm) {

	int return_code;
	struct mdhim_bgetrm_t *rm;
	char *buf;

	rm = malloc(sizeof(struct mdhim_rm_t));
	rm->error = MDHIM_SUCCESS ;

	return_code = send_message(md, bgm->server_rank, bgm);
	// If the send did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d from server while sending "
	       "bget message request",  md->mdhim_rank, return_code);
	  rm->error = MDHIM_ERROR;
	}

	return_code = receive_message(md, bgm->server_rank, buf);
	// If the receive did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d from server while receiving "
	       "bget message request",  md->mdhim_rank, return_code);
	  rm->error = MDHIM_ERROR;
	}

	// Return any error code if an error is encountered
	return rm;
}

/*
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
	rm->error = MDHIM_SUCCESS ;
        
        // Pack dm ==> message and then send message
        return_code = pack_del_message(md, dm, message);
        // If the pack did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d while packing "
	       "del message",  md->mdhim_rank, return_code);
	  rm->error = MDHIM_ERROR;
	}

	return_code = send_message(md, dm->server_rank, message);
	// If the send did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d from server while sending "
	       "del message request",  md->mdhim_rank, return_code);
	  rm->error = MDHIM_ERROR;
          return rm;
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

/*
 * Send bulk delete to MDHIM
 *
 * @param md main MDHIM struct
 * @param bdm pointer to bulk del message to be sent or inserted into the range server's work queue
 * @return return_message structure with ->error = MDHIM_SUCCESS or MDHIM_ERROR
 */
struct mdhim_rm_t *client_bdelete(struct mdhim_t *md, struct mdhim_bdelm_t *bdm) {

	int return_code;
	struct mdhim_rm_t *rm;
        void *message;
	
	rm = malloc(sizeof(struct mdhim_rm_t));
	rm->error = MDHIM_SUCCESS ;
        
        // Pack bdm ==> message and then send message
        return_code = pack_bdel_message(md, bdm, message);
        // If the pack did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d while packing "
	       "bdel message",  md->mdhim_rank, return_code);
	  rm->error = MDHIM_ERROR;
	}

	return_code = send_message(md, bdm->server_rank, message);
	// If the send did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d from server while sending "
	       "bdel message request",  md->mdhim_rank, return_code);
	  rm->error = MDHIM_ERROR;
          return rm;
	}

	return_code = receive_message(md, bdm->server_rank, message);
	// If the receive did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d from server while receiving "
	       "bdel message request",  md->mdhim_rank, return_code);
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

/*
 * add range server
 * Adds a range server to the md->rangesrvs list 
 * @param md main MDHIM struct
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int client_add_rangesrv(struct mdhim_t *md) {
  return MDHIM_SUCCESS;
}

