/*
 * MDHIM TNG
 * 
 * Client specific implementation for sending to range server that is not yourself
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
	
	rm = malloc(sizeof(struct mdhim_rm_t));
	rm->error = MDHIM_SUCCESS ;

	return_code = send_message(md, pm->server_rank, pm);
	// If the send did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d from server while sending "
	       "put record request",  md->mdhim_rank, return_code);
	  rm->error = MDHIM_ERROR;
	}

	return_code = receive_message(md, pm->server_rank, (void *) rm);
	// If the receive did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d from server while receiving "
	       "put record request",  md->mdhim_rank, return_code);
	  rm->error = MDHIM_ERROR;
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
	
	rm = malloc(sizeof(struct mdhim_rm_t));
	rm->error = MDHIM_SUCCESS ;

	return_code = send_message(md, bpm->server_rank, bpm);
	// If the send did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d from server while sending "
	       "bput record request",  md->mdhim_rank, return_code);
	  rm->error = MDHIM_ERROR;
	}

	return_code = receive_message(md, bpm->server_rank, (void *) rm);
	// If the receive did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d from server while receiving "
	       "bput record request",  md->mdhim_rank, return_code);
	  rm->error = MDHIM_ERROR;
	}

	// Return any error code if an error is encountered
	return rm;
}

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
	char *buf;

	rm = malloc(sizeof(struct mdhim_rm_t));
	rm->error = MDHIM_SUCCESS ;

	return_code = send_message(md, gm->server_rank, gm);
	// If the send did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d from server while sending "
	       "get record request",  md->mdhim_rank, return_code);
	  rm->error = MDHIM_ERROR;
	}

	return_code = receive_message(md, gm->server_rank, buf);
	// If the receive did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d from server while receiving "
	       "get record request",  md->mdhim_rank, return_code);
	  rm->error = MDHIM_ERROR;
	}

	// Return any error code if an error is encountered
	return rm;
}

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
	       "bget record request",  md->mdhim_rank, return_code);
	  rm->error = MDHIM_ERROR;
	}

	return_code = receive_message(md, bgm->server_rank, buf);
	// If the receive did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d from server while receiving "
	       "bget record request",  md->mdhim_rank, return_code);
	  rm->error = MDHIM_ERROR;
	}

	// Return any error code if an error is encountered
	return rm;
}

/*
 * Send delete to range server
 *
 * @param md main MDHIM struct
 * @param bgm pointer to get message to be sent or inserted into the range server's work queue
 * @return return_message structure with ->error = MDHIM_SUCCESS or MDHIM_ERROR
 */
struct mdhim_rm_t *client_delete(struct mdhim_t *md, struct mdhim_delm_t *dm) {

	int return_code;
	struct mdhim_rm_t *rm;
	
	rm = malloc(sizeof(struct mdhim_rm_t));
	rm->error = MDHIM_SUCCESS ;

	return_code = send_message(md, dm->server_rank, dm);
	// If the send did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d from server while sending "
	       "delete record request",  md->mdhim_rank, return_code);
	  rm->error = MDHIM_ERROR;
	}

	return_code = receive_message(md, dm->server_rank, (void *) rm);
	// If the receive did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d from server while receiving "
	       "delete record request",  md->mdhim_rank, return_code);
	  rm->error = MDHIM_ERROR;
	}

	// Return any error code if an error is encountered
	return rm;
}

/*
 * Send bulk delete to MDHIM
 *
 * @param md main MDHIM struct
 * @param bgm pointer to get message to be sent or inserted into the range server's work queue
 * @return return_message structure with ->error = MDHIM_SUCCESS or MDHIM_ERROR
 */
struct mdhim_rm_t *client_bdelete(struct mdhim_t *md, struct mdhim_bdelm_t *bdm) {

	int return_code;
	struct mdhim_rm_t *rm;
	
	rm = malloc(sizeof(struct mdhim_rm_t));
	rm->error = MDHIM_SUCCESS ;

	return_code = send_message(md, bdm->server_rank, bdm);
	// If the send did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d from server while sending "
	       "bdelete record request",  md->mdhim_rank, return_code);
	  rm->error = MDHIM_ERROR;
	}

	return_code = receive_message(md, bdm->server_rank, (void *) rm);
	// If the receive did not succeed then log the error code and return MDHIM_ERROR
	if (return_code != MDHIM_SUCCESS) {
	  mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d from server while receiving "
	       "bdelete record request",  md->mdhim_rank, return_code);
	  rm->error = MDHIM_ERROR;
	}

	// Return any error code if an error is encountered
	return rm;
}
