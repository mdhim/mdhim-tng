/*
 * MDHIM TNG
 * 
 * Client specific implementation
 */

#include "mdhim.h"
#include "client.h"
#include "hash.h"

/*
 * Send put to range server
 *
 * @param md main MDHIM struct
 * @param pm pointer to put message to be sent or inserted into the range server's work queue
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int client_put(struct mdhim_t *md, struct mdhim_putm_t *pm) {
}

/*
 * Send bulk put to range server
 * 
 * @param md main MDHIM struct
 * @param bpm pointer to bulk put message to be sent or inserted into the range server's work queue
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int client_bput(struct mdhim_t *md, struct mdhim_bputm_t *bpm) {

}

/*
 * Send get to range server
 *
 * @param md main MDHIM struct
 * @param gm pointer to get message to be sent or inserted into the range server's work queue
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int client_get(struct mdhim_t *md, struct mdhim_getm_t *gm) {

}

/*
 * Send bulk get to range server
 *
 * @param md main MDHIM struct
 * @param bgm pointer to get message to be sent or inserted into the range server's work queue
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int client_bget(struct mdhim_t *md, struct mdhim_bgetm_t *bgm) {
}

/*
 * Send delete to range server
 *
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int client_delete(struct mdhim_t *md, struct mdhim_delm_t *dm) {

}

/*
 * Send bulk delete to MDHIM
 *
 * @param md main MDHIM struct
 * @param bgm pointer to get message to be sent or inserted into the range server's work queue
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int client_bdelete(struct mdhim_t *md, struct mdhim_bdelm_t *dm) {

}

/*
 * add range server
 * Adds a range server to the md->rangesrvs list 
 * @param md main MDHIM struct
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int client_add_rangesrv(struct mdhim_t *md) {

}

