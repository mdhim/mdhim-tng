/*
 * MDHIM TNG
 * 
 * API implementation that calls functions in client.c and range_server.c
 */

#include "mdhim.h"
#include "range_server.h"
#include "client.h"
#include "hash.h"

/*
 * mdhimInit
 * Initializes MDHIM
 *
 * @param appComm   the communicator that was passed in from the application (e.g., MPI_COMM_WORLD)
 * @return mdhim_t* that contains info about this instance or NULL if there was an error
 */
mdhim_t *mdhimInit(MPI_Comm appComm) {
  mdhim_t *md = malloc(sizeof(mdhim_t));
  
  //Populate md
  //Start range server if I'm a range server
  //Scatter/Gather range server data (i.e., ranges, ranks)

  return md;
}

/*
 * Quits the MDHIM instance
 * @param md main MDHIM struct
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int mdhimClose(mdhim_t *md) {
  //Stop range server if I'm a range server
}

/*
 * Put into MDHIM
 *
 * @param md main MDHIM struct
 * @param pm pointer to put message to be sent or inserted into the range server's work queue
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int mdhimPut(mdhim_t *md, mdhim_putm_t *pm) {
  //If I'm a range server this is to be sent to, create a message_item and call add_work to add this 
  //Otherwise, call client_put
}

/*
 * Bulk put into MDHIM
 * 
 * @param md main MDHIM struct
 * @param bpm pointer to bulk put message to be sent or inserted into the range server's work queue
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int mdhimBput(mdhim_t *md, mdhim_bputm_t *bpm) {
  //If I'm a range server this is to be sent to, create a message_item and call add_work to add this 
  //Otherwise, call client_bput
}

/*
 * Get a record from MDHIM
 *
 * @param md main MDHIM struct
 * @param gm pointer to get message to be sent or inserted into the range server's work queue
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int mdhimGet(mdhim_t *md, mdhim_getm_t *gm) {
  //If I'm a range server this is to be sent to, create a message_item and call add_work to add this 
  //Otherwise, call client_get
}

/*
 * Bulk Get from MDHIM
 *
 * @param md main MDHIM struct
 * @param bgm pointer to get message to be sent or inserted into the range server's work queue
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int mdhimBGet(mdhim_t *md, mdhim_bgetm_t *bgm) {
  //If I'm a range server this is to be sent to, create a message_item and call add_work to add this 
  //Otherwise, call client_bget
}

/*
 * Delete from MDHIM
 *
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int mdhimDelete(mdhim_t *md, mdhim_deletem_t *dm) {
  //If I'm a range server this is to be sent to, create a message_item and call add_work to add this 
  //Otherwise, call client_delete
}

/*
 * Bulk delete from MDHIM
 *
 * @param md main MDHIM struct
 * @param bgm pointer to get message to be sent or inserted into the range server's work queue
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int mdhimBdelete(mdhim_t *md, mdhim_deletem_t *dm) {
  //If I'm a range server this is to be sent to, create a message_item and call add_work to add this 
  //Otherwise, call client_bdelete
}

