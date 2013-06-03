/*
 * MDHIM TNG
 * 
 * Client specific implementation
 */

#include "mdhim.h"
#include "range_server.h"
#include "hash.h"

/*
 * add_work
 * Adds work to the work queue and signals the condition variable for the worker thread
 *
 * @param work_item    pointer to new work item that contains a message to handle
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */

int range_server_add_work(work_item *item) {

}

/*
 * get_work
 * Returns the next work item from the work queue
 *
 * @return  the next work_item to process
 */

work_item *get_work() {
  
}

/*
 * range_server_stop
 * Stop the range server (i.e., stops the threads and frees the relevant data in md)
 *
 * @param md  Pointer to the main MDHIM structure
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_stop(mdhim_t *md) {
  //stop threads
  //free relevant pointers in md
}

/*
 * range_server_put
 * Handles the put message and puts data in the database
 *
 * @param im  pointer to the put message to handle
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_put(mdhim_putm_t *im) {
 
}

/*
 * range_server_bput
 * Handles the bulk put message and puts data in the database
 *
 * @param im  pointer to the put message to handle
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_bput(mdhim_bputm_t *bim) {
 
}

/*
 * range_server_delete
 * Handles the delete message and deletes the data from the database
 *
 * @param im  pointer to the delete message to handle
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_delete(mdhim_deletem_t *dm) {
 
}

/*
 * range_server_bdelete
 * Handles the bulk delete message and deletes the data from the database
 *
 * @param im  pointer to the delete message to handle
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_bdelete(mdhim_bdeletem_t *bdm) {
 
}

/*
 * range_server_get
 * Handles the get message, retrieves the data from the database, and sends the results back
 * 
 * @param im  pointer to the get message to handle
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_get(mdhim_getm_t *gm) {
 
}

/*
 * range_server_bget
 * Handles the bulk get message, retrieves the data from the database, and sends the results back
 * 
 * @param im  pointer to the bulk get message to handle
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_bget(mdhim_bgetm_t *bgm) {
 
}

/*
 * listener_thread
 * Function for the thread that listens for new messages
 */
void *listener_thread(void *) {
  //Receive messages sent to this server and add messages to work queue
}

/*
 * worker_thread
 * Function for the thread that processes work in work queue
 */
void *worker_thread(void *) {
  //Processes work queue - will use a condition variable to signal that more work is available
  //Calls getWork to retrieve the next item

}

/*
 * range_server_init
 * Initializes the range server (i.e., starts the threads and populates the relevant data in md)
 *
 * @param md  Pointer to the main MDHIM structure
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_init(mdhim_t *md) {
  //Start worker queue thread
  //Start listener for messages sent to the MDHIM communicator
}
