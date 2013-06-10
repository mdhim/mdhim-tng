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

}

/*
 * receive_message
 * Receives a message from the given source
 *
 * @param md      main MDHIM struct
 * @param src     source to receive from 
 * @return        message pointer to message received
 */
int *receive_message(struct mdhim_t *md, int src, void *message) {

}
