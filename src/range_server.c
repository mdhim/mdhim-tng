/*
 * MDHIM TNG
 * 
 * Client specific implementation
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "mdhim.h"
#include "range_server.h"
#include "partitioner.h"

/*
 * add_work
 * Adds work to the work queue and signals the condition variable for the worker thread
 *
 * @param work_item    pointer to new work item that contains a message to handle
 * @return MDHIM_SUCCESS
 */

int range_server_add_work(struct mdhim_t *md, work_item *item) {
	item->next = NULL;
	item->prev = NULL;
	
	//Lock the work queue mutex
	pthread_mutex_lock(md->mdhim_rs->work_queue_mutex);
	//Add work to the work queue
	if (md->mdhim_rs->work_queue->tail) {
		md->mdhim_rs->work_queue->tail->next = item;
		item->prev = md->mdhim_rs->work_queue->tail;
		md->mdhim_rs->work_queue->tail = item;
	} else if (!md->mdhim_rs->work_queue->head) {
		md->mdhim_rs->work_queue->head = item;
		md->mdhim_rs->work_queue->tail = item;
	}

	//Signal the waiting thread that there is work available
	pthread_cond_signal(md->mdhim_rs->work_ready_cv);
	pthread_mutex_unlock(md->mdhim_rs->work_queue_mutex);

	return MDHIM_SUCCESS;
}

/*
 * get_work
 * Returns the next work item from the work queue
 *
 * @return  the next work_item to process
 */

work_item *get_work(struct mdhim_rs_t *mdhim_rs) {
	work_item *item;

	item = mdhim_rs->work_queue->tail;
	if (!item) {
		return NULL;
	}

	//Remove the item from the tail and set the pointers accordingly
	if (item->prev) {
		item->prev->next = NULL;
		mdhim_rs->work_queue->tail = item->prev;
	} else if (!item->prev) {
		mdhim_rs->work_queue->tail = NULL;
		mdhim_rs->work_queue->head = NULL;
	}

	item->next = NULL;
	item->prev = NULL;
	return item;
}

/*
 * range_server_stop
 * Stop the range server (i.e., stops the threads and frees the relevant data in md)
 *
 * @param md  Pointer to the main MDHIM structure
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_stop(struct mdhim_t *md) {
	work_item *head, *temp_item;
	int ret;

	//Cancel the threads
	if ((ret = pthread_cancel(md->mdhim_rs->listener)) != 0) {
		return MDHIM_ERROR;
	}
	if ((ret = pthread_cancel(md->mdhim_rs->worker)) != 0) {
		return MDHIM_ERROR;
	}
	//Destroy the mutex
	if ((ret = pthread_mutex_destroy(md->mdhim_rs->work_queue_mutex)) != 0) {
		return MDHIM_ERROR;
	}
	//Destroy the condition variable
	if ((ret = pthread_cond_destroy(md->mdhim_rs->work_ready_cv)) != 0) {
		return MDHIM_ERROR;
	}

	//Free the work queue
	head = md->mdhim_rs->work_queue->head;
	while (head) {
		temp_item = head->next;
		free(head);
		head = temp_item;
	}

	//Close the database
	if ((ret = mdhim_db_close( md->mdhim_rs->mdhim_store, NULL)) != MDHIM_SUCCESS) {
		return MDHIM_ERROR;
	}

	//Free the range server information
	free(md->mdhim_rs);
	md->mdhim_rs = NULL;
	
	return MDHIM_SUCCESS;
}

/*
 * range_server_put
 * Handles the put message and puts data in the database
 *
 * @param im  pointer to the put message to handle
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_put(struct mdhim_putm_t *im) {
 
}

/*
 * range_server_bput
 * Handles the bulk put message and puts data in the database
 *
 * @param im  pointer to the put message to handle
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_bput(struct mdhim_bputm_t *bim) {
 
}

/*
 * range_server_del
 * Handles the delete message and deletes the data from the database
 *
 * @param im  pointer to the delete message to handle
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_del(struct mdhim_delm_t *dm) {
 
}

/*
 * range_server_bdel
 * Handles the bulk delete message and deletes the data from the database
 *
 * @param im  pointer to the delete message to handle
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_bdel(struct mdhim_bdelm_t *bdm) {
 
}

/*
 * range_server_get
 * Handles the get message, retrieves the data from the database, and sends the results back
 * 
 * @param im  pointer to the get message to handle
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_get(struct mdhim_getm_t *gm) {
 
}

/*
 * range_server_bget
 * Handles the bulk get message, retrieves the data from the database, and sends the results back
 * 
 * @param im  pointer to the bulk get message to handle
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_bget(struct mdhim_bgetm_t *bgm) {
 
}

/*
 * listener_thread
 * Function for the thread that listens for new messages
 */
void *listener_thread(void *data) {	
	struct mdhim_t *md = (struct mdhim_t *) data;
	char *buf, *message_buf;
	int max_size = 1048576; //At most receive 1MB
	MPI_Status status;
	int source; //The source of the message
	int mtype; //The message type
	work_item *item;

	buf = malloc(sizeof(max_size));
	while (1) {
		memset(buf, 0, max_size);
		
		//Receive messages sent to this server
		MPI_Recv(buf, max_size, MPI_CHAR, MPI_ANY_SOURCE, RANGESRV_WORK, md->mdhim_comm, 
			 &status);
		
		//Get the source of the message
		source = status.MPI_SOURCE;
		
                //Get the message type
		mtype = ((struct mdhim_basem_t *) buf)->mtype;
		mlog(MPI_DBG, "Rank: %d - Received message from : %d of type: %d", 
		     md->mdhim_rank, source, mtype);
		
                //Create a new work item
		item = malloc(sizeof(work_item));
		memset(item, 0, sizeof(work_item));
		
                //Copy the received buffer
		message_buf = malloc(max_size);
		memcpy(message_buf, buf, max_size);
		
		//Set the new buffer to the new item's message
		item->message = message_buf;
		//Add the new item to the work queue
		range_server_add_work(md, item);
	}
}

/*
 * worker_thread
 * Function for the thread that processes work in work queue
 */
void *worker_thread(void *data) {
	struct mdhim_rs_t *mdhim_rs = (struct mdhim_rs_t *) data;
	work_item *item;
	int mtype;

	while (1) {
		//Lock the work queue mutex
		pthread_mutex_lock(mdhim_rs->work_queue_mutex);
		//Wait until there is work to be performed
		pthread_cond_wait(mdhim_rs->work_ready_cv, mdhim_rs->work_queue_mutex);
		//while there is work, get it
		while ((item = get_work(mdhim_rs)) != NULL) {
			//Call the appropriate function depending on the message type

			//Get the message type
			mtype = ((struct mdhim_basem_t *) item->message)->mtype;
			switch(mtype) {
			case MDHIM_PUT:
				range_server_put((struct mdhim_putm_t *) item->message);
				break;
			case MDHIM_BULK_PUT:
				range_server_bput((struct mdhim_bputm_t *) item->message);
				break;
			case MDHIM_GET:
				range_server_get((struct mdhim_getm_t *) item->message);
				break;
			case MDHIM_BULK_GET:
				range_server_bget((struct mdhim_bgetm_t *) item->message);
				break;
			case MDHIM_DEL:
				range_server_del((struct mdhim_delm_t *) item->message);
				break;
			case MDHIM_BULK_DEL:
				range_server_bdel((struct mdhim_bdelm_t *) item->message);
				break;
			default:
				break;
			}
			
			free(item);
		}

		pthread_mutex_unlock(mdhim_rs->work_queue_mutex);
	}
}

/*
 * range_server_init
 * Initializes the range server (i.e., starts the threads and populates the relevant data in md)
 *
 * @param md  Pointer to the main MDHIM structure
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_init(struct mdhim_t *md) {
	int ret;
	char filename[255];

	//Allocate memory for the mdhim_rs_t struct
	md->mdhim_rs = malloc(sizeof(struct mdhim_rs_t));
	if (!md->mdhim_rs) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - Error while allocating memory for range server", md->mdhim_rank);
		return MDHIM_ERROR;
	}
  
	//Populate md->mdhim_rs
	if ((ret = populate_my_ranges(md)) == MDHIM_ERROR) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - Error populating my ranges", md->mdhim_rank);
		return MDHIM_ERROR;
	}

	//Initialize data store
	//Filename is dependent on ranges
	sprintf(filename, "%s", "test");
	md->mdhim_rs->mdhim_store = mdhim_db_open(filename, UNQLITE, NULL);
	if (!md->mdhim_rs->mdhim_store) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - Error while allocating memory for range server", md->mdhim_rank);
		return MDHIM_ERROR;
	}
	//Initialize work queue to null
	md->mdhim_rs->work_queue = NULL;
	//Initialize work queue mutex
	md->mdhim_rs->work_queue_mutex = malloc(sizeof(pthread_mutex_t));
	if (!md->mdhim_rs->work_queue_mutex) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - Error while allocating memory for range server", md->mdhim_rank);
		return MDHIM_ERROR;
	}
	if ((ret = pthread_mutex_init(md->mdhim_rs->work_queue_mutex, NULL)) != 0) {    
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - Error while initializing work queue mutex", md->mdhim_rank);
		return MDHIM_ERROR;
	}

	//Initialize the condition variable
	md->mdhim_rs->work_ready_cv = malloc(sizeof(pthread_cond_t));
	if (!md->mdhim_rs->work_ready_cv) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - Error while allocating memory for range server", md->mdhim_rank);
		return MDHIM_ERROR;
	}
	if ((ret = pthread_cond_init(md->mdhim_rs->work_ready_cv, NULL)) != 0) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - Error while initializing condition variable", md->mdhim_rank);
		return MDHIM_ERROR;
	}

	//Initialize worker thread
	if ((ret = pthread_create(&md->mdhim_rs->worker, NULL, worker_thread, (void *) md->mdhim_rs)) != 0) {    
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - Error while initializing worker thread", md->mdhim_rank);
		return MDHIM_ERROR;
	}
	//Initialize listener thread
	if ((ret = pthread_create(&md->mdhim_rs->listener, NULL, listener_thread, (void *) md)) != 0) {    
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - Error while initializing listener thread", md->mdhim_rank);
		return MDHIM_ERROR;
	}

	return MDHIM_SUCCESS;
}

