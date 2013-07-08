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

//Global cursor list - one for each rank
struct mdhim_cursor *mdhim_cursors = NULL;

/**
 * is_range_server
 * checks if I'm a range server
 *
 * @param md  Pointer to the main MDHIM structure
 * @return 0 if false, 1 if true
 */

int im_range_server(struct mdhim_t *md) {
	if (md->mdhim_rs) {
		return 1;
	}
	
	return 0;
}

void *get_cursor(struct mdhim_t *md, int source) {
	struct mdhim_cursor *mc;
	void *cursor;

	HASH_FIND_INT(mdhim_cursors, &source, mc);
	if (mc) {
		return mc->cursor;
	}

	cursor = md->mdhim_rs->mdhim_store->cursor_init(md->mdhim_rs->mdhim_store->db_handle);
	if (!cursor) {
		mlog(MDHIM_SERVER_CRIT, "Rank: %d - Error initializing cursor", 
		     md->mdhim_rank);
		return NULL;
	}

	//Create a new mdhim_char to hold our entry
	mc = malloc(sizeof(struct mdhim_cursor));

	//Set the mdhim_char
	mc->id = source;
	mc->cursor = cursor;

	//Add it to the hash table
	HASH_ADD_INT(mdhim_cursors, id, mc);    
	return cursor;
}

/**
 * send_locally_or_remote
 * Sends the message remotely or locally
 *
 * @param md       Pointer to the main MDHIM structure
 * @param dest     Destination rank
 * @param message  pointer to message to send
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int send_locally_or_remote(struct mdhim_t *md, int dest, void *message) {
	int ret = MDHIM_SUCCESS;


	mlog(MDHIM_SERVER_DBG, "Rank: %d - Sending response to: %d", 
	     md->mdhim_rank, dest);

	if (md->mdhim_rank != dest) {
		//Sends the message remotely
		mlog(MDHIM_SERVER_DBG, "Rank: %d - Sending remote response to: %d", 
		     md->mdhim_rank, dest);
		ret = send_client_response(md, dest, message);
		mdhim_full_release_msg(message);
	} else {
		//Sends the message locally
		mlog(MDHIM_SERVER_DBG, "Rank: %d - Sending local response to: %d", 
		     md->mdhim_rank, dest);
		pthread_mutex_lock(md->receive_msg_mutex);
		md->receive_msg = message;
		pthread_cond_signal(md->receive_msg_ready_cv);
		pthread_mutex_unlock(md->receive_msg_mutex);
	}

	return ret;
}

/**
 * range_server_add_work
 * Adds work to the work queue and signals the condition variable for the worker thread
 *
 * @param md  Pointer to the main MDHIM structure
 * @param work_item    pointer to new work item that contains a message to handle
 * @return MDHIM_SUCCESS
 */
int range_server_add_work(struct mdhim_t *md, work_item *item) {
	//Lock the work queue mutex
	pthread_mutex_lock(md->mdhim_rs->work_queue_mutex);
	item->next = NULL;
	item->prev = NULL;       
	mlog(MDHIM_SERVER_DBG, "Rank: %d - Adding work to queue", 
			     md->mdhim_rank);

	//Add work to the head of the work queue
	if (md->mdhim_rs->work_queue->head) {
		md->mdhim_rs->work_queue->head->prev = item;
		item->next = md->mdhim_rs->work_queue->head;
		md->mdhim_rs->work_queue->head = item;
	} else {
		md->mdhim_rs->work_queue->head = item;
		md->mdhim_rs->work_queue->tail = item;
	}

	//Signal the waiting thread that there is work available
	pthread_cond_signal(md->mdhim_rs->work_ready_cv);
	pthread_mutex_unlock(md->mdhim_rs->work_queue_mutex);

	return MDHIM_SUCCESS;
}

/**
 * get_work
 * Returns the next work item from the work queue
 *
 * @param mdhim_rs  Pointer to the range server info
 * @return  the next work_item to process
 */

work_item *get_work(struct mdhim_t *md) {
	work_item *item;

	item = md->mdhim_rs->work_queue->tail;
	if (!item) {
		return NULL;
	}

	//Remove the item from the tail and set the pointers accordingly
	if (item->prev) {
		item->prev->next = NULL;
		md->mdhim_rs->work_queue->tail = item->prev;
	} else if (!item->prev) {
		md->mdhim_rs->work_queue->tail = NULL;
		md->mdhim_rs->work_queue->head = NULL;
	}

	item->next = NULL;
	item->prev = NULL;
	return item;
}

/**
 * range_server_stop
 * Stop the range server (i.e., stops the threads and frees the relevant data in md)
 *
 * @param md  Pointer to the main MDHIM structure
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_stop(struct mdhim_t *md) {
	work_item *head, *temp_item;
	int ret;	
	struct mdhim_cursor *cur_cursor, *tmp;

	//Cancel the worker thread
	if ((ret = pthread_cancel(md->mdhim_rs->worker)) != 0) {
		mlog(MDHIM_SERVER_DBG, "Rank: %d - Error canceling worker thread", 
		     md->mdhim_rank);
	}

	/* Wait for the threads to finish */
	pthread_join(md->mdhim_rs->listener, NULL);
	pthread_join(md->mdhim_rs->worker, NULL);

	//Destroy the condition variable
	if ((ret = pthread_cond_destroy(md->mdhim_rs->work_ready_cv)) != 0) {
		mlog(MDHIM_SERVER_DBG, "Rank: %d - Error destroying work cond variable", 
		     md->mdhim_rank);
	}
	free(md->mdhim_rs->work_ready_cv);

	//Destroy the mutex
	if ((ret = pthread_mutex_destroy(md->mdhim_rs->work_queue_mutex)) != 0) {
		mlog(MDHIM_SERVER_DBG, "Rank: %d - Error destroying work queue mutex", 
		     md->mdhim_rank);
	}
	free(md->mdhim_rs->work_queue_mutex);

	//Free the work queue
	head = md->mdhim_rs->work_queue->head;
	while (head) {
		temp_item = head->next;
		free(head);
		head = temp_item;
	}
	free(md->mdhim_rs->work_queue);

	//Close all the open cursors and free the hash table
	HASH_ITER(hh, mdhim_cursors, cur_cursor, tmp) {
		if ((ret = md->mdhim_rs->mdhim_store->cursor_release(
			     md->mdhim_rs->mdhim_store->db_handle, cur_cursor->cursor)) 
		    != MDHIM_SUCCESS) {
			mlog(MDHIM_SERVER_CRIT, "Rank: %d - Error releasing cursor", 
			     md->mdhim_rank);
		}
		HASH_DEL(mdhim_cursors, cur_cursor);  /*delete it (mdhim_cursors advances to next)*/
		free(cur_cursor);            /* free it */
	}

	//Close the database
	if ((ret = md->mdhim_rs->mdhim_store->close(md->mdhim_rs->mdhim_store->db_handle, NULL)) 
	    != MDHIM_SUCCESS) {
		mlog(MDHIM_SERVER_CRIT, "Rank: %d - Error closing database", 
		     md->mdhim_rank);
	}

	//Free the range server information
	free(md->mdhim_rs);
	md->mdhim_rs = NULL;
	
	return MDHIM_SUCCESS;
}

/**
 * range_server_put
 * Handles the put message and puts data in the database
 *
 * @param md        pointer to the main MDHIM struct
 * @param im        pointer to the put message to handle
 * @param source    source of the message
 * @return          MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_put(struct mdhim_t *md, struct mdhim_putm_t *im, int source) {
	int ret;
	struct mdhim_rm_t *rm;

        //Put the record in the database
	if ((ret = 
	     md->mdhim_rs->mdhim_store->put(md->mdhim_rs->mdhim_store->db_handle, 
					im->key, im->key_len, im->value, 
					im->value_len, NULL)) != MDHIM_SUCCESS) {
		mlog(MDHIM_SERVER_CRIT, "Rank: %d - Error putting record", 
		     md->mdhim_rank);	
	}

	//Create the response message
	rm = malloc(sizeof(struct mdhim_rm_t));
	//Set the type
	rm->mtype = MDHIM_RECV;
	//Set the operation return code as the error
	rm->error = ret;
	//Set the server's rank
	rm->server_rank = md->mdhim_rank;

	mlog(MDHIM_SERVER_DBG, "Rank: %d - Sending response for put request", 
	     md->mdhim_rank);
	//Send response
	ret = send_locally_or_remote(md, source, rm);

	return MDHIM_SUCCESS;
}

/**
 * range_server_bput
 * Handles the bulk put message and puts data in the database
 *
 * @param md        Pointer to the main MDHIM struct
 * @param im        pointer to the put message to handle
 * @param source    source of the message
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_bput(struct mdhim_t *md, struct mdhim_bputm_t *bim, int source) {
	int i;
	int ret;
	int error = 0;
	struct mdhim_rm_t *brm;

	//Iterate through the arrays and insert each record
	for (i = 0; i < bim->num_records && i < MAX_BULK_OPS; i++) {	
		//Put the record in the database
		if ((ret = 
		     md->mdhim_rs->mdhim_store->put(md->mdhim_rs->mdhim_store->db_handle, 
						bim->keys[i], bim->key_lens[i], bim->values[i], 
						bim->value_lens[i], NULL)) != MDHIM_SUCCESS) {
			mlog(MDHIM_SERVER_CRIT, "Rank: %d - Error putting record", 
			     md->mdhim_rank);
			error = ret;
		}
	}

	//Create the response message
	brm = malloc(sizeof(struct mdhim_rm_t));
	//Set the type
	brm->mtype = MDHIM_RECV;
	//Set the operation return code as the error
	brm->error = error;
	//Set the server's rank
	brm->server_rank = md->mdhim_rank;

	//Release the bputm message, but don't free the keys and values
	mdhim_partial_release_msg(bim);

	//Send response
	ret = send_locally_or_remote(md, source, brm);

	return MDHIM_SUCCESS;
}

/**
 * range_server_del
 * Handles the delete message and deletes the data from the database
 *
 * @param md       Pointer to the main MDHIM struct
 * @param im       pointer to the delete message to handle
 * @param source   source of the message
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_del(struct mdhim_t *md, struct mdhim_delm_t *dm, int source) {
	int ret;
	struct mdhim_rm_t *rm;

	//Put the record in the database
	if ((ret = 
	     md->mdhim_rs->mdhim_store->del(md->mdhim_rs->mdhim_store->db_handle, 
					dm->key, dm->key_len, NULL)) != MDHIM_SUCCESS) {
		mlog(MDHIM_SERVER_CRIT, "Rank: %d - Error deleting record", 
		     md->mdhim_rank);
	}

	//Create the response message
	rm = malloc(sizeof(struct mdhim_rm_t));
	//Set the type
	rm->mtype = MDHIM_RECV;
	//Set the operation return code as the error
	rm->error = ret;
	//Set the server's rank
	rm->server_rank = md->mdhim_rank;

	//Send response
	ret = send_locally_or_remote(md, source, rm);

	return MDHIM_SUCCESS;
}

/**
 * range_server_bdel
 * Handles the bulk delete message and deletes the data from the database
 *
 * @param md        Pointer to the main MDHIM struct
 * @param im        pointer to the delete message to handle
 * @param source    source of the message
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_bdel(struct mdhim_t *md, struct mdhim_bdelm_t *bdm, int source) {
 	int i;
	int ret;
	int error;
	struct mdhim_rm_t *brm;

	//Iterate through the arrays and delete each record
	for (i = 0; i < bdm->num_keys && i < MAX_BULK_OPS; i++) {
		//Put the record in the database
		if ((ret = 
		     md->mdhim_rs->mdhim_store->del(md->mdhim_rs->mdhim_store->db_handle, 
						bdm->keys[i], bdm->key_lens[i],
						NULL)) != MDHIM_SUCCESS) {
			mlog(MDHIM_SERVER_CRIT, "Rank: %d - Error deleting record", 
			     md->mdhim_rank);
			error = ret;
		}
	}

	//Create the response message
	brm = malloc(sizeof(struct mdhim_rm_t));
	//Set the type
	brm->mtype = MDHIM_RECV;
	//Set the operation return code as the error
	brm->error = error;
	//Set the server's rank
	brm->server_rank = md->mdhim_rank;

	//Send response
	ret = send_locally_or_remote(md, source, brm);

	return MDHIM_SUCCESS;
}

/**
 * range_server_get
 * Handles the get message, retrieves the data from the database, and sends the results back
 * 
 * @param md        Pointer to the main MDHIM struct
 * @param im        pointer to the get message to handle
 * @param source    source of the message
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_get(struct mdhim_t *md, struct mdhim_getm_t *gm, int source) {
	int error;
	void **value;
	int32_t value_len;
	struct mdhim_getrm_t *grm;
	int ret;

	value = malloc(sizeof(void *));
	*value = NULL;
	value_len = 0;
	//Get a record from the database
	if ((error = 
	     md->mdhim_rs->mdhim_store->get(md->mdhim_rs->mdhim_store->db_handle, 
					    gm->key, gm->key_len, value, 
					    &value_len, NULL)) != MDHIM_SUCCESS) {
		mlog(MDHIM_SERVER_DBG, "Rank: %d - Couldn't get a record", 
		     md->mdhim_rank);
	}

	if (*((char **) value)) {
		mlog(MDHIM_SERVER_DBG, "Rank: %d - Retrieved value: %d with length: %d for rank: %d", 
		     md->mdhim_rank, **((int **) value), value_len, source);
	}

	//Create the response message
	grm = malloc(sizeof(struct mdhim_getrm_t));
	//Set the type
	grm->mtype = MDHIM_RECV_GET;
	//Set the operation return code as the error
	grm->error = error;
	//Set the server's rank
	grm->server_rank = md->mdhim_rank;
	//Set the key and value
	grm->key = gm->key;
	grm->key_len = gm->key_len;
	grm->value = (void *) *(((char **) value));
	grm->value_len = value_len;
	//Send response
	mlog(MDHIM_SERVER_DBG, "Rank: %d - About to send get response to: %d", 
	     md->mdhim_rank, source);
	ret = send_locally_or_remote(md, source, grm);

	return MDHIM_SUCCESS;
}

/**
 * range_server_bget
 * Handles the bulk get message, retrieves the data from the database, and sends the results back
 * 
 * @param md        Pointer to the main MDHIM struct
 * @param im        pointer to the bulk get message to handle
 * @param source    source of the message
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_bget(struct mdhim_t *md, struct mdhim_bgetm_t *bgm, int source) {
	int ret;
	void **values;
	int32_t *value_lens;
	int i;
	struct mdhim_bgetrm_t *bgrm;
	int error = 0;
	
	values = malloc(sizeof(void *) * MAX_BULK_OPS);
	memset(values, 0, sizeof(void *) * MAX_BULK_OPS);
	value_lens = malloc(sizeof(int) * MAX_BULK_OPS);
	memset(value_lens, 0, sizeof(int) * MAX_BULK_OPS);
	//Iterate through the arrays and delete each record
	for (i = 0; i < bgm->num_records && i < MAX_BULK_OPS; i++) {
		//Get records from the database
		if ((ret = 
		     md->mdhim_rs->mdhim_store->get(md->mdhim_rs->mdhim_store->db_handle, 
						    bgm->keys[i], bgm->key_lens[i], (void **) (values + i), 
						    (int32_t *) (value_lens + i), NULL)) != MDHIM_SUCCESS) {
			mlog(MDHIM_SERVER_CRIT, "Rank: %d - Error getting record", 
			     md->mdhim_rank);
			error = ret;
			value_lens[i] = 0;
			continue;
		}

		if (*((char *) values[i])) {
		  mlog(MDHIM_SERVER_DBG, "Rank: %d - Retrieved value: %d with length: %d for rank: %d", 
		       md->mdhim_rank, *((int **) values)[i], value_lens[i], source);
		}
	}

	//Create the response message
	bgrm = malloc(sizeof(struct mdhim_bgetrm_t));
	//Set the type
	bgrm->mtype = MDHIM_RECV_BULK_GET;
	//Set the operation return code as the error
	bgrm->error = error;
	//Set the server's rank
	bgrm->server_rank = md->mdhim_rank;
	//Set the key and value
	bgrm->keys = bgm->keys;
	bgrm->key_lens = bgm->key_lens;
	bgrm->values = values;
	bgrm->value_lens = value_lens;
	bgrm->num_records = bgm->num_records;
	//Send response
	ret = send_locally_or_remote(md, source, bgrm);

	return MDHIM_SUCCESS;
}

/*
 * listener_thread
 * Function for the thread that listens for new messages
 */
void *listener_thread(void *data) {	
	struct mdhim_t *md = (struct mdhim_t *) data;
	void *message;
	int source; //The source of the message
	int mtype; //The message type
	int ret;
	work_item *item;

	pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);
	pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);

	while (1) {		
		//Receive messages sent to this server
		ret = receive_rangesrv_work(md, &source, &message);
		if (ret < MDHIM_SUCCESS) {
			mlog(MDHIM_SERVER_CRIT, "Rank: %d - Error receiving message in listener", 
			     md->mdhim_rank);
			continue;
		}
	
		//We received a close message - so quit
		if (ret == MDHIM_CLOSE) {
			mlog(MDHIM_SERVER_CRIT, "Rank: %d - Received close message", 
			     md->mdhim_rank);
			break;
		}

                //Get the message type
		mtype = ((struct mdhim_basem_t *) message)->mtype;
		mlog(MPI_DBG, "Rank: %d - Received message from rank: %d of type: %d", 
		     md->mdhim_rank, source, mtype);
		
                //Create a new work item
		item = malloc(sizeof(work_item));
		memset(item, 0, sizeof(work_item));
		             
		//Set the new buffer to the new item's message
		item->message = message;
		//Set the source in the work item
		item->source = source;
		//Add the new item to the work queue
		range_server_add_work(md, item);
	}

	return NULL;
}

/*
 * worker_thread
 * Function for the thread that processes work in work queue
 */
void *worker_thread(void *data) {
	struct mdhim_t *md = (struct mdhim_t *) data;
	work_item *item;
	int mtype;
	int op;

	pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);
	pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
	while (1) {
		//Lock the work queue mutex
		pthread_mutex_lock(md->mdhim_rs->work_queue_mutex);
		pthread_cleanup_push((void (*)(void *)) pthread_mutex_unlock,
				     (void *) md->mdhim_rs->work_queue_mutex);
		//Wait until there is work to be performed
		if ((item = get_work(md)) == NULL) {
			pthread_cond_wait(md->mdhim_rs->work_ready_cv, md->mdhim_rs->work_queue_mutex);
			item = get_work(md);
		}
	       
		pthread_cleanup_pop(0);
		if (!item) {
			mlog(MDHIM_SERVER_DBG, "Rank: %d - Got empty work item from queue", 
			     md->mdhim_rank);
			pthread_mutex_unlock(md->mdhim_rs->work_queue_mutex);
			continue;
		}

		while (item) {
			//Call the appropriate function depending on the message type			
			//Get the message type
			mtype = ((struct mdhim_basem_t *) item->message)->mtype;
			mlog(MDHIM_SERVER_DBG, "Rank: %d - Got work item from queue with type: %d" 
			     " from: %d", md->mdhim_rank, mtype, item->source);
			switch(mtype) {
			case MDHIM_PUT:
				//Pack the put message and pass to range_server_put
				range_server_put(md, 
						 item->message, 
						 item->source);
				break;
			case MDHIM_BULK_PUT:
				//Pack the bulk put message and pass to range_server_put
				range_server_bput(md, 
						  item->message, 
						  item->source);
				break;
			case MDHIM_GET:
				//Determine the operation passed and call the appropriate function
				op = ((struct mdhim_bgetm_t *) item->message)->op;
				if (op == MDHIM_GET_VAL) {
					range_server_get(md, 
							 item->message, 
							 item->source);
				} else if (op == MDHIM_GET_NEXT) {
/*					range_server_get_next(md, 
					item->message, 
					item->source); */
				} else if (op == MDHIM_GET_PREV) {
					/*
					  range_server_get_prev(md, item->message, 
					  item->source);*/
				}
				break;
			case MDHIM_BULK_GET:
				//Determine the operation passed and call the appropriate function
				range_server_bget(md, 
						  item->message, 
						  item->source);
				break;
			case MDHIM_DEL:
				range_server_del(md, item->message, item->source);
				break;
			case MDHIM_BULK_DEL:
				range_server_bdel(md, item->message, item->source);
				break;
			default:
				mlog(MDHIM_SERVER_CRIT, "Rank: %d - Got unknown work type: %d" 
				     " from: %d", md->mdhim_rank, mtype, item->source);
				break;
			}
				
			free(item);
			item = get_work(md);
		}		

		pthread_mutex_unlock(md->mdhim_rs->work_queue_mutex);
	}
	
	return NULL;
}

/**
 * range_server_init
 * Initializes the range server (i.e., starts the threads and populates the relevant data in md)
 *
 * @param md  Pointer to the main MDHIM structure
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_init(struct mdhim_t *md) {
	int ret;
	char filename[255];
	int rangesrv_num;

	//There was an error figuring out if I'm a range server
	if ((rangesrv_num = is_range_server(md, md->mdhim_rank)) == MDHIM_ERROR) {
		return MDHIM_ERROR;		
	}

	//I'm not a range server
	if (!rangesrv_num) {
		return MDHIM_SUCCESS;
	}

	//Allocate memory for the mdhim_rs_t struct
	md->mdhim_rs = malloc(sizeof(struct mdhim_rs_t));
	if (!md->mdhim_rs) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - " 
		     "Error while allocating memory for range server", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}
  
	//Populate md->mdhim_rs
	md->mdhim_rs->info.rank = md->mdhim_rank;
	md->mdhim_rs->info.rangesrv_num = rangesrv_num;

	//Database filename is dependent on ranges.  This needs to be configurable and take a prefix
	sprintf(filename, "%s%d", "mdhim_db", md->mdhim_rank);
	//Initialize data store
	md->mdhim_rs->mdhim_store = mdhim_db_init(UNQLITE);
	if (!md->mdhim_rs->mdhim_store) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - " 
		     "Error while initializing data store with file: %s",
		     md->mdhim_rank,
		     filename);
		return MDHIM_ERROR;
	}

	//Open the database
	if ((ret = md->mdhim_rs->mdhim_store->open(&md->mdhim_rs->mdhim_store->db_handle, 
						   filename, MDHIM_CREATE, NULL)) != MDHIM_SUCCESS){
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - " 
		     "Error while opening database", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}
	
	//Initialize work queue
	md->mdhim_rs->work_queue = malloc(sizeof(work_queue));
	md->mdhim_rs->work_queue->head = NULL;
	md->mdhim_rs->work_queue->tail = NULL;

	//Initialize work queue mutex
	md->mdhim_rs->work_queue_mutex = malloc(sizeof(pthread_mutex_t));
	if (!md->mdhim_rs->work_queue_mutex) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - " 
		     "Error while allocating memory for range server", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}
	if ((ret = pthread_mutex_init(md->mdhim_rs->work_queue_mutex, NULL)) != 0) {    
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - " 
		     "Error while initializing work queue mutex", md->mdhim_rank);
		return MDHIM_ERROR;
	}

	//Initialize the condition variable
	md->mdhim_rs->work_ready_cv = malloc(sizeof(pthread_cond_t));
	if (!md->mdhim_rs->work_ready_cv) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - " 
		     "Error while allocating memory for range server", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}
	if ((ret = pthread_cond_init(md->mdhim_rs->work_ready_cv, NULL)) != 0) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - " 
		     "Error while initializing condition variable", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}

	//Initialize worker thread
	if ((ret = pthread_create(&md->mdhim_rs->worker, NULL, worker_thread, (void *) md)) != 0) {    
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - " 
		     "Error while initializing worker thread", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}
	//Initialize listener thread
	if ((ret = pthread_create(&md->mdhim_rs->listener, NULL, listener_thread, (void *) md)) != 0) {    
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - " 
		     "Error while initializing listener thread", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}

	return MDHIM_SUCCESS;
}

