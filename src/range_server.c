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

int send_locally_or_remote(struct mdhim_t *md, int dest, void *message) {
	int ret = MDHIM_SUCCESS;

	if (md->mdhim_rank != dest) {
		ret = send_client_response(md, dest, message);
	} else {

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

/**
 * get_work
 * Returns the next work item from the work queue
 *
 * @param mdhim_rs  Pointer to the range server info
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
	free(md->mdhim_rs->work_queue_mutex);

	//Destroy the condition variable
	if ((ret = pthread_cond_destroy(md->mdhim_rs->work_ready_cv)) != 0) {
		return MDHIM_ERROR;
	}
	free(md->mdhim_rs->work_ready_cv);

	//Free the work queue
	head = md->mdhim_rs->work_queue->head;
	while (head) {
		temp_item = head->next;
		free(head);
		head = temp_item;
	}

	//Close all the open cursors and free the hash table
	HASH_ITER(hh, mdhim_cursors, cur_cursor, tmp) {
		if ((ret = md->mdhim_rs->mdhim_store->cursor_release(
			     md->mdhim_rs->mdhim_store->db_handle, cur_cursor->cursor)) 
		    != MDHIM_SUCCESS) {
			mlog(MDHIM_SERVER_CRIT, "Rank: %d - Error releasing cursor", 
			     md->mdhim_rank);
			return MDHIM_ERROR;
		}
		HASH_DEL(mdhim_cursors, cur_cursor);  /*delete it (mdhim_cursors advances to next)*/
		free(cur_cursor);            /* free it */
	}

	//Close the database
	if ((ret = md->mdhim_rs->mdhim_store->close(md->mdhim_rs->mdhim_store->db_handle, NULL)) 
	    != MDHIM_SUCCESS) {
		return MDHIM_ERROR;
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

	//Send response
	ret = send_locally_or_remote(md, source, rm);

	//We are done with this message
	mdhim_release_recv_msg(rm);

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
	int error;
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

	//Send response
	ret = send_locally_or_remote(md, source, brm);

	//We are done with this message
	mdhim_release_recv_msg(brm);

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

	//We are done with this message
	mdhim_release_recv_msg(rm);

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

	//We are done with this message
	mdhim_release_recv_msg(brm);

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
	void *value;
	int value_len;
	struct mdhim_getrm_t *grm;
	int ret;

	//Get a record from the database
	if ((error = 
	     md->mdhim_rs->mdhim_store->get(md->mdhim_rs->mdhim_store->db_handle, 
					    gm->key, gm->key_len, (void **) &value, 
					    (int64_t *) &value_len, NULL)) != MDHIM_SUCCESS) {
		mlog(MDHIM_SERVER_CRIT, "Rank: %d - Error getting record", 
		     md->mdhim_rank);
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
	grm->value = value;
	grm->value_len = value_len;
	//Send response
	ret = send_locally_or_remote(md, source, grm);

	//We are done with this message
	mdhim_release_recv_msg(grm);

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
	void *values[MAX_BULK_OPS];
	int value_lens[MAX_BULK_OPS];
	int i;
	struct mdhim_bgetrm_t *bgrm;
	int error;

	//Iterate through the arrays and delete each record
	for (i = 0; i < bgm->num_records && i < MAX_BULK_OPS; i++) {
		//Get records from the database
		if ((ret = 
		     md->mdhim_rs->mdhim_store->get(md->mdhim_rs->mdhim_store->db_handle, 
						    bgm->keys[i], bgm->key_lens[i], (void **) &values[i], 
						    (int64_t *) &value_lens[i], NULL)) != MDHIM_SUCCESS) {
			mlog(MDHIM_SERVER_CRIT, "Rank: %d - Error getting record", 
			     md->mdhim_rank);
			error = ret;
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
	//Send response
	ret = send_locally_or_remote(md, source, bgrm);

	//We are done with this message
	mdhim_release_recv_msg(bgrm);

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

	while (1) {		
		//Receive messages sent to this server
		ret = receive_rangesrv_work(md, &source, &message);
		if (ret != MDHIM_SUCCESS) {
			mlog(MDHIM_SERVER_CRIT, "Rank: %d - Error receiving message in listener", 
			     md->mdhim_rank);
			continue;
		}
	
                //Get the message type
		mtype = ((struct mdhim_basem_t *) message)->mtype;
		mlog(MPI_DBG, "Rank: %d - Received message from : %d of type: %d", 
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
}

/*
 * worker_thread
 * Function for the thread that processes work in work queue
 */
void *worker_thread(void *data) {
	struct mdhim_t *md = (struct mdhim_t *) data;
	struct mdhim_rs_t *mdhim_rs = md->mdhim_rs;
	work_item *item;
	int mtype;
	int op;

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
				break;
			}
			
			free(item);
		}

		pthread_mutex_unlock(mdhim_rs->work_queue_mutex);
	}
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

	//Allocate memory for the mdhim_rs_t struct
	md->mdhim_rs = malloc(sizeof(struct mdhim_rs_t));
	if (!md->mdhim_rs) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - " 
		     "Error while allocating memory for range server", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}
  
	//Populate md->mdhim_rs
	if ((ret = populate_my_ranges(md)) == MDHIM_ERROR) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - Error populating my ranges",
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}

	//Database filename is dependent on ranges.  This needs to be configurable and take a prefix
	sprintf(filename, "%s%lld", "/scratch/mdhim/range", (long long int) md->mdhim_rs->info.start_range);
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
	
	//Initialize work queue to null
	md->mdhim_rs->work_queue = NULL;
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

