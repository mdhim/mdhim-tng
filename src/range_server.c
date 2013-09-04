/*
 * MDHIM TNG
 * 
 * Client specific implementation
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <linux/limits.h>
#include "mdhim.h"
#include "range_server.h"
#include "partitioner.h"
#include "db_options.h"

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

void set_store_opts(struct mdhim_t *md, struct mdhim_store_opts_t *opts) {
	opts->db_ptr1 = md->mdhim_rs->mdhim_store->db_ptr1;
	opts->db_ptr2 = md->mdhim_rs->mdhim_store->db_ptr2;
	opts->db_ptr3 = md->mdhim_rs->mdhim_store->db_ptr3;
	opts->db_ptr4 = md->mdhim_rs->mdhim_store->db_ptr4;
}

/**
 * update_all_stats
 * Adds or updates the given stat to the hash table
 *
 * @param md    pointer to the main MDHIM structure
 * @param type  int that represents the type of stat
 * @param fval  double value for key types that are represented in a float type
 * @param ival  uint64_t for key types that are represented in an int type
 * @param init  int flag to indicate whether this stat is just being initialized, 
 *              but there is not value for it yet
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int update_all_stats(struct mdhim_t *md, void *key, uint32_t key_len) {
	int slice_num;
	void *val1, *val2;
	int float_type = 0;
	struct mdhim_stat *os, *stat;

	if ((float_type = is_float_key(md->key_type)) == 1) {
		val1 = (void *) malloc(sizeof(long double));
		val2 = (void *) malloc(sizeof(long double));
	} else {
		val1 = (void *) malloc(sizeof(uint64_t));
		val2 = (void *) malloc(sizeof(uint64_t));
	}

	if (md->key_type == MDHIM_STRING_KEY) {
		*(long double *)val1 = get_str_num(key, key_len);
		*(long double *)val2 = *(long double *)val1;
	} else if (md->key_type == MDHIM_FLOAT_KEY) {
		*(long double *)val1 = *(float *) key;
		*(long double *)val2 = *(float *) key;
	} else if (md->key_type == MDHIM_DOUBLE_KEY) {
		*(long double *)val1 = *(double *) key;
		*(long double *)val2 = *(double *) key;
	} else if (md->key_type == MDHIM_INT_KEY) {
		*(uint64_t *)val1 = *(uint32_t *) key;
		*(uint64_t *)val2 = *(uint32_t *) key;
	} else if (md->key_type == MDHIM_LONG_INT_KEY) {
		*(uint64_t *)val1 = *(uint64_t *) key;
		*(uint64_t *)val2 = *(uint64_t *) key;
	} else if (md->key_type == MDHIM_BYTE_KEY) {
		*(long double *)val1 = get_byte_num(key, key_len);
		*(long double *)val2 = *(long double *)val1;
	} 

	slice_num = get_slice_num(md, key, key_len);
	HASH_FIND_INT(md->mdhim_rs->mdhim_store->mdhim_store_stats, &slice_num, os);

	stat = malloc(sizeof(struct mdhim_stat));
	stat->min = val1;
	stat->max = val2;
	stat->num = 1;
	stat->key = slice_num;

	if (float_type && os) {
		if (*(long double *)os->min > *(long double *)val1) {
			free(os->min);
			stat->min = val1;
		} else {
			free(val1);
			stat->min = os->min;
		}

		if (*(long double *)os->max < *(long double *)val2) {
			free(os->max);
			stat->max = val2;
		} else {
			free(val2);
			stat->max = os->max;
		}
	}
	if (!float_type && os) {
		mlog(MDHIM_SERVER_DBG, "Rank: %d - stat exists for slice: %d with " 
		     "min: %lu and max: %lu and val1: %lu and val2: %lu", 
		     md->mdhim_rank, slice_num, *(uint64_t *)os->min, *(uint64_t *)os->max, 
		     *(uint64_t *)val1, *(uint64_t *)val2);
		if (*(uint64_t *)os->min > *(uint64_t *)val1) {
			free(os->min);
			stat->min = val1;
		} else {
			free(val1);
			stat->min = os->min;
		}

		if (*(uint64_t *)os->max < *(uint64_t *)val2) {
			free(os->max);
			stat->max = val2;
		} else {
			free(val2);
			stat->max = os->max;
		}		
	}

	if (!os) {
		HASH_ADD_INT(md->mdhim_rs->mdhim_store->mdhim_store_stats, key, stat);    
	} else {	
		stat->num = os->num + 1;
		//Replace the existing stat
		HASH_REPLACE_INT(md->mdhim_rs->mdhim_store->mdhim_store_stats, key, stat, os);  
		free(os);
	}

	return MDHIM_SUCCESS;
}

/**
 * load_stats
 * Loads the statistics from the database
 *
 * @param md  Pointer to the main MDHIM structure
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int load_stats(struct mdhim_t *md) {
	void **val;
	int *val_len, *key_len;
	struct mdhim_store_opts_t opts;
	int **slice;
	int *old_slice;
	struct mdhim_stat *stat;
	int float_type = 0;
	void *min, *max;
	int done = 0;

	float_type = is_float_key(md->key_type);
	slice = malloc(sizeof(int *));
	*slice = NULL;
	key_len = malloc(sizeof(int));
	*key_len = sizeof(int);
	val = malloc(sizeof(struct mdhim_db_stat *));	
	val_len = malloc(sizeof(int));
	set_store_opts(md, &opts);
	old_slice = NULL;
	while (!done) {
		//Check the db for the key/value
		*val = NULL;
		*val_len = 0;		
		md->mdhim_rs->mdhim_store->get_next(md->mdhim_rs->mdhim_store->db_stats, 
						    (void **) slice, key_len, (void **) val, 
						    val_len, &opts);	
		if (old_slice) {
			free(old_slice);
			old_slice = NULL;
		}

		//Add the stat to the hash table - the value is 0 if the key was not in the db
		if (!*val || !*val_len) {
			done = 1;
			continue;
		}

		mlog(MDHIM_SERVER_DBG, "Rank: %d - Loaded stat for slice: %d with " 
		     "imin: %lu and imax: %lu, dmin: %Lf, dmax: %Lf, and num: %lu", 
		     md->mdhim_rank, **slice, (*(struct mdhim_db_stat **)val)->imin, 
		     (*(struct mdhim_db_stat **)val)->imax, (*(struct mdhim_db_stat **)val)->dmin, 
		     (*(struct mdhim_db_stat **)val)->dmax, (*(struct mdhim_db_stat **)val)->num);
	
		stat = malloc(sizeof(struct mdhim_stat));
		if (float_type) {
			min = (void *) malloc(sizeof(long double));
			max = (void *) malloc(sizeof(long double));
			*(long double *)min = (*(struct mdhim_db_stat **)val)->dmin;
			*(long double *)max = (*(struct mdhim_db_stat **)val)->dmax;
		} else {
			min = (void *) malloc(sizeof(uint64_t));
			max = (void *) malloc(sizeof(uint64_t));
			*(uint64_t *)min = (*(struct mdhim_db_stat **)val)->imin;
			*(uint64_t *)max = (*(struct mdhim_db_stat **)val)->imax;
		}

		stat->min = min;
		stat->max = max;
		stat->num = (*(struct mdhim_db_stat **)val)->num;
		stat->key = **slice;
		old_slice = *slice;
		HASH_ADD_INT(md->mdhim_rs->mdhim_store->mdhim_store_stats, key, stat); 
		free(*val);
	}

	free(val);
	free(val_len);
	free(*slice);
	free(slice);
	return MDHIM_SUCCESS;
}

/**
 * write_stats
 * Writes the statistics stored in a hash table to the database
 *
 * @param md  Pointer to the main MDHIM structure
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int write_stats(struct mdhim_t *md) {
	struct mdhim_store_opts_t opts;
	struct mdhim_stat *stat, *tmp;
	struct mdhim_db_stat *dbstat;
	int val_len;
	int float_type = 0;

	float_type = is_float_key(md->key_type);
	set_store_opts(md, &opts);
	val_len = sizeof(struct mdhim_db_stat);

	//Iterate through the stat hash entries
	HASH_ITER(hh, md->mdhim_rs->mdhim_store->mdhim_store_stats, stat, tmp) {	
		if (!stat) {
			continue;
		}

		dbstat = malloc(sizeof(struct mdhim_db_stat));
		if (float_type) {
			dbstat->dmax = *(long double *)stat->max;
			dbstat->dmin = *(long double *)stat->min;
			dbstat->imax = 0;
			dbstat->imin = 0;
		} else {
			dbstat->imax = *(uint64_t *)stat->max;
			dbstat->imin = *(uint64_t *)stat->min;
			dbstat->dmax = 0;
			dbstat->dmin = 0;
		}

		dbstat->slice = stat->key;
		dbstat->num = stat->num;
		//Write the key to the database		
		md->mdhim_rs->mdhim_store->put(md->mdhim_rs->mdhim_store->db_stats, 
					       &dbstat->slice, sizeof(int), dbstat, 
					       sizeof(struct mdhim_db_stat), &opts);	
		//Delete and free hash entry
		HASH_DEL(md->mdhim_rs->mdhim_store->mdhim_store_stats, stat); 
		free(stat->max);
		free(stat->min);
		free(stat);
	}

	return MDHIM_SUCCESS;
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
	struct mdhim_store_opts_t opts;

	//Cancel the worker thread
	if ((ret = pthread_cancel(md->mdhim_rs->worker)) != 0) {
		mlog(MDHIM_SERVER_DBG, "Rank: %d - Error canceling worker thread", 
		     md->mdhim_rank);
	}

	/* Wait for the threads to finish */
	pthread_join(md->mdhim_rs->listener, NULL);
	pthread_join(md->mdhim_rs->worker, NULL);

	//Destroy the condition variables
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

	//Write the stats to the database
	if ((ret = write_stats(md)) != MDHIM_SUCCESS) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - " 
		     "Error while loading stats", 
		     md->mdhim_rank);
	}

	set_store_opts(md, &opts);
	//Close the database
	if ((ret = md->mdhim_rs->mdhim_store->close(md->mdhim_rs->mdhim_store->db_handle, 
						    md->mdhim_rs->mdhim_store->db_stats, &opts)) 
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
	int error = 0;
	struct mdhim_store_opts_t opts;
	void **value;
	int32_t *value_len;
	int exists = 0;
	void *new_value;
	int32_t new_value_len;
	void *old_value;
	int32_t old_value_len;

	set_store_opts(md, &opts);
	value = malloc(sizeof(void *));
	*value = NULL;
	value_len = malloc(sizeof(int32_t));
	*value_len = 0;
        
	//Check for the key's existence
	md->mdhim_rs->mdhim_store->get(md->mdhim_rs->mdhim_store->db_handle, 
				       im->key, im->key_len, value, 
				       value_len, &opts);
	//The key already exists
	if (*value && *value_len) {
		exists = 1;
	}

        //If the option to append was specified and there is old data, concat the old and new
	if (exists &&  md->db_opts->db_append == MDHIM_DB_APPEND) {
		old_value = *value;
		old_value_len = *value_len;
		new_value_len = old_value_len + im->value_len;
		new_value = malloc(new_value_len);
		memcpy(new_value, old_value, old_value_len);
		memcpy(new_value + old_value_len, im->value, im->value_len);
	} else {
		new_value = im->value;
		new_value_len = im->value_len;
	}
    
	free(value);
	free(value_len);
        //Put the record in the database
	if ((ret = 
	     md->mdhim_rs->mdhim_store->put(md->mdhim_rs->mdhim_store->db_handle, 
					    im->key, im->key_len, new_value, 
					    new_value_len, &opts)) != MDHIM_SUCCESS) {
		mlog(MDHIM_SERVER_CRIT, "Rank: %d - Error putting record", 
		     md->mdhim_rank);	
		error = ret;
	}

	if (!exists && error == MDHIM_SUCCESS) {
		update_all_stats(md, im->key, im->key_len);
	}else {
		mlog(MDHIM_SERVER_DBG, "Rank: %d - not updating all stats", md->mdhim_rank);
	}

	//Create the response message
	rm = malloc(sizeof(struct mdhim_rm_t));
	//Set the type
	rm->mtype = MDHIM_RECV;
	//Set the operation return code as the error
	rm->error = error;
	//Set the server's rank
	rm->server_rank = md->mdhim_rank;

	mlog(MDHIM_SERVER_DBG, "Rank: %d - Sending response for put request", 
	     md->mdhim_rank);
	//Send response
	ret = send_locally_or_remote(md, source, rm);

	if (exists && md->db_opts->db_append == MDHIM_DB_APPEND) {
		free(new_value);
	}

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
	int error = MDHIM_SUCCESS;
	struct mdhim_rm_t *brm;
	struct mdhim_store_opts_t opts;
	void **value;
	int32_t *value_len;
	int exists = 0;
	void *new_value;
	int32_t new_value_len;
	void *old_value;
	int32_t old_value_len;

	set_store_opts(md, &opts);
	//Iterate through the arrays and insert each record
	for (i = 0; i < bim->num_records && i < MAX_BULK_OPS; i++) {	
		value = malloc(sizeof(void *));
		*value = NULL;
		value_len = malloc(sizeof(int32_t));
		*value_len = 0;
		exists = 0;
		//Check for the key's existence
		md->mdhim_rs->mdhim_store->get(md->mdhim_rs->mdhim_store->db_handle, 
					       bim->keys[i], bim->key_lens[i], value, 
					       value_len, &opts);
		//The key already exists
		if (*value && *value_len) {
			exists = 1;
		}

		//If the option to append was specified and there is old data, concat the old and new
		if (exists && md->db_opts->db_append == MDHIM_DB_APPEND) {
			old_value = *value;
			old_value_len = *value_len;
			new_value_len = old_value_len + bim->value_lens[i];
			new_value = malloc(new_value_len);
			memcpy(new_value, old_value, old_value_len);
			memcpy(new_value + old_value_len, bim->values[i], bim->value_lens[i]);
		} else {
			new_value = bim->values[i];
			new_value_len = bim->value_lens[i];
		}

		error = MDHIM_SUCCESS;
		//Put the record in the database
		if ((ret = 
		     md->mdhim_rs->mdhim_store->put(md->mdhim_rs->mdhim_store->db_handle, 
						bim->keys[i], bim->key_lens[i], new_value, 
						new_value_len, &opts)) != MDHIM_SUCCESS) {
			mlog(MDHIM_SERVER_CRIT, "Rank: %d - Error putting record", 
			     md->mdhim_rank);
			error = ret;
		}

		if (!exists && error == MDHIM_SUCCESS) {
			update_all_stats(md, bim->keys[i], bim->key_lens[i]);
		}

		if (exists && md->db_opts->db_append == MDHIM_DB_APPEND) {
			free(new_value);
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
	struct mdhim_store_opts_t opts;

	set_store_opts(md, &opts);
	//Put the record in the database
	if ((ret = 
	     md->mdhim_rs->mdhim_store->del(md->mdhim_rs->mdhim_store->db_handle, 
					dm->key, dm->key_len, &opts)) != MDHIM_SUCCESS) {
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
	int error = 0;
	struct mdhim_rm_t *brm;
	struct mdhim_store_opts_t opts;

	set_store_opts(md, &opts);
	//Iterate through the arrays and delete each record
	for (i = 0; i < bdm->num_records && i < MAX_BULK_OPS; i++) {
		//Put the record in the database
		if ((ret = 
		     md->mdhim_rs->mdhim_store->del(md->mdhim_rs->mdhim_store->db_handle, 
						bdm->keys[i], bdm->key_lens[i],
						&opts)) != MDHIM_SUCCESS) {
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
 * range_server_commit
 * Handles the commit message and commits outstanding writes to the database
 *
 * @param md        pointer to the main MDHIM struct
 * @param im        pointer to the commit message to handle
 * @param source    source of the message
 * @return          MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_commit(struct mdhim_t *md, struct mdhim_basem_t *im, int source) {
	int ret;
	struct mdhim_rm_t *rm;
	
        //Put the record in the database
	if ((ret = 
	     md->mdhim_rs->mdhim_store->commit(md->mdhim_rs->mdhim_store->db_handle)) 
	    != MDHIM_SUCCESS) {
		mlog(MDHIM_SERVER_CRIT, "Rank: %d - Error committing database", 
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

	mlog(MDHIM_SERVER_DBG, "Rank: %d - Sending response for commit request", 
	     md->mdhim_rank);
	//Send response
	ret = send_locally_or_remote(md, source, rm);

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
int range_server_get(struct mdhim_t *md, struct mdhim_getm_t *gm, int source, int op) {
	int error = 0;
	void **value;
	void **key;
	int32_t *value_len, key_len;
	struct mdhim_getrm_t *grm;
	int ret;
	struct mdhim_store_opts_t opts;

	set_store_opts(md, &opts);
	//Initialize pointers and lengths
	value = malloc(sizeof(void *));
	value_len = malloc(sizeof(int32_t));
	*value = NULL;
	key = malloc(sizeof(void *));
	*key = NULL;
	key_len = 0;
	*value_len = 0;

	//Set our local pointer to the key and length
	if (gm->key_len) {
		*key = gm->key;
		key_len = gm->key_len;
	}

	//Get a record from the database
	switch(op) {
	// Gets the value for the given key
	case MDHIM_GET_EQ:
		if ((ret = 
		     md->mdhim_rs->mdhim_store->get(md->mdhim_rs->mdhim_store->db_handle, 
						    *key, key_len, value, 
						    value_len, &opts)) != MDHIM_SUCCESS) {
			mlog(MDHIM_SERVER_DBG, "Rank: %d - Couldn't get a record", 
			     md->mdhim_rank);
			error = ret;
		}
		break;
	/* Gets the next key and value that is in order after the passed in key */
	case MDHIM_GET_NEXT:	
		if ((ret = 
		     md->mdhim_rs->mdhim_store->get_next(md->mdhim_rs->mdhim_store->db_handle, 
							 key, &key_len, value, 
							 value_len, &opts)) != MDHIM_SUCCESS) {
			mlog(MDHIM_SERVER_DBG, "Rank: %d - Couldn't get next record", 
			     md->mdhim_rank);
			error = ret;
		}	
		break;
	/* Gets the previous key and value that is in order before the passed in key
	   or the last key if no key was passed in */
	case MDHIM_GET_PREV:
		if ((ret = 
		     md->mdhim_rs->mdhim_store->get_prev(md->mdhim_rs->mdhim_store->db_handle, 
							 key, &key_len, value, 
							 value_len, &opts)) != MDHIM_SUCCESS) {
			mlog(MDHIM_SERVER_DBG, "Rank: %d - Couldn't get previous record", 
			     md->mdhim_rank);
			error = ret;
		}
		break;
	/* Gets the first key/value */
	case MDHIM_GET_FIRST:
		key_len = 0;
		if ((ret = 
		     md->mdhim_rs->mdhim_store->get_next(md->mdhim_rs->mdhim_store->db_handle, 
							 key, &key_len, value, 
							 value_len, &opts)) != MDHIM_SUCCESS) {
			mlog(MDHIM_SERVER_DBG, "Rank: %d - Couldn't get next record", 
			     md->mdhim_rank);
			error = ret;
		}
		break;
	/* Gets the last key/value */
	case MDHIM_GET_LAST:
		key_len = 0;
		if ((ret = 
		     md->mdhim_rs->mdhim_store->get_prev(md->mdhim_rs->mdhim_store->db_handle, 
							 key, &key_len, value, 
							 value_len, &opts)) != MDHIM_SUCCESS) {
			mlog(MDHIM_SERVER_DBG, "Rank: %d - Couldn't get next record", 
			     md->mdhim_rank);
			error = ret;
		}
		break;
	default:
		mlog(MDHIM_SERVER_DBG, "Rank: %d - Invalid operation: %d given in range_server_get", 
		     md->mdhim_rank, op);
		break;
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

	//If we are responding to ourselves, copy the passed in key
	if (source == md->mdhim_rank && gm->key_len && op == MDHIM_GET_EQ) {
		//If this message is coming from myself and a key was sent, copy the key
		grm->key = malloc(key_len);
		memcpy(grm->key, *key, key_len);
	}  else {
		/* Otherwise, just set the pointer to be the key passed in or found 
		   (depends on the op) */
		grm->key = *key;
	}

	//If we aren't responding to ourselves and the op isn't MDHIM_GET_EQ, free the passed in key
	if (source != md->mdhim_rank && gm->key_len && op != MDHIM_GET_EQ) {
	  	free(gm->key);
		gm->key = NULL;
		gm->key_len = 0;
	}

	grm->key_len = key_len;
	grm->value = *value;
	grm->value_len = *value_len;

	//Send response
	mlog(MDHIM_SERVER_DBG, "Rank: %d - About to send get response to: %d", 
	     md->mdhim_rank, source);
	ret = send_locally_or_remote(md, source, grm);
	free(value_len);
	free(value);
	free(key);

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
	struct mdhim_store_opts_t opts;

	set_store_opts(md, &opts);
	values = malloc(sizeof(void *) * bgm->num_records);
	value_lens = malloc(sizeof(int32_t) * bgm->num_records);
	memset(value_lens, 0, sizeof(int32_t) * bgm->num_records);
	//Iterate through the arrays and get each record
	for (i = 0; i < bgm->num_records && i < MAX_BULK_OPS; i++) {
	  //Get records from the database
		if ((ret = 
		     md->mdhim_rs->mdhim_store->get(md->mdhim_rs->mdhim_store->db_handle, 
						    bgm->keys[i], bgm->key_lens[i], &values[i], 
						    &value_lens[i], &opts)) != MDHIM_SUCCESS) {
			mlog(MDHIM_SERVER_DBG, "Rank: %d - Error getting record", md->mdhim_rank);
			error = ret;
			value_lens[i] = 0;
			values[i] = NULL;
			continue;
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
	if (source == md->mdhim_rank) {
		//If this message is coming from myself, copy the keys
		bgrm->key_lens = malloc(bgm->num_records * sizeof(int));		
		bgrm->keys = malloc(bgm->num_records * sizeof(void *));
		for (i = 0; i < bgm->num_records; i++) {
			bgrm->key_lens[i] = bgm->key_lens[i];
			bgrm->keys[i] = malloc(bgrm->key_lens[i]);
			memcpy(bgrm->keys[i], bgm->keys[i], bgrm->key_lens[i]);
		}
	} else {
		bgrm->keys = bgm->keys;
		bgrm->key_lens = bgm->key_lens;
	}

	bgrm->values = values;
	bgrm->value_lens = value_lens;
	bgrm->num_records = bgm->num_records;
	//Send response
	ret = send_locally_or_remote(md, source, bgrm);

	return MDHIM_SUCCESS;
}

/**
 * range_server_bget_op
 * Handles the get message given an op and number of records greater than 1
 * 
 * @param md        Pointer to the main MDHIM struct
 * @param gm        pointer to the get message to handle
 * @param source    source of the message
 * @param op        operation to perform
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_bget_op(struct mdhim_t *md, struct mdhim_getm_t *gm, int source, int op) {
	int error = 0;
	void **values;
	void **keys;
	int32_t *key_lens;
	int32_t *value_lens;
	void *last_key;
	int32_t last_key_len;
	struct mdhim_bgetrm_t *bgrm;
	int ret;
	struct mdhim_store_opts_t opts;
	int i;

	set_store_opts(md, &opts);

	//Initialize pointers and lengths
	values = malloc(sizeof(void *) * gm->num_records);
	value_lens = malloc(sizeof(int32_t) * gm->num_records);
	memset(value_lens, 0, sizeof(int32_t) * gm->num_records);
	keys = malloc(sizeof(void *) * gm->num_records);
	key_lens = malloc(sizeof(int32_t) * gm->num_records);
	memset(key_lens, 0, sizeof(int32_t) * gm->num_records);
	last_key = NULL;
	last_key_len = 0;

	//Iterate through the arrays and get each record
	for (i = 0; i < gm->num_records && i < MAX_BULK_OPS; i++) {
		keys[i] = NULL;
		key_lens[i] = 0;

		//Set our local pointer to the key and length
		if (!i && gm->key_len && gm->key) {
			keys[i] = gm->key;
			key_lens[i] = gm->key_len;			
		} else {
			keys[i] = last_key;
			key_lens[i] = last_key_len;
		}

		switch(op) {
		//Get a record from the database
		case MDHIM_GET_FIRST:	
			if (i == 0) {
				keys[i] = NULL;
				key_lens[i] = sizeof(int32_t);
			}
		case MDHIM_GET_NEXT:	
			if ((ret = 
			     md->mdhim_rs->mdhim_store->get_next(md->mdhim_rs->mdhim_store->db_handle, 
								 (void **) (keys + i), (int32_t *) (key_lens + i), 
								 (void **) (values + i), 
								 (int32_t *) (value_lens + i), &opts)) 
			    != MDHIM_SUCCESS) {
				mlog(MDHIM_SERVER_DBG, "Rank: %d - Couldn't get next record", 
				     md->mdhim_rank);
				error = ret;
				key_lens[i] = 0;
				value_lens[i] = 0;
				goto respond;
			}
			break;
		case MDHIM_GET_LAST:	
			if (i == 0) {
				keys[i] = NULL;
				key_lens[i] = sizeof(int32_t);
			}
		case MDHIM_GET_PREV:
			if ((ret = 
			     md->mdhim_rs->mdhim_store->get_prev(md->mdhim_rs->mdhim_store->db_handle, 
								 (void **) (keys + i), (int32_t *) (key_lens + i), 
								 (void **) (values + i), 
								 (int32_t *) (value_lens + i), &opts)) 
			    != MDHIM_SUCCESS) {
				mlog(MDHIM_SERVER_DBG, "Rank: %d - Couldn't get next record", 
				     md->mdhim_rank);
				error = ret;
				key_lens[i] = 0;
				value_lens[i] = 0;
				goto respond;
			}
			break;
		default:
			mlog(MDHIM_SERVER_CRIT, "Rank: %d - Invalid operation for bulk get op", 
			     md->mdhim_rank);
			goto respond;
			break;
		}
	
		last_key = keys[i];
		last_key_len = key_lens[i];
	}

respond:
       //Create the response message
	bgrm = malloc(sizeof(struct mdhim_bgetrm_t));
	//Set the type
	bgrm->mtype = MDHIM_RECV_BULK_GET;
	//Set the operation return code as the error
	bgrm->error = error;
	//Set the server's rank
	bgrm->server_rank = md->mdhim_rank;
	bgrm->keys = keys;
	bgrm->key_lens = key_lens;
	//Set the key and value
	if (source != md->mdhim_rank) {
		//If this message is not coming from myself, free the key in the gm message
		free(gm->key);
	}

	bgrm->values = values;
	bgrm->value_lens = value_lens;
	bgrm->num_records = gm->num_records;
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
			mlog(MDHIM_SERVER_DBG, "Rank: %d - Received close message", 
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
	int num_records;

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
				op = ((struct mdhim_getm_t *) item->message)->op;
				num_records = ((struct mdhim_getm_t *) item->message)->num_records;
				if (num_records > 1) {
					range_server_bget_op(md, 
							 item->message, 
							 item->source, op);
				} else {
					range_server_get(md, 
							 item->message, 
							 item->source, op);
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
			case MDHIM_COMMIT:
				range_server_commit(md, item->message, item->source);
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
	char filename[PATH_MAX];
	int rangesrv_num;
	int flags = MDHIM_CREATE;
	struct mdhim_store_opts_t opts;

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
        sprintf(filename, "%s%s%d", md->db_opts->db_path, md->db_opts->db_name, md->mdhim_rank);
        
	//Initialize data store
	md->mdhim_rs->mdhim_store = mdhim_db_init(md->db_opts->db_type);
	if (!md->mdhim_rs->mdhim_store) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - " 
		     "Error while initializing data store with file: %s",
		     md->mdhim_rank,
		     filename);
		return MDHIM_ERROR;
	}

	//Clear the options
	memset(&opts, 0, sizeof(struct mdhim_store_opts_t));
	//Set the key type
	opts.key_type = md->key_type;

	//Open the main database and the stats database
	if ((ret = md->mdhim_rs->mdhim_store->open(&md->mdhim_rs->mdhim_store->db_handle,
						   &md->mdhim_rs->mdhim_store->db_stats,
						   filename, flags, &opts)) != MDHIM_SUCCESS){
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - " 
		     "Error while opening database", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}
	
	md->mdhim_rs->mdhim_store->db_ptr1 = opts.db_ptr1;
	md->mdhim_rs->mdhim_store->db_ptr2 = opts.db_ptr2;
	md->mdhim_rs->mdhim_store->db_ptr3 = opts.db_ptr3;
	md->mdhim_rs->mdhim_store->db_ptr4 = opts.db_ptr4;
	
	//Load the stats from the database
	if ((ret = load_stats(md)) != MDHIM_SUCCESS) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - " 
		     "Error while loading stats", 
		     md->mdhim_rank);
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

	//Initialize the condition variables
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

/**
 * range_server_init_comm
 * Initializes the range server communicator that is used for range server to range 
 * server collectives
 * The stat flush function will use this communicator
 *
 * @param md  Pointer to the main MDHIM structure
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_init_comm(struct mdhim_t *md) {
	MPI_Group orig, new_group;
	int *ranks;
	rangesrv_info *rp;
	int i = 0, j = 0;
	int ret;
	int size;
	MPI_Comm new_comm;

	//Populate the ranks array that will be in our new comm
	if ((ret = im_range_server(md)) == 1) {
		ranks = malloc(sizeof(int) * md->num_rangesrvs);
		rp = md->rangesrvs;
		size = 0;
		while (rp) {
			ranks[i] = rp->rank;
			mlog(MDHIM_SERVER_DBG2, "MDHIM Rank: %d - " 
			     "Adding rank: %d to range server comm", 
			     md->mdhim_rank, rp->rank);
			i++;
			size++;
			rp = rp->next;
		}
	} else {
		MPI_Comm_size(md->mdhim_comm, &size);
		ranks = malloc(sizeof(int) * (size - md->num_rangesrvs));
		rp = md->rangesrvs;
		size = 0;
		j = i = 0;
		while (rp) {
			if (i == rp->rank) {
				rp = rp->next;
				i++;
				continue;
			}

			ranks[j] = i;
			mlog(MDHIM_SERVER_DBG, "MDHIM Rank: %d - " 
			     "Adding rank: %d to clients only comm", 
			     md->mdhim_rank, i);
			i++;
			j++;
			size++;
		}
	}

	//Create a new group with the range servers only
	if ((ret = MPI_Comm_group(md->mdhim_comm, &orig)) != MPI_SUCCESS) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - " 
		     "Error while creating a new group in range_server_init_comm", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}

	if ((ret = MPI_Group_incl(orig, size, ranks, &new_group)) != MPI_SUCCESS) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - " 
		     "Error while creating adding ranks to the new group in range_server_init_comm", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}

	if ((ret = MPI_Comm_create(md->mdhim_comm, new_group, &new_comm)) 
	    != MPI_SUCCESS) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - " 
		     "Error while creating the new communicator in range_server_init_comm", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}
	if ((ret = im_range_server(md)) == 1) {
		memcpy(&md->mdhim_rs->rs_comm, &new_comm, sizeof(MPI_Comm));
	}

	free(ranks);
	return MDHIM_SUCCESS;
}
