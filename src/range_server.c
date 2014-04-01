/*
 * MDHIM TNG
 * 
 * Client specific implementation
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <linux/limits.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "mdhim.h"
#include "range_server.h"
#include "partitioner.h"
#include "mdhim_options.h"

/**
 * im_range_server
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

void add_timing(struct timeval start, struct timeval end, int num, 
		struct mdhim_t *md, int mtype) {
	long double elapsed;

	elapsed = (long double) (end.tv_sec - start.tv_sec) + 
		((long double) (end.tv_usec - start.tv_usec)/1000000.0);
	if (mtype == MDHIM_PUT || mtype == MDHIM_BULK_PUT) {
		md->mdhim_rs->put_time += elapsed;
		md->mdhim_rs->num_put += num;
	} else if (mtype == MDHIM_GET || mtype == MDHIM_BULK_GET) {
		md->mdhim_rs->get_time += elapsed;
		md->mdhim_rs->num_get += num;
	}
}

/**
 * open_manifest
 * Opens the manifest file
 *
 * @param md       Pointer to the main MDHIM structure
 * @param flags    Flags to open the file with
 */
int open_manifest(struct mdhim_t *md, int flags) {
	int fd;	
	
	fd = open(md->db_opts->manifest_path, flags, 00600);
	if (fd < 0) {
		mlog(MDHIM_SERVER_CRIT, "Rank: %d - Error opening manifest file", 
		     md->mdhim_rank);
	}
	
	return fd;
}

/**
 * write_manifest
 * Writes out the manifest file
 *
 * @param md       Pointer to the main MDHIM structure
 */
void write_manifest(struct mdhim_t *md) {
	mdhim_manifest_t manifest;
	int fd;
	int ret;

	//Range server with range server number 1 is in charge of the manifest
	if (md->mdhim_rs->info.rangesrv_num != 1) {	
		return;
	}

	if ((fd = open_manifest(md, O_RDWR | O_CREAT | O_TRUNC)) < 0) {
		mlog(MDHIM_SERVER_CRIT, "Rank: %d - Error opening manifest file", 
		     md->mdhim_rank);
		return;
	}
	
	//Populate the manifest structure
	manifest.num_rangesrvs = md->num_rangesrvs;
	manifest.key_type = md->key_type;
	manifest.db_type = md->db_opts->db_type;
	manifest.rangesrv_factor = md->db_opts->rserver_factor;
	manifest.slice_size = md->db_opts->max_recs_per_slice;
	manifest.num_nodes = md->mdhim_comm_size;
	if ((ret = write(fd, &manifest, sizeof(manifest))) < 0) {
		mlog(MDHIM_SERVER_CRIT, "Rank: %d - Error writing manifest file", 
		     md->mdhim_rank);
	}

	close(fd);
}

/**
 * read_manifest
 * Reads in and validates the manifest file
 *
 * @param md       Pointer to the main MDHIM structure
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int read_manifest(struct mdhim_t *md) {
	int fd;
	int ret;
	mdhim_manifest_t manifest;
	struct stat st;

	if ((ret = stat(md->db_opts->manifest_path, &st)) < 0) {
		if (errno == ENOENT) {
			mlog(MDHIM_SERVER_DBG, "Rank: %d - Manifest doesn't exist", 
			     md->mdhim_rank);
			return MDHIM_SUCCESS;
		} else {
			mlog(MDHIM_SERVER_DBG, "Rank: %d - There was a problem accessing" 
			     " the manifest file", 
			     md->mdhim_rank);
			return MDHIM_ERROR;
		}		
	}
	
	if (st.st_size != sizeof(manifest)) {
		mlog(MDHIM_SERVER_DBG, "Rank: %d - The manifest file has the wrong size", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}

	if ((fd = open_manifest(md, O_RDWR)) < 0) {
		return MDHIM_ERROR;
	}

	if ((ret = read(fd, &manifest, sizeof(manifest))) < 0) {
		mlog(MDHIM_SERVER_CRIT, "Rank: %d - Error reading manifest file", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}

	ret = MDHIM_SUCCESS;	
	mlog(MDHIM_SERVER_DBG, "Rank: %d - Manifest contents - \nnum_rangesrvs: %d, key_type: %d, " 
	     "db_type: %d, rs_factor: %u, slice_size: %lu, num_nodes: %d", 
	     md->mdhim_rank, manifest.num_rangesrvs, manifest.key_type, manifest.db_type, 
	     manifest.rangesrv_factor, manifest.slice_size, manifest.num_nodes);
	
	//Check that the manifest and the current config match
	if (manifest.key_type != md->key_type) {
		mlog(MDHIM_SERVER_CRIT, "Rank: %d - The key type in the manifest file" 
		     " doesn't match the current key type", 
		     md->mdhim_rank);
		ret = MDHIM_ERROR;
	}
	if (manifest.db_type != md->db_opts->db_type) {
		mlog(MDHIM_SERVER_CRIT, "Rank: %d - The database type in the manifest file" 
		     " doesn't match the current database type", 
		     md->mdhim_rank);
		ret = MDHIM_ERROR;
	}
	if (manifest.rangesrv_factor != md->db_opts->rserver_factor) {
		mlog(MDHIM_SERVER_CRIT, "Rank: %d - The range server factor in the manifest file" 
		     " doesn't match the current range server factor", 
		     md->mdhim_rank);
		ret = MDHIM_ERROR;
	}
	if (manifest.slice_size != md->db_opts->max_recs_per_slice) {
		mlog(MDHIM_SERVER_CRIT, "Rank: %d - The slice size in the manifest file" 
		     " doesn't match the current slice size", 
		     md->mdhim_rank);
		ret = MDHIM_ERROR;
	}
	if (manifest.num_nodes != md->mdhim_comm_size) {
		mlog(MDHIM_SERVER_CRIT, "Rank: %d - The number of nodes in this MDHIM instance" 
		     " doesn't match the number used previously", 
		     md->mdhim_rank);
		ret = MDHIM_ERROR;
	}
	
	close(fd);
	return ret;
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
	MPI_Request **size_req, **msg_req;
	void **sendbuf;

	if (md->mdhim_rank != dest) {
		//Sends the message remotely
		size_req = malloc(sizeof(MPI_Request *));
		msg_req = malloc(sizeof(MPI_Request *));
		sendbuf = malloc(sizeof(void *));
		ret = send_client_response(md, dest, message, sendbuf, size_req, msg_req);
		if (*size_req) {
			range_server_add_oreq(md, *size_req, NULL);
		}
		if (*msg_req) {
			range_server_add_oreq(md, *msg_req, *sendbuf);
		} else if (*sendbuf) {
			free(*sendbuf);
		}
		
		free(sendbuf);
		mdhim_full_release_msg(message);
		free(size_req);
		free(msg_req);
	} else {
		//Sends the message locally
		pthread_mutex_lock(md->receive_msg_mutex);
		md->receive_msg = message;
		pthread_mutex_unlock(md->receive_msg_mutex);
		pthread_cond_signal(md->receive_msg_ready_cv);
	}

	return ret;
}

void set_store_opts(struct mdhim_t *md, struct mdhim_store_opts_t *opts, int stat) {
	if (!stat) {
		opts->db_ptr1 = md->mdhim_rs->mdhim_store->db_ptr1;
		opts->db_ptr2 = md->mdhim_rs->mdhim_store->db_ptr2;
		opts->db_ptr3 = md->mdhim_rs->mdhim_store->db_ptr3;
		opts->db_ptr4 = md->mdhim_rs->mdhim_store->db_ptr4;
		opts->db_ptr5 = md->mdhim_rs->mdhim_store->db_ptr5;
		opts->db_ptr6 = md->mdhim_rs->mdhim_store->db_ptr6;
		opts->db_ptr7 = md->mdhim_rs->mdhim_store->db_ptr7;
	} else {
		opts->db_ptr1 = NULL;	       
		opts->db_ptr2 = md->mdhim_rs->mdhim_store->db_ptr5;
		opts->db_ptr3 = md->mdhim_rs->mdhim_store->db_ptr6;
		opts->db_ptr4 = md->mdhim_rs->mdhim_store->db_ptr7;
		opts->db_ptr5 = NULL;	       
		opts->db_ptr6 = NULL;	       
		opts->db_ptr7 = NULL;	       
	}  

	opts->db_ptr8 = md->mdhim_rs->mdhim_store->db_ptr8;
	opts->db_ptr9 = md->mdhim_rs->mdhim_store->db_ptr9;
	opts->db_ptr10 = md->mdhim_rs->mdhim_store->db_ptr10;
	opts->db_ptr11 = md->mdhim_rs->mdhim_store->db_ptr11;
	opts->db_ptr12 = md->mdhim_rs->mdhim_store->db_ptr12;
	opts->db_ptr13 = md->mdhim_rs->mdhim_store->db_ptr13;
	opts->db_ptr14 = md->mdhim_rs->mdhim_store->db_ptr14;

	opts->key_type = md->key_type;
}

/**
 * update_all_stats
 * Adds or updates the given stat to the hash table
 *
 * @param md       pointer to the main MDHIM structure
 * @param key      pointer to the key we are examining
 * @param key_len  the key's length
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
	set_store_opts(md, &opts, 1);
	old_slice = NULL;
	while (!done) {
		//Check the db for the key/value
		*val = NULL;
		*val_len = 0;		
		md->mdhim_rs->mdhim_store->get_next(md->mdhim_rs->mdhim_store->db_stats, 
						    (void **) slice, key_len, (void **) val, 
						    val_len, &opts);	
		
		//Add the stat to the hash table - the value is 0 if the key was not in the db
		if (!*val || !*val_len) {
			done = 1;
			continue;
		}

		if (old_slice) {
			free(old_slice);
			old_slice = NULL;
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
	free(key_len);
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
	int float_type = 0;

	float_type = is_float_key(md->key_type);
	set_store_opts(md, &opts, 1);

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
		free(dbstat);
	}

	return MDHIM_SUCCESS;
}

/**
 * range_server_add_work
 * Adds work to the work queue and signals the condition variable for the worker thread
 *
 * @param md      Pointer to the main MDHIM structure
 * @param item    pointer to new work item that contains a message to handle
 * @return MDHIM_SUCCESS
 */
int range_server_add_work(struct mdhim_t *md, work_item *item) {
	//Lock the work queue mutex
	pthread_mutex_lock(md->mdhim_rs->work_queue_mutex);
	item->next = NULL;
	item->prev = NULL;       
	
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
	pthread_mutex_unlock(md->mdhim_rs->work_queue_mutex);
	pthread_cond_signal(md->mdhim_rs->work_ready_cv);

	return MDHIM_SUCCESS;
}

/**
 * get_work
 * Returns the next work item from the work queue
 *
 * @param md  Pointer to the main MDHIM structure
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
	int ret, i;	
	struct mdhim_store_opts_t opts;

	//Cancel the worker threads
	for (i = 0; i < md->db_opts->num_wthreads; i++) {
		if ((ret = pthread_cancel(*md->mdhim_rs->workers[i])) != 0) {
			mlog(MDHIM_SERVER_DBG, "Rank: %d - Error canceling worker thread", 
			     md->mdhim_rank);
		}
	}
	
	/* Wait for the threads to finish */
	//pthread_join(md->mdhim_rs->listener, NULL);
	for (i = 0; i < md->db_opts->num_wthreads; i++) {
		pthread_join(*md->mdhim_rs->workers[i], NULL);
	}

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

	//Write the manifest
	write_manifest(md);

	set_store_opts(md, &opts, 0);
	//Close the database
	if ((ret = md->mdhim_rs->mdhim_store->close(md->mdhim_rs->mdhim_store->db_handle, 
						    md->mdhim_rs->mdhim_store->db_stats, &opts)) 
	    != MDHIM_SUCCESS) {
		mlog(MDHIM_SERVER_CRIT, "Rank: %d - Error closing database", 
		     md->mdhim_rank);
	}

	mlog(MDHIM_SERVER_INFO, "Rank: %d - Inserted: %ld records in %Lf seconds", 
	     md->mdhim_rank, md->mdhim_rs->num_put, md->mdhim_rs->put_time);
	mlog(MDHIM_SERVER_INFO, "Rank: %d - Retrieved: %ld records in %Lf seconds", 
	     md->mdhim_rank, md->mdhim_rs->num_get, md->mdhim_rs->get_time);
	
	//Free the range server data
	range_server_clean_oreqs(md);
	MPI_Comm_free(&md->mdhim_rs->rs_comm);
	free(md->mdhim_rs->mdhim_store);
	free(md->mdhim_rs);
	md->mdhim_rs = NULL;
	
	free(md);
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
	struct timeval start, end;
	int inserted = 0;

	set_store_opts(md, &opts, 0);
	value = malloc(sizeof(void *));
	*value = NULL;
	value_len = malloc(sizeof(int32_t));
	*value_len = 0;
        
	gettimeofday(&start, NULL);
       //Check for the key's existence
	md->mdhim_rs->mdhim_store->get(md->mdhim_rs->mdhim_store->db_handle, 
				       im->key, im->key_len, value, 
				       value_len, &opts);
	//The key already exists
	if (*value && *value_len) {
		exists = 1;
	}

        //If the option to append was specified and there is old data, concat the old and new
	if (exists &&  md->db_opts->db_value_append == MDHIM_DB_APPEND) {
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
    
	if (*value && *value_len) {
		free(*value);
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
	} else {
		inserted = 1;
	}

	if (!exists && error == MDHIM_SUCCESS) {
		update_all_stats(md, im->key, im->key_len);
	}

	gettimeofday(&end, NULL);
	add_timing(start, end, inserted, md, MDHIM_PUT);

	//Create the response message
	rm = malloc(sizeof(struct mdhim_rm_t));
	//Set the type
	rm->mtype = MDHIM_RECV;
	//Set the operation return code as the error
	rm->error = error;
	//Set the server's rank
	rm->server_rank = md->mdhim_rank;
	
	//Send response
	ret = send_locally_or_remote(md, source, rm);

	//Free memory
	if (exists && md->db_opts->db_value_append == MDHIM_DB_APPEND) {
		free(new_value);
	}
	if (source != md->mdhim_rank) {
		free(im->key);
		free(im->value);
	} 
	free(im);
	
	return MDHIM_SUCCESS;
}


/**
 * range_server_bput
 * Handles the bulk put message and puts data in the database
 *
 * @param md        Pointer to the main MDHIM struct
 * @param bim       pointer to the bulk put message to handle
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
	int *exists;
	void *new_value;
	int32_t new_value_len;
	void **new_values;
	int32_t *new_value_lens;
	void *old_value;
	int32_t old_value_len;
	struct timeval start, end;
	int num_put = 0;

	gettimeofday(&start, NULL);
	set_store_opts(md, &opts, 0);
	exists = malloc(bim->num_records * sizeof(int));
	new_values = malloc(bim->num_records * sizeof(void *));
	new_value_lens = malloc(bim->num_records * sizeof(int));
	value = malloc(sizeof(void *));
	value_len = malloc(sizeof(int32_t));

	//Iterate through the arrays and insert each record
	for (i = 0; i < bim->num_records && i < MAX_BULK_OPS; i++) {	
		*value = NULL;
		*value_len = 0;

                //Check for the key's existence
		md->mdhim_rs->mdhim_store->get(md->mdhim_rs->mdhim_store->db_handle, 
					       bim->keys[i], bim->key_lens[i], value, 
					       value_len, &opts);
		//The key already exists
		if (*value && *value_len) {
			exists[i] = 1;
		} else {
			exists[i] = 0;
		}

		//If the option to append was specified and there is old data, concat the old and new
		if (exists[i] && md->db_opts->db_value_append == MDHIM_DB_APPEND) {
			old_value = *value;
			old_value_len = *value_len;
			new_value_len = old_value_len + bim->value_lens[i];
			new_value = malloc(new_value_len);
			memcpy(new_value, old_value, old_value_len);
			memcpy(new_value + old_value_len, bim->values[i], bim->value_lens[i]);		
			if (exists[i] && source != md->mdhim_rank) {
				free(bim->values[i]);
			}

			new_values[i] = new_value;
			new_value_lens[i] = new_value_len;
		} else {
			new_values[i] = bim->values[i];
			new_value_lens[i] = bim->value_lens[i];
		}
		
		if (*value) {
			free(*value);
		}
	}

	//Put the record in the database
	if ((ret = 
	     md->mdhim_rs->mdhim_store->batch_put(md->mdhim_rs->mdhim_store->db_handle, 
						  bim->keys, bim->key_lens, new_values, 
						  new_value_lens, bim->num_records,
						  &opts)) != MDHIM_SUCCESS) {
		mlog(MDHIM_SERVER_CRIT, "Rank: %d - Error batch putting records", 
		     md->mdhim_rank);
		error = ret;
	} else {
		num_put = bim->num_records;
	}

	for (i = 0; i < bim->num_records && i < MAX_BULK_OPS; i++) {
		//Update the stats if this key didn't exist before
		if (!exists[i] && error == MDHIM_SUCCESS) {
			update_all_stats(md, bim->keys[i], bim->key_lens[i]);
		}
	       
		if (exists[i] && md->db_opts->db_value_append == MDHIM_DB_APPEND) {
			//Release the value created for appending the new and old value
			free(new_values[i]);
		}		

		//Release the bput keys/value if the message isn't coming from myself
		if (source != md->mdhim_rank) {
			free(bim->keys[i]);
			free(bim->values[i]);
		} 
	}

	free(exists);
	free(new_values);
	free(new_value_lens);
	free(value);
	free(value_len);
	gettimeofday(&end, NULL);
	add_timing(start, end, num_put, md, MDHIM_BULK_PUT);

	//Create the response message
	brm = malloc(sizeof(struct mdhim_rm_t));
	//Set the type
	brm->mtype = MDHIM_RECV;
	//Set the operation return code as the error
	brm->error = error;
	//Set the server's rank
	brm->server_rank = md->mdhim_rank;

	//Release the internals of the bput message
	free(bim->keys);
	free(bim->key_lens);
	free(bim->values);
	free(bim->value_lens);
	free(bim);

	//Send response
	ret = send_locally_or_remote(md, source, brm);

	return MDHIM_SUCCESS;
}

/**
 * range_server_del
 * Handles the delete message and deletes the data from the database
 *
 * @param md       Pointer to the main MDHIM struct
 * @param dm       pointer to the delete message to handle
 * @param source   source of the message
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_del(struct mdhim_t *md, struct mdhim_delm_t *dm, int source) {
	int ret;
	struct mdhim_rm_t *rm;
	struct mdhim_store_opts_t opts;

	set_store_opts(md, &opts, 0);
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
	free(dm);

	return MDHIM_SUCCESS;
}

/**
 * range_server_bdel
 * Handles the bulk delete message and deletes the data from the database
 *
 * @param md        Pointer to the main MDHIM struct
 * @param bdm       pointer to the bulk delete message to handle
 * @param source    source of the message
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_bdel(struct mdhim_t *md, struct mdhim_bdelm_t *bdm, int source) {
 	int i;
	int ret;
	int error = 0;
	struct mdhim_rm_t *brm;
	struct mdhim_store_opts_t opts;

	set_store_opts(md, &opts, 0);
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
	free(bdm->keys);
	free(bdm->key_lens);
	free(bdm);

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

	//Send response
	ret = send_locally_or_remote(md, source, rm);
	free(im);

	return MDHIM_SUCCESS;
}

/**
 * range_server_get
 * Handles the get message, retrieves the data from the database, and sends the results back
 * 
 * @param md        Pointer to the main MDHIM struct
 * @param gm        pointer to the get message to handle
 * @param source    source of the message
 * @param op        Operation to perform (MDHIM_GET_EQ, MDHIM_GET_NEXT, etc...)
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
	struct timeval start, end;
	int num_retrieved = 0;

	set_store_opts(md, &opts, 0);
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

	gettimeofday(&start, NULL);
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

	if (error == MDHIM_SUCCESS) {
		num_retrieved = 1;
	}

	gettimeofday(&end, NULL);
	add_timing(start, end, num_retrieved, md, MDHIM_GET);

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
	ret = send_locally_or_remote(md, source, grm);
	free(gm);
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
 * @param bgm       pointer to the bulk get message to handle
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
	struct timeval start, end;
	int num_retrieved = 0;

	gettimeofday(&start, NULL);
	set_store_opts(md, &opts, 0);
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

		num_retrieved++;
	}

	gettimeofday(&end, NULL);
	add_timing(start, end, num_retrieved, md, MDHIM_BULK_GET);

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

		free(bgm->keys);
		free(bgm->key_lens);
	} else {
		bgrm->keys = bgm->keys;
		bgrm->key_lens = bgm->key_lens;
	}

	bgrm->values = values;
	bgrm->value_lens = value_lens;
	bgrm->num_records = bgm->num_records;

	//Send response
	ret = send_locally_or_remote(md, source, bgrm);

	//Release the bget message
	free(bgm);

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
	void **get_key; //Used for passing the key to the db
	int *get_key_len; //Used for passing the key len to the db
	void **get_value;
	int *get_value_len;
	int32_t *key_lens;
	int32_t *value_lens;
	struct mdhim_bgetrm_t *bgrm;
	int ret;
	struct mdhim_store_opts_t opts;
	int i;
	int num_records;
	struct timeval start, end;

	set_store_opts(md, &opts, 0);

	//Initialize pointers and lengths
	values = malloc(sizeof(void *) * gm->num_records);
	value_lens = malloc(sizeof(int32_t) * gm->num_records);
	memset(value_lens, 0, sizeof(int32_t) * gm->num_records);
	keys = malloc(sizeof(void *) * gm->num_records);
	memset(keys, 0, sizeof(void *) * gm->num_records);
	key_lens = malloc(sizeof(int32_t) * gm->num_records);
	memset(key_lens, 0, sizeof(int32_t) * gm->num_records);
	get_key = malloc(sizeof(void *));
	*get_key = NULL;
	get_key_len = malloc(sizeof(int32_t));
	*get_key_len = 0;
	get_value = malloc(sizeof(void *));
	get_value_len = malloc(sizeof(int32_t));
	num_records = 0;

	gettimeofday(&start, NULL);
	//Iterate through the arrays and get each record
	for (i = 0; i < gm->num_records && i < MAX_BULK_OPS; i++) {
		keys[i] = NULL;
		key_lens[i] = 0;

		//If we were passed in a key, copy it
		if (!i && gm->key_len && gm->key) {
			*get_key = malloc(gm->key_len);
			memcpy(*get_key, gm->key, gm->key_len);
			*get_key_len = gm->key_len;
		//If we were not passed a key and this is a next/prev, then return an error
		} else if (!i && (!gm->key_len || !gm->key)
			   && (op ==  MDHIM_GET_NEXT || 
			       op == MDHIM_GET_PREV)) {
			error = MDHIM_ERROR;
			goto respond;
		}

		switch(op) {
		//Get a record from the database
		case MDHIM_GET_FIRST:	
			if (i == 0) {
				keys[i] = NULL;
				key_lens[i] = sizeof(int32_t);
			}
		case MDHIM_GET_NEXT:	
			if (i && (ret = 
				  md->mdhim_rs->mdhim_store->get_next(md->mdhim_rs->mdhim_store->db_handle, 
								      get_key, get_key_len, 
								      get_value, 
								      get_value_len,
								      &opts)) 
			    != MDHIM_SUCCESS) {
				mlog(MDHIM_SERVER_DBG, "Rank: %d - Couldn't get next record", 
				     md->mdhim_rank);
				error = ret;
				key_lens[i] = 0;
				value_lens[i] = 0;
				goto respond;
			} else if (!i && (ret = 
					  md->mdhim_rs->mdhim_store->get(md->mdhim_rs->mdhim_store->db_handle, 
									 *get_key, *get_key_len, 
									 get_value, 
									 get_value_len,
									 &opts))
				   != MDHIM_SUCCESS) {
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
			if (i && (ret = 
			     md->mdhim_rs->mdhim_store->get_prev(md->mdhim_rs->mdhim_store->db_handle, 
								 get_key, get_key_len, 
								 get_value, 
								 get_value_len,
								 &opts)) 
			    != MDHIM_SUCCESS) {
				mlog(MDHIM_SERVER_DBG, "Rank: %d - Couldn't get prev record", 
				     md->mdhim_rank);
				error = ret;
				key_lens[i] = 0;
				value_lens[i] = 0;
				goto respond;
			} else if (!i && (ret = 
					  md->mdhim_rs->mdhim_store->get(md->mdhim_rs->mdhim_store->db_handle, 
									 *get_key, *get_key_len, 
									 get_value, 
									 get_value_len,
									 &opts))
				   != MDHIM_SUCCESS) {
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

		keys[i] = *get_key;
		key_lens[i] = *get_key_len;
	        values[i] = *get_value;
		value_lens[i] = *get_value_len;
		num_records++;
	}

respond:

	gettimeofday(&end, NULL);
	add_timing(start, end, num_records, md, MDHIM_BULK_GET);

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
	bgrm->values = values;
	bgrm->value_lens = value_lens;
	bgrm->num_records = num_records;
	//Send response
	ret = send_locally_or_remote(md, source, bgrm);

	//Free the incoming message
	if (source != md->mdhim_rank) {
		//If this message is not coming from myself, free the key in the gm message
		free(gm->key);
	} 
	free(gm);
	free(get_key);
	free(get_key_len);
	free(get_value);
	free(get_value_len);

	return MDHIM_SUCCESS;
}

/*
 * listener_thread
 * Function for the thread that listens for new messages
 */
void *listener_thread(void *data) {	
	//Mlog statements could cause a deadlock on range_server_stop due to canceling of threads

	struct mdhim_t *md = (struct mdhim_t *) data;
	void *message;
	int source; //The source of the message
	int ret;
	work_item *item;

	pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);
	pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);

	while (1) {		
		//Receive messages sent to this server
		ret = receive_rangesrv_work(md, &source, &message);
		if (ret < MDHIM_SUCCESS) {		
			continue;
		}

//		printf("Rank: %d - Received message from rank: %d of type: %d", 
//		     md->mdhim_rank, source, mtype);

		//We received a close message - so quit
		if (ret == MDHIM_CLOSE) {
            range_server_stop(md);
			break;
		}
		
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
	//Mlog statements could cause a deadlock on range_server_stop due to canceling of threads

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
			pthread_mutex_unlock(md->mdhim_rs->work_queue_mutex);
			continue;
		}

		while (item) {
			//Clean outstanding sends
			range_server_clean_oreqs(md);

			//Call the appropriate function depending on the message type			
			//Get the message type
			mtype = ((struct mdhim_basem_t *) item->message)->mtype;

//			printf("Rank: %d - Got work item from queue with type: %d" 
//			     " from: %d", md->mdhim_rank, mtype, item->source);

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
			case MDHIM_CLOSE:
				free(item);
				pthread_mutex_unlock(md->mdhim_rs->work_queue_mutex);
			
			    goto done;
				break;
			default:
				printf("Rank: %d - Got unknown work type: %d" 
				       " from: %d", md->mdhim_rank, mtype, item->source);
				break;
			}
			
			free(item);		    
			item = get_work(md);
		}		
		
		pthread_mutex_unlock(md->mdhim_rs->work_queue_mutex);
	}
	
done:
	return NULL;
}

int range_server_add_oreq(struct mdhim_t *md, MPI_Request *req, void *msg) {
	out_req *oreq;
	out_req *item = md->mdhim_rs->out_req_list;

	oreq = malloc(sizeof(out_req));
	oreq->next = NULL;
	oreq->prev = NULL;
	oreq->message = msg;
	oreq->req = req;

	if (!item) {
		md->mdhim_rs->out_req_list = oreq;
		return MDHIM_SUCCESS;
	}

	while (item) {
		if (!item->next) {
			item->next = oreq;
			oreq->prev = item;
			break;
		}

		item = item->next;
	}

	return MDHIM_SUCCESS;
	
}

int range_server_clean_oreqs(struct mdhim_t *md) {
	out_req *item = md->mdhim_rs->out_req_list;
	out_req *t;
	int ret;
	int flag = 0;
	MPI_Status status;

	while (item) {
		if (!item->req) {
			item = item->next;
			continue;
		}

		ret = MPI_Test((MPI_Request *)item->req, &flag, &status); 
		if (ret == MPI_ERR_REQUEST) {
			//flag = 1;
		}
		if (!flag) {
			item = item->next;
			continue;
		}
		
		if (item->next) {
			item->next->prev = item->prev;
		}
		if (item->prev) {
			item->prev->next = item->next;
		}
		if (item == md->mdhim_rs->out_req_list) {
			md->mdhim_rs->out_req_list = item->next;
		}

		t = item->next;
		free(item->req);
		if (item->message) {
			free(item->message);
		}

		free(item);
		item = t;
	}

	return MDHIM_SUCCESS;
}

/**
 * range_server_init
 * Initializes the range server (i.e., starts the threads and populates the relevant data in md)
 *
 * @param md  Pointer to the main MDHIM structure
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_init(struct mdhim_t *md) {
	int ret, i;
	char filename[PATH_MAX];
	int rangesrv_num;
	int flags = MDHIM_CREATE;
	struct mdhim_store_opts_t opts;
	int path_num = 0;

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
	if (!md->db_opts->db_paths) {
		sprintf(filename, "%s%s%d", md->db_opts->db_path, md->db_opts->db_name, md->mdhim_rank);
	} else {
		path_num = rangesrv_num/((double) md->num_rangesrvs/(double) md->db_opts->num_paths);
	        path_num = path_num >= md->db_opts->num_paths ? md->db_opts->num_paths - 1 : path_num;
		if (path_num < 0) {
			sprintf(filename, "%s%s%d", md->db_opts->db_path, md->db_opts->db_name, md->mdhim_rank);
		} else {
			sprintf(filename, "%s%s%d", md->db_opts->db_paths[path_num], 
				md->db_opts->db_name, md->mdhim_rank);
		}
	}

	//Read in the manifest file if the rangesrv_num is 1
	if (md->mdhim_rs->info.rangesrv_num == 1 && 
	    (ret = read_manifest(md)) != MDHIM_SUCCESS) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - " 
		     "Fatal error: There was a problem reading or validating the manifest file",
		     md->mdhim_rank);
		MPI_Abort(md->mdhim_comm, 0);
	}
        
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
	md->mdhim_rs->mdhim_store->db_ptr5 = opts.db_ptr5;
	md->mdhim_rs->mdhim_store->db_ptr6 = opts.db_ptr6;
	md->mdhim_rs->mdhim_store->db_ptr7 = opts.db_ptr7;
	md->mdhim_rs->mdhim_store->db_ptr8 = opts.db_ptr8;

	//Load the stats from the database
	if ((ret = load_stats(md)) != MDHIM_SUCCESS) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - " 
		     "Error while loading stats", 
		     md->mdhim_rank);
	}

	//Initialize variables for printing out timings
	md->mdhim_rs->put_time = 0;
	md->mdhim_rs->get_time = 0;
	md->mdhim_rs->num_put = 0;
	md->mdhim_rs->num_get = 0;
	//Initialize work queue
	md->mdhim_rs->work_queue = malloc(sizeof(work_queue_t));
	md->mdhim_rs->work_queue->head = NULL;
	md->mdhim_rs->work_queue->tail = NULL;

	//Initialize the outstanding request list
	md->mdhim_rs->out_req_list = NULL;

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
	
	//Initialize worker threads
	md->mdhim_rs->workers = malloc(sizeof(pthread_t *) * md->db_opts->num_wthreads);
	for (i = 0; i < md->db_opts->num_wthreads; i++) {
		md->mdhim_rs->workers[i] = malloc(sizeof(pthread_t));
		if ((ret = pthread_create(md->mdhim_rs->workers[i], NULL, 
					  worker_thread, (void *) md)) != 0) {    
			mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - " 
			     "Error while initializing worker thread", 
			     md->mdhim_rank);
			return MDHIM_ERROR;
		}
	}

	//Initialize listener threads
	if ((ret = pthread_create(&md->mdhim_rs->listener, NULL, 
				  listener_thread, (void *) md)) != 0) {
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
	int ret, server;
	int comm_size, size;
	MPI_Comm new_comm;

	ranks = NULL;
	//Populate the ranks array that will be in our new comm
	if ((ret = im_range_server(md)) == 1) {
		ranks = malloc(sizeof(int) * md->num_rangesrvs);
		rp = md->rangesrvs;
		size = 0;
		while (rp) {
			ranks[i] = rp->rank;
			i++;
			size++;
			rp = rp->next;
		}
	} else {
		MPI_Comm_size(md->mdhim_comm, &comm_size);
		ranks = malloc(sizeof(int) * comm_size);
		size = j = 0;
		for (i = 0; i < comm_size; i++) {
			server = 0;
			rp = md->rangesrvs;
			while (rp) {			
				if (i == rp->rank) {
					server = 1;
					break;
				}

				rp = rp->next;
			}		

			if (server) {
				continue;
			}
			ranks[j] = i;
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
