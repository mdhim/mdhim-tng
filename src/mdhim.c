/*
 * MDHIM TNG
 * 
 * MDHIM API implementation
 */

#include <stdlib.h>
#include "mdhim.h"
#include "range_server.h"
#include "client.h"
#include "local_client.h"
#include "partitioner.h"
#include "db_options.h"

/*! \mainpage MDHIM TNG
 *
 * \section intro_sec Introduction
 *
 * MDHIM TNG is a key/value store for HPC
 *
 */


/**
 * mdhimInit
 * Initializes MDHIM - Collective call
 *
 * @param appComm  the communicator that was passed in from the application (e.g., MPI_COMM_WORLD)
 * @param opts Options structure for DB creation, such as name, and primary key type
 * @return mdhim_t* that contains info about this instance or NULL if there was an error
 */
struct mdhim_t *mdhimInit(MPI_Comm appComm, struct db_options_t *opts) {
	int ret;
	struct mdhim_t *md;
	struct rangesrv_info *rangesrvs;

	//Open mlog - stolen from plfs
	ret = mlog_open((char *)"mdhim", 0,
			MLOG_CRIT, MLOG_CRIT, NULL, 0, MLOG_LOGPID, 0);

	//Allocate memory for the main MDHIM structure
	md = malloc(sizeof(struct mdhim_t));
	memset(md, 0, sizeof(struct mdhim_t));
	if (!md) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM - Error while allocating memory while initializing");
		return NULL;
	}

	//Set the key type for this database from the options passed
        md->db_opts = opts;
	md->key_type = opts->db_key_type;
	if (md->key_type < MDHIM_INT_KEY || md->key_type > MDHIM_BYTE_KEY) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM - Invalid key type specified");
		return NULL;
	}

	//Dup the communicator passed in 
	if ((ret = MPI_Comm_dup(appComm, &md->mdhim_comm)) != MPI_SUCCESS) {
		mlog(MDHIM_CLIENT_CRIT, "Error while initializing the MDHIM communicator");
		return NULL;
	}
  
	//Get our rank in the main MDHIM communicator
	if ((ret = MPI_Comm_rank(md->mdhim_comm, &md->mdhim_rank)) != MPI_SUCCESS) {
		mlog(MDHIM_CLIENT_CRIT, "Error getting our rank while initializing MDHIM");
		return NULL;
	}
 
        //Get the size of the main MDHIM communicator
	if ((ret = MPI_Comm_size(md->mdhim_comm, &md->mdhim_comm_size)) != MPI_SUCCESS) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error getting the size of the " 
		     "comm while initializing", 
		     md->mdhim_rank);
		return NULL;
	}

	if (md->mdhim_comm_size < 2) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - The communicator size must be 2" 
		     " or greater", md->mdhim_rank);
		return NULL;
	}

	//Initialize receive msg mutex - used for receiving a message from myself
	md->receive_msg_mutex = malloc(sizeof(pthread_mutex_t));
	if (!md->receive_msg_mutex) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - " 
		     "Error while allocating memory for client", 
		     md->mdhim_rank);
		return NULL;
	}
	if ((ret = pthread_mutex_init(md->receive_msg_mutex, NULL)) != 0) {    
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - " 
		     "Error while initializing receive queue mutex", md->mdhim_rank);
		return NULL;
	}
	//Initialize the receive condition variable - used for receiving a message from myself
	md->receive_msg_ready_cv = malloc(sizeof(pthread_cond_t));
	if (!md->receive_msg_ready_cv) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - " 
		     "Error while allocating memory for client", 
		     md->mdhim_rank);
		return NULL;
	}
	if ((ret = pthread_cond_init(md->receive_msg_ready_cv, NULL)) != 0) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - " 
		     "Error while initializing client receive condition variable", 
		     md->mdhim_rank);
		return NULL;
	}

	//Initialize the partitioner
	partitioner_init(md);

	//Start range server if I'm a range server
	if ((ret = range_server_init(md)) != MDHIM_SUCCESS) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - Error initializing MDHIM range server", 
		     md->mdhim_rank);
		return NULL;
	}

	//Get a list of all the range servers including myself if I am one
	if (!(rangesrvs = get_rangesrvs(md))) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Did not receive any range servers" 
		     "while initializing", md->mdhim_rank);
		return NULL;
	}

	//Set the range server list in the md struct
	md->rangesrvs = rangesrvs;

	//Set the receive queue to NULL 
	md->receive_msg = NULL;

	MPI_Barrier(md->mdhim_comm);
	return md;
}

/**
 * Quits the MDHIM instance - collective call
 *
 * @param md main MDHIM struct
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int mdhimClose(struct mdhim_t *md) {
	int ret;
	struct rangesrv_info *rsrv, *trsrv;
	struct mdhim_basem_t *cm;

	MPI_Barrier(md->mdhim_comm);
	//If I'm rank 0, send a close message to every range server to it can stop its thread
	if (!md->mdhim_rank) {
		cm = malloc(sizeof(struct mdhim_basem_t));
		cm->mtype = MDHIM_CLOSE;
		client_close(md, cm);
		free(cm);
	}

	//Stop range server if I'm a range server	
	if (im_range_server(md) && (ret = range_server_stop(md)) != MDHIM_SUCCESS) {
		return MDHIM_ERROR;
	}

	//Free up memory used by the partitioner
	partitioner_release();

	//Free up the range server list
	rsrv = md->rangesrvs;
	while (rsrv) {
		trsrv = rsrv->next;
		free(rsrv);
		rsrv = trsrv;
	}

	//Destroy the receive condition variable
	if ((ret = pthread_cond_destroy(md->receive_msg_ready_cv)) != 0) {
		return MDHIM_ERROR;
	}
	free(md->receive_msg_ready_cv);

	//Destroy the receive mutex
	if ((ret = pthread_mutex_destroy(md->receive_msg_mutex)) != 0) {
		return MDHIM_ERROR;
	}
	free(md->receive_msg_mutex);
       	MPI_Barrier(md->mdhim_comm);

	return MDHIM_SUCCESS;
}

/**
 * Commits outstanding MDHIM writes - collective call
 *
 * @param md main MDHIM struct
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int mdhimCommit(struct mdhim_t *md) {
	int ret = MDHIM_SUCCESS;
	struct mdhim_basem_t *cm;
	struct mdhim_rm_t *rm = NULL;
	int rs = 0;

	MPI_Barrier(md->mdhim_comm);      
	//If I'm a range server, send a commit message to myself
	if ((rs = im_range_server(md)) == 1) {       
		cm = malloc(sizeof(struct mdhim_basem_t));
		cm->mtype = MDHIM_COMMIT;
		rm = local_client_commit(md, cm);
		if (!rm || rm->error) {
			ret = MDHIM_ERROR;
			mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - " 
			     "Error while committing database in mdhimCommit",
			     md->mdhim_rank);
		}

		if (rm) {
			free(rm);
		}
		free(cm);
	}

	MPI_Barrier(md->mdhim_comm);      

	return ret;
}

/**
 * Inserts a single record into MDHIM
 *
 * @param md main MDHIM struct
 * @param key       pointer to key to store
 * @param key_len   the length of the key
 * @param value     pointer to the value to store
 * @param value_len the length of the value
 * @return mdhim_rm_t * or NULL on error
 */
struct mdhim_rm_t *mdhimPut(struct mdhim_t *md, void *key, int key_len,  
			    void *value, int value_len) {
	int ret;
	struct mdhim_putm_t *pm;
	struct mdhim_rm_t *rm = NULL;
	rangesrv_info *ri;

	//Get the range server this key will be sent to
	if ((ri = get_range_server(md, key, key_len)) == NULL) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - " 
		     "Error while determining range server in mdhimPut", 
		     md->mdhim_rank);
		return NULL;
	}
	
	mlog(MDHIM_CLIENT_DBG, "MDHIM Rank: %d - " 
	     "Sending put request for key: %d to rank: %d", 
	     md->mdhim_rank, *(int *)key, ri->rank);
	pm = malloc(sizeof(struct mdhim_putm_t));
	if (!pm) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - " 
		     "Error while allocating memory in mdhimPut", 
		     md->mdhim_rank);
		return NULL;
	}

	//Initialize the put message
	pm->mtype = MDHIM_PUT;
	pm->key = key;
	pm->key_len = key_len;
	pm->value = value;
	pm->value_len = value_len;
	pm->server_rank = ri->rank;
	
	//Test if I'm a range server
	ret = im_range_server(md);

	//If I'm a range server and I'm the one this key goes to, send the message locally
	if (ret && md->mdhim_rank == pm->server_rank) {
		rm = local_client_put(md, pm);
	} else {
		//Send the message through the network as this message is for another rank
		rm = client_put(md, pm);
		free(pm);
	}

	return rm;
}

/**
 * Inserts multiple records into MDHIM
 * 
 * @param md main MDHIM struct
 * @param keys         pointer to array of keys to store
 * @param key_lens     array with lengths of each key in keys
 * @param values       pointer to array of values to store
 * @param value_lens   array with lengths of each value
 * @param num_records  the number of records to store (i.e., the number of keys in keys array)
 * @return mdhim_brm_t * or NULL on error
 */
struct mdhim_brm_t *mdhimBPut(struct mdhim_t *md, void **keys, int *key_lens, 
			      void **values, int *value_lens, int num_records) {
	struct mdhim_bputm_t **bpm_list;
	struct mdhim_bputm_t *bpm, *lbpm;
	struct mdhim_brm_t *brm, *brm_head;
	struct mdhim_rm_t *rm;
	int i;
	rangesrv_info *ri;

	//The message to be sent to ourselves if necessary
	lbpm = NULL;
	//Create an array of bulk put messages that holds one bulk message per range server
	bpm_list = malloc(sizeof(struct mdhim_bputm_t *) * md->num_rangesrvs);
	//Initialize the pointers of the list to null
	for (i = 0; i < md->num_rangesrvs; i++) {
		bpm_list[i] = NULL;
	}

	/* Go through each of the records to find the range server the record belongs to.
	   If there is not a bulk message in the array for the range server the key belongs to, 
	   then it is created.  Otherwise, the data is added to the existing message in the array.*/
	for (i = 0; i < num_records && i < MAX_BULK_OPS; i++) {
		//Get the range server this key will be sent to
		if ((ri = get_range_server(md, keys[i], key_lens[i])) == 
		    NULL) {
			mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - " 
			     "Error while determining range server in mdhimBput", 
			     md->mdhim_rank);
			continue;
		}
       
		if (ri->rank != md->mdhim_rank) {
			//Set the message in the list for this range server
			bpm = bpm_list[ri->rangesrv_num - 1];
		} else {
			//Set the local message
			bpm = lbpm;
		}

		//If the message doesn't exist, create one
		if (!bpm) {
			bpm = malloc(sizeof(struct mdhim_bputm_t));			       
			bpm->keys = malloc(sizeof(void *) * MAX_BULK_OPS);
			bpm->key_lens = malloc(sizeof(int) * MAX_BULK_OPS);
			bpm->values = malloc(sizeof(void *) * MAX_BULK_OPS);
			bpm->value_lens = malloc(sizeof(int) * MAX_BULK_OPS);
			bpm->num_records = 0;
			bpm->server_rank = ri->rank;
			bpm->mtype = MDHIM_BULK_PUT;
			if (ri->rank != md->mdhim_rank) {
				bpm_list[ri->rangesrv_num - 1] = bpm;
			} else {
				lbpm = bpm;
			}
		}

		//Add the key, lengths, and data to the message
		bpm->keys[bpm->num_records] = keys[i];
		bpm->key_lens[bpm->num_records] = key_lens[i];
		bpm->values[bpm->num_records] = values[i];
		bpm->value_lens[bpm->num_records] = value_lens[i];
		bpm->num_records++;		
	}

	//Make a list out of the received messages to return
	brm_head = client_bput(md, bpm_list);
	if (lbpm) {
		rm = local_client_bput(md, lbpm);
		brm = malloc(sizeof(struct mdhim_brm_t));
		brm->error = rm->error;
		brm->mtype = rm->mtype;
		brm->server_rank = rm->server_rank;
		brm->next = brm_head;
		brm_head = brm;
		free(rm);	
	}
	
	for (i = 0; i < md->num_rangesrvs; i++) {
		if (!bpm_list[i]) {
			continue;
		}

		free(bpm_list[i]);
	}

	free(bpm_list);

	//Return the head of the list
	return brm_head;
}

/**
 * Retrieves a single record from MDHIM
 *
 * @param md main MDHIM struct
 * @param key       pointer to key to get value of or last key to start from if op is 
                    (MDHIM_GET_NEXT or MDHIM_GET_PREV)
 * @param key_len   the length of the key
 * @param op        the operation type
 * @return mdhim_getrm_t * or NULL on error
 */
struct mdhim_getrm_t *mdhimGet(struct mdhim_t *md, void *key, int key_len, 
			       int op) {
	int ret;
	struct mdhim_getm_t *gm;
	struct mdhim_getrm_t *grm;
	rangesrv_info *ri;

	//Get the range server this key will be sent to
	if ((ri = get_range_server(md, key, key_len)) == NULL) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - " 
		     "Error while determining range server in mdhimGet", 
		     md->mdhim_rank);
		return NULL;
	}

	gm = malloc(sizeof(struct mdhim_getm_t));
	if (!gm) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - " 
		     "Error while allocating memory in mdhimGet", 
		     md->mdhim_rank);
		return NULL;
	}

	//Initialize the get message
	gm->mtype = MDHIM_GET;
	gm->op = op;
	gm->key = key;
	gm->key_len = key_len;
	gm->server_rank = ri->rank;
	
	mlog(MDHIM_CLIENT_DBG, "MDHIM Rank: %d - " 
	     "Sending get request for key: %d to rank: %d", 
	     md->mdhim_rank, *(int *)key, ri->rank);
	//Test if I'm a range server
	ret = im_range_server(md);

	//If I'm a range server and I'm the one this key goes to, send the message locally
	if (ret && md->mdhim_rank == gm->server_rank) {
		grm = local_client_get(md, gm);
	} else {
		//Send the message through the network as this message is for another rank
		grm = client_get(md, gm);
		free(gm);
	}

	return grm;
}

/**
 * Retrieves multiple records from MDHIM
 *
 * @param md main MDHIM struct
 * @param keys         pointer to array of keys to get values for
 * @param key_lens     array with lengths of each key in keys
 * @param num_keys     the number of keys to get (i.e., the number of keys in keys array)
 * @return mdhim_bgetrm_t * or NULL on error
 */
struct mdhim_bgetrm_t *mdhimBGet(struct mdhim_t *md, void **keys, int *key_lens, 
				 int num_records) {
	struct mdhim_bgetm_t **bgm_list;
	struct mdhim_bgetm_t *bgm, *lbgm;
	struct mdhim_bgetrm_t *bgrm_head, *lbgrm;
	int i;
	rangesrv_info *ri;

	//The message to be sent to ourselves if necessary
	lbgm = NULL;
	//Create an array of bulk get messages that holds one bulk message per range server
	bgm_list = malloc(sizeof(struct mdhim_bgetm_t *) * md->num_rangesrvs);
	//Initialize the pointers of the list to null
	for (i = 0; i < md->num_rangesrvs; i++) {
		bgm_list[i] = NULL;
	}

	/* Go through each of the records to find the range server the record belongs to.
	   If there is not a bulk message in the array for the range server the key belongs to, 
	   then it is created.  Otherwise, the data is added to the existing message in the array.*/
	for (i = 0; i < num_records && i < MAX_BULK_OPS; i++) {
		//Get the range server this key will be sent to
		if ((ri = get_range_server(md, keys[i], key_lens[i])) == 
		    NULL) {
			mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - " 
			     "Error while determining range server in mdhimBget", 
			     md->mdhim_rank);
			continue;
		}
       
		if (ri->rank != md->mdhim_rank) {
			//Set the message in the list for this range server
			bgm = bgm_list[ri->rangesrv_num - 1];
		} else {
			//Set the local message
			bgm = lbgm;
		}

		//If the message doesn't exist, create one
		if (!bgm) {
			bgm = malloc(sizeof(struct mdhim_bgetm_t));			       
			bgm->keys = malloc(sizeof(void *) * MAX_BULK_OPS);
			bgm->key_lens = malloc(sizeof(int) * MAX_BULK_OPS);
			bgm->num_records = 0;
			bgm->server_rank = ri->rank;
			bgm->mtype = MDHIM_BULK_GET;
			if (ri->rank != md->mdhim_rank) {
				bgm_list[ri->rangesrv_num - 1] = bgm;
			} else {
				lbgm = bgm;
			}
		}

		//Add the key, lengths, and data to the message
		bgm->keys[bgm->num_records] = keys[i];
		bgm->key_lens[bgm->num_records] = key_lens[i];
		bgm->num_records++;		
	}

	//Make a list out of the received messages to return
	bgrm_head = client_bget(md, bgm_list);
	if (lbgm) {
		lbgrm = local_client_bget(md, lbgm);
		lbgrm->next = bgrm_head;
		bgrm_head = lbgrm;
	}
	
	for (i = 0; i < md->num_rangesrvs; i++) {
		if (!bgm_list[i]) {
			continue;
		}

		free(bgm_list[i]);
	}

	free(bgm_list);

	//Return the head of the list
	return bgrm_head;
}

/**
 * Deletes a single record from MDHIM
 *
 * @param md main MDHIM struct
 * @param key       pointer to key to delete
 * @param key_len   the length of the key
 * @return mdhim_rm_t * or NULL on error
 */
struct mdhim_rm_t *mdhimDelete(struct mdhim_t *md, void *key, int key_len) {
	struct mdhim_delm_t *dm;
	struct mdhim_rm_t *rm = NULL;
	rangesrv_info *ri;
	int ret;

	//Get the range server this key will be sent to
	if ((ri = get_range_server(md, key, key_len)) == NULL) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - " 
		     "Error while determining range server in mdhimDel", 
		     md->mdhim_rank);
		return NULL;
	}

	dm = malloc(sizeof(struct mdhim_delm_t));
	if (!dm) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - " 
		     "Error while allocating memory in mdhimDel", 
		     md->mdhim_rank);
		return NULL;
	}

	//Initialize the del message
	dm->mtype = MDHIM_DEL;
	dm->key = key;
	dm->key_len = key_len;
	dm->server_rank = ri->rank;
	
	//Test if I'm a range server
	ret = im_range_server(md);

	//If I'm a range server and I'm the one this key goes to, send the message locally
	if (ret && md->mdhim_rank == dm->server_rank) {
		rm = local_client_delete(md, dm);
	} else {
		//Send the message through the network as this message is for another rank
		rm = client_delete(md, dm);
		free(dm);
	}

	return rm;
}

/**
 * Deletes multiple records from MDHIM
 *
 * @param md main MDHIM struct
 * @param keys         pointer to array of keys to delete
 * @param key_lens     array with lengths of each key in keys
 * @param num_keys     the number of keys to delete (i.e., the number of keys in keys array)
 * @return mdhim_brm_t * or NULL on error
 */
struct mdhim_brm_t *mdhimBDelete(struct mdhim_t *md, void **keys, int *key_lens,
				 int num_records) {
	struct mdhim_bdelm_t **bdm_list;
	struct mdhim_bdelm_t *bdm, *lbdm;
	struct mdhim_brm_t *brm, *brm_head;
	struct mdhim_rm_t *rm;
	int i;
	rangesrv_info *ri;

	//The message to be sent to ourselves if necessary
	lbdm = NULL;
	//Create an array of bulk del messages that holds one bulk message per range server
	bdm_list = malloc(sizeof(struct mdhim_bdelm_t *) * md->num_rangesrvs);
	//Initialize the pointers of the list to null
	for (i = 0; i < md->num_rangesrvs; i++) {
		bdm_list[i] = NULL;
	}

	/* Go through each of the records to find the range server the record belongs to.
	   If there is not a bulk message in the array for the range server the key belongs to, 
	   then it is created.  Otherwise, the data is added to the existing message in the array.*/
	for (i = 0; i < num_records && i < MAX_BULK_OPS; i++) {
		//Get the range server this key will be sent to
		if ((ri = get_range_server(md, keys[i], key_lens[i])) == 
		    NULL) {
			mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - " 
			     "Error while determining range server in mdhimBdel", 
			     md->mdhim_rank);
			continue;
		}
       
		if (ri->rank != md->mdhim_rank) {
			//Set the message in the list for this range server
			bdm = bdm_list[ri->rangesrv_num - 1];
		} else {
			//Set the local message
			bdm = lbdm;
		}

		//If the message doesn't exist, create one
		if (!bdm) {
			bdm = malloc(sizeof(struct mdhim_bdelm_t));			       
			bdm->keys = malloc(sizeof(void *) * MAX_BULK_OPS);
			bdm->key_lens = malloc(sizeof(int) * MAX_BULK_OPS);
			bdm->num_records = 0;
			bdm->server_rank = ri->rank;
			bdm->mtype = MDHIM_BULK_DEL;
			if (ri->rank != md->mdhim_rank) {
				bdm_list[ri->rangesrv_num - 1] = bdm;
			} else {
				lbdm = bdm;
			}
		}

		//Add the key, lengths, and data to the message
		bdm->keys[bdm->num_records] = keys[i];
		bdm->key_lens[bdm->num_records] = key_lens[i];
		bdm->num_records++;		
	}

	//Make a list out of the received messages to return
	brm_head = client_bdelete(md, bdm_list);
	if (lbdm) {
		rm = local_client_bdelete(md, lbdm);
		brm = malloc(sizeof(struct mdhim_brm_t));
		brm->error = rm->error;
		brm->mtype = rm->mtype;
		brm->server_rank = rm->server_rank;
		brm->next = brm_head;
		brm_head = brm;
		free(rm);	
	}
	
	for (i = 0; i < md->num_rangesrvs; i++) {
		if (!bdm_list[i]) {
			continue;
		}

		free(bdm_list[i]);
	}

	free(bdm_list);

	//Return the head of the list
	return brm_head;
}
