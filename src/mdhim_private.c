#include <stdlib.h>
#include "mdhim.h"
#include "client.h"
#include "local_client.h"
#include "partitioner.h"
#include "indexes.h"

struct mdhim_rm_t *_put_record(struct mdhim_t *md, struct index_t *index, 
			       void *key, int key_len, 
			       void *value, int value_len) {
	struct mdhim_rm_t *rm = NULL;
	rangesrv_list *rl, *rlp;
	int ret;
	struct mdhim_putm_t *pm;
	struct index_t *lookup_index, *put_index;

	put_index = index;
	if (index->type == LOCAL_INDEX) {
		lookup_index = get_index(md, index->primary_id);
		if (!lookup_index) {
			return NULL;
		}
	} else {
		lookup_index = index;
	}

	//Get the range server this key will be sent to
	if (put_index->type == LOCAL_INDEX) {
		if ((rl = get_range_servers(md, lookup_index, value, value_len)) == 
		    NULL) {
			mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - " 
			     "Error while determining range server in mdhimBPut", 
			     md->mdhim_rank);
			return NULL;
		}
	} else {
		//Get the range server this key will be sent to
		if ((rl = get_range_servers(md, lookup_index, key, key_len)) == NULL) {
			mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - " 
			     "Error while determining range server in _put_record", 
			     md->mdhim_rank);
			return NULL;
		}
	}
	
	while (rl) {
		pm = malloc(sizeof(struct mdhim_putm_t));
		if (!pm) {
			mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - " 
			     "Error while allocating memory in _put_record", 
			     md->mdhim_rank);
			return NULL;
		}

		//Initialize the put message
		pm->mtype = MDHIM_PUT;
		pm->key = key;
		pm->key_len = key_len;
		pm->value = value;
		pm->value_len = value_len;
		pm->server_rank = rl->ri->rank;
		pm->index = put_index->id;
		pm->index_type = put_index->type;

		//Test if I'm a range server
		ret = im_range_server(put_index);

		//If I'm a range server and I'm the one this key goes to, send the message locally
		if (ret && md->mdhim_rank == pm->server_rank) {
			rm = local_client_put(md, pm);
		} else {
			//Send the message through the network as this message is for another rank
			rm = client_put(md, pm);
			free(pm);
		}

		rl = rl->next;
		rlp = rl;
		free(rlp);
	}

	return rm;
}

/* Creates a linked list of mdhim_rm_t messages */
struct mdhim_brm_t *_create_brm(struct mdhim_rm_t *rm) {
	struct mdhim_brm_t *brm;

	if (!rm) {
		return NULL;
	}

	brm = malloc(sizeof(struct mdhim_brm_t));
	memset(brm, 0, sizeof(struct mdhim_brm_t));
	brm->error = rm->error;
	brm->mtype = rm->mtype;
	brm->index = rm->index;
	brm->index_type = rm->index_type;
	brm->server_rank = rm->server_rank;

	return brm;
}

/* adds new to the list pointed to by head */
void _concat_brm(struct mdhim_brm_t *head, struct mdhim_brm_t *new) {
	struct mdhim_brm_t *brmp;

	brmp = head;
	while (brmp->next) {
		brmp = brmp->next;
	}

	brmp->next = new;

	return;
}

struct mdhim_brm_t *_bput_records(struct mdhim_t *md, struct index_t *index, 
				  void **keys, int *key_lens, 
				  void **values, int *value_lens, 
				  int num_keys) {
	struct mdhim_bputm_t **bpm_list, *lbpm;
	struct mdhim_bputm_t *bpm;
	struct mdhim_brm_t *brm, *brm_head;
	struct mdhim_rm_t *rm;
	int i;
	rangesrv_list *rl, *rlp;
	struct index_t *lookup_index, *put_index;

	put_index = index;
	if (index->type == LOCAL_INDEX) {
		lookup_index = get_index(md, index->primary_id);
		if (!lookup_index) {
			return NULL;
		}
	} else {
		lookup_index = index;
	}

	//Check to see that we were given a sane amount of records
	if (num_keys > MAX_BULK_OPS) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - " 
		     "To many bulk operations requested in mdhimBGetOp", 
		     md->mdhim_rank);
		return NULL;
	}

	//The message to be sent to ourselves if necessary
	lbpm = NULL;
	//Create an array of bulk put messages that holds one bulk message per range server
	bpm_list = malloc(sizeof(struct mdhim_bputm_t *) * lookup_index->num_rangesrvs);

	//Initialize the pointers of the list to null
	for (i = 0; i < lookup_index->num_rangesrvs; i++) {
		bpm_list[i] = NULL;
	}

	/* Go through each of the records to find the range server(s) the record belongs to.
	   If there is not a bulk message in the array for the range server the key belongs to, 
	   then it is created.  Otherwise, the data is added to the existing message in the array.*/
	for (i = 0; i < num_keys && i < MAX_BULK_OPS; i++) {
		//Get the range server this key will be sent to
		if (put_index->type == LOCAL_INDEX) {
			if ((rl = get_range_servers(md, lookup_index, values[i], value_lens[i])) == 
			    NULL) {
				mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - " 
				     "Error while determining range server in mdhimBPut", 
				     md->mdhim_rank);
				continue;
			}
		} else {
			if ((rl = get_range_servers(md, lookup_index, keys[i], key_lens[i])) == 
			    NULL) {
				mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - " 
				     "Error while determining range server in mdhimBPut", 
				     md->mdhim_rank);
				continue;
			}
		}
       
		//There could be more than one range server returned in the case of the local index
		while (rl) {
			if (rl->ri->rank != md->mdhim_rank) {
				//Set the message in the list for this range server
				bpm = bpm_list[rl->ri->rangesrv_num - 1];
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
				bpm->num_keys = 0;
				bpm->server_rank = rl->ri->rank;
				bpm->mtype = MDHIM_BULK_PUT;
				bpm->index = put_index->id;
				bpm->index_type = put_index->type;
				if (rl->ri->rank != md->mdhim_rank) {
					bpm_list[rl->ri->rangesrv_num - 1] = bpm;
				} else {
					lbpm = bpm;
				}
			}
		
			//Add the key, lengths, and data to the message
			bpm->keys[bpm->num_keys] = keys[i];
			bpm->key_lens[bpm->num_keys] = key_lens[i];
			bpm->values[bpm->num_keys] = values[i];
			bpm->value_lens[bpm->num_keys] = value_lens[i];
			bpm->num_keys++;
			rlp = rl;
			rl = rl->next;
			free(rlp);
		}	
	}

	//Make a list out of the received messages to return
	brm_head = client_bput(md, put_index, bpm_list);
	if (lbpm) {
		rm = local_client_bput(md, lbpm);
                if (rm) {
			brm = _create_brm(rm);
                        brm->next = brm_head;
                        brm_head = brm;
                        free(rm);
                }
	}
	
	//Free up messages sent
	for (i = 0; i < lookup_index->num_rangesrvs; i++) {
		if (!bpm_list[i]) {
			continue;
		}
			
		free(bpm_list[i]->keys);
		free(bpm_list[i]->values);
		free(bpm_list[i]->key_lens);
		free(bpm_list[i]->value_lens);
		free(bpm_list[i]);
	}

	free(bpm_list);

	//Return the head of the list
	return brm_head;
}

struct mdhim_bgetrm_t *_bget_records(struct mdhim_t *md, struct index_t *index,
				     void **keys, int *key_lens, 
				     int num_keys, int num_records, int op) {
	struct mdhim_bgetm_t **bgm_list;
	struct mdhim_bgetm_t *bgm, *lbgm;
	struct mdhim_bgetrm_t *bgrm_head, *lbgrm;
	int i;
	rangesrv_list *rl = NULL, *rlp;

	//The message to be sent to ourselves if necessary
	lbgm = NULL;
	//Create an array of bulk get messages that holds one bulk message per range server
	bgm_list = malloc(sizeof(struct mdhim_bgetm_t *) * index->num_rangesrvs);
	//Initialize the pointers of the list to null
	for (i = 0; i < index->num_rangesrvs; i++) {
		bgm_list[i] = NULL;
	}

	/* Go through each of the records to find the range server the record belongs to.
	   If there is not a bulk message in the array for the range server the key belongs to, 
	   then it is created.  Otherwise, the data is added to the existing message in the array.*/
	for (i = 0; i < num_keys && i < MAX_BULK_OPS; i++) {
		//Get the range server this key will be sent to
		if ((op == MDHIM_GET_EQ || op == MDHIM_GET_PRIMARY_EQ) && 
		    index->type != LOCAL_INDEX &&
		    (rl = get_range_servers(md, index, keys[i], key_lens[i])) == 
		    NULL) {
			mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - " 
			     "Error while determining range server in mdhimBget", 
			     md->mdhim_rank);
			free(bgm_list);
			return NULL;
		} else if ((index->type == LOCAL_INDEX || 
			   (op != MDHIM_GET_EQ && op != MDHIM_GET_PRIMARY_EQ)) &&
			   (rl = get_range_servers_from_stats(md, index, keys[i], key_lens[i], op)) == 
			   NULL) {
			mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - " 
			     "Error while determining range server in mdhimBget", 
			     md->mdhim_rank);
			free(bgm_list);
			return NULL;
		}	   	

		while (rl) {
			if (rl->ri->rank != md->mdhim_rank) {
				//Set the message in the list for this range server
				bgm = bgm_list[rl->ri->rangesrv_num - 1];
			} else {
				//Set the local message
				bgm = lbgm;
			}

			//If the message doesn't exist, create one
			if (!bgm) {
				bgm = malloc(sizeof(struct mdhim_bgetm_t));			       
				bgm->keys = malloc(sizeof(void *) * MAX_BULK_OPS);
				bgm->key_lens = malloc(sizeof(int) * MAX_BULK_OPS);
				bgm->num_keys = 0;
				bgm->num_recs = num_records;
				bgm->server_rank = rl->ri->rank;
				bgm->mtype = MDHIM_BULK_GET;
				bgm->op = (op == MDHIM_GET_PRIMARY_EQ) ? MDHIM_GET_EQ : op;
				bgm->index = index->id;
				bgm->index_type = index->type;
				if (rl->ri->rank != md->mdhim_rank) {
					bgm_list[rl->ri->rangesrv_num - 1] = bgm;
				} else {
					lbgm = bgm;
				}
			}
		
			//Add the key, lengths, and data to the message
			bgm->keys[bgm->num_keys] = keys[i];
			bgm->key_lens[bgm->num_keys] = key_lens[i];
			bgm->num_keys++;	
			rlp = rl;
			rl = rl->next;
			free(rlp);
		}
	}

	//Make a list out of the received messages to return
	bgrm_head = client_bget(md, index, bgm_list);
	if (lbgm) {
		lbgrm = local_client_bget(md, lbgm);
		lbgrm->next = bgrm_head;
		bgrm_head = lbgrm;
	}
	
	for (i = 0; i < index->num_rangesrvs; i++) {
		if (!bgm_list[i]) {
			continue;
		}

		free(bgm_list[i]->keys);
		free(bgm_list[i]->key_lens);
		free(bgm_list[i]);
	}

	free(bgm_list);

	return bgrm_head;
}

/**
 * Deletes multiple records from MDHIM
 *
 * @param md main MDHIM struct
 * @param keys         pointer to array of keys to delete
 * @param key_lens     array with lengths of each key in keys
 * @param num_keys  the number of keys to delete (i.e., the number of keys in keys array)
 * @return mdhim_brm_t * or NULL on error
 */
struct mdhim_brm_t *_bdel_records(struct mdhim_t *md, struct index_t *index,
				  void **keys, int *key_lens,
				  int num_keys) {
	struct mdhim_bdelm_t **bdm_list;
	struct mdhim_bdelm_t *bdm, *lbdm;
	struct mdhim_brm_t *brm, *brm_head;
	struct mdhim_rm_t *rm;
	int i;
	rangesrv_list *rl;


	//The message to be sent to ourselves if necessary
	lbdm = NULL;
	//Create an array of bulk del messages that holds one bulk message per range server
	bdm_list = malloc(sizeof(struct mdhim_bdelm_t *) * index->num_rangesrvs);
	//Initialize the pointers of the list to null
	for (i = 0; i < index->num_rangesrvs; i++) {
		bdm_list[i] = NULL;
	}

	/* Go through each of the records to find the range server the record belongs to.
	   If there is not a bulk message in the array for the range server the key belongs to, 
	   then it is created.  Otherwise, the data is added to the existing message in the array.*/
	for (i = 0; i < num_keys && i < MAX_BULK_OPS; i++) {
		//Get the range server this key will be sent to
		if ((rl = get_range_servers(md, index, keys[i], key_lens[i])) == 
		    NULL) {
			mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - " 
			     "Error while determining range server in mdhimBdel", 
			     md->mdhim_rank);
			continue;
		}
       
		if (rl->ri->rank != md->mdhim_rank) {
			//Set the message in the list for this range server
			bdm = bdm_list[rl->ri->rangesrv_num - 1];
		} else {
			//Set the local message
			bdm = lbdm;
		}

		//If the message doesn't exist, create one
		if (!bdm) {
			bdm = malloc(sizeof(struct mdhim_bdelm_t));			       
			bdm->keys = malloc(sizeof(void *) * MAX_BULK_OPS);
			bdm->key_lens = malloc(sizeof(int) * MAX_BULK_OPS);
			bdm->num_keys = 0;
			bdm->server_rank = rl->ri->rank;
			bdm->mtype = MDHIM_BULK_DEL;
			bdm->index = index->id;
			bdm->index_type = index->type;
			if (rl->ri->rank != md->mdhim_rank) {
				bdm_list[rl->ri->rangesrv_num - 1] = bdm;
			} else {
				lbdm = bdm;
			}
		}

		//Add the key, lengths, and data to the message
		bdm->keys[bdm->num_keys] = keys[i];
		bdm->key_lens[bdm->num_keys] = key_lens[i];
		bdm->num_keys++;		
	}

	//Make a list out of the received messages to return
	brm_head = client_bdelete(md, index, bdm_list);
	if (lbdm) {
		rm = local_client_bdelete(md, lbdm);
		brm = malloc(sizeof(struct mdhim_brm_t));
		brm->error = rm->error;
		brm->mtype = rm->mtype;
		brm->index = rm->index;
		brm->index_type = rm->index_type;
		brm->server_rank = rm->server_rank;
		brm->next = brm_head;
		brm_head = brm;
		free(rm);	
	}
	
	for (i = 0; i < index->num_rangesrvs; i++) {
		if (!bdm_list[i]) {
			continue;
		}

		free(bdm_list[i]->keys);
		free(bdm_list[i]->key_lens);
		free(bdm_list[i]);
	}

	free(bdm_list);

	//Return the head of the list
	return brm_head;
}
