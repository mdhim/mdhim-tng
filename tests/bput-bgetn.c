#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include "mpi.h"
#include "mdhim.h"

#define KEYS 10000
int main(int argc, char **argv) {
	int ret;
	int provided;
	int i;
	struct mdhim_t *md;
	int **keys;
	int key_lens[KEYS];
	int **values;
	int value_lens[KEYS];
	struct mdhim_brm_t *brm, *brmp;
	struct mdhim_bgetrm_t *bgrm;
	struct timeval start_tv, end_tv;
	char     *db_path = "./";
	char     *db_name = "mdhimTstDB-";
	int      dbug = MLOG_DBG; //MLOG_CRIT=1, MLOG_DBG=2
	db_options_t *db_opts; // Local variable for db create options to be passed
	int db_type = 2; //UNQLITE=1, LEVELDB=2 (data_store.h) 
	int size;

	// Create options for DB initialization
	db_opts = db_options_init();
	db_options_set_path(db_opts, db_path);
	db_options_set_name(db_opts, db_name);
	db_options_set_type(db_opts, db_type);
	db_options_set_key_type(db_opts, MDHIM_INT_KEY);
	db_options_set_debug_level(db_opts, dbug);

	//Initialize MPI with multiple thread support
	ret = MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
	if (ret != MPI_SUCCESS) {
		printf("Error initializing MPI with threads\n");
		exit(1);
	}

	//Quit if MPI didn't initialize with multiple threads
	if (provided != MPI_THREAD_MULTIPLE) {
                printf("Not able to enable MPI_THREAD_MULTIPLE mode\n");
                exit(1);
        }

	gettimeofday(&start_tv, NULL);
	//Initialize MDHIM
	md = mdhimInit(MPI_COMM_WORLD, db_opts);
	if (!md) {
		printf("Error initializing MDHIM\n");
		MPI_Abort(MPI_COMM_WORLD, ret);
		exit(1);
	}
	
	MPI_Comm_size(md->mdhim_comm, &size);	
	//Populate the keys and values to insert
	keys = malloc(sizeof(int *) * KEYS);
	values = malloc(sizeof(int *) * KEYS);
	for (i = 0; i < KEYS; i++) {
		keys[i] = malloc(sizeof(int));
		*keys[i] = size * i + md->mdhim_rank + 1;
		printf("Rank: %d - Inserting key: %d\n", md->mdhim_rank, *keys[i]);
		key_lens[i] = sizeof(int);
		values[i] = malloc(sizeof(int));
		*values[i] = md->mdhim_rank;
		value_lens[i] = sizeof(int);		
	}

	//Insert the keys into MDHIM
	brm = mdhimBPut(md, (void **) keys, key_lens,  
			(void **) values, value_lens, KEYS);
	brmp = brm;
	while (brmp) {
		if (brmp->error < 0) {
			printf("Rank: %d - Error inserting key/values info MDHIM\n", md->mdhim_rank);
		}
	
		brmp = brmp->next;
		//Free the message
		mdhim_full_release_msg(brm);
		brm = brmp;
	}
	

	//Commit the database
	ret = mdhimCommit(md);
	if (ret != MDHIM_SUCCESS) {
		printf("Error committing MDHIM database\n");
	} else {
		printf("Committed MDHIM database\n");
	}
	//Get the stats
	ret = mdhimStatFlush(md);
	if (ret != MDHIM_SUCCESS) {
		printf("Error getting stats from MDHIM database\n");
	} else {
		printf("Got stats\n");
	}

	//Get the values back for each key inserted

	//start at the first key
	if (*keys[0] == 1) {
		bgrm = mdhimBGetOp(md, keys[0], key_lens[0], 
				   KEYS, MDHIM_GET_FIRST);
	} else {
		bgrm = mdhimBGetOp(md, keys[0], key_lens[0], 
				   KEYS, MDHIM_GET_NEXT);
	}
	if (!bgrm || bgrm->error) {
		printf("Rank: %d - Error retrieving values", md->mdhim_rank);
		goto done;
	}
	
	for (i = 0; i < bgrm->num_records && !bgrm->error; i++) {
		
		printf("Rank: %d - Got key: %d value: %d\n", md->mdhim_rank, 
		       *(int *)bgrm->keys[i], *(int *)bgrm->values[i]);
	}
	
	//Free the message received
	mdhim_full_release_msg(bgrm);


	

done:
	//Quit MDHIM
	ret = mdhimClose(md);
	gettimeofday(&end_tv, NULL);
	if (ret != MDHIM_SUCCESS) {
		printf("Error closing MDHIM\n");
	}
	
	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();
	for (i = 0; i < KEYS; i++) {
		free(keys[i]);
		free(values[i]);
	}

	free(keys);
	free(values);

	printf("Took: %u seconds to insert and get %u keys/values\n", 
	       (unsigned int) (end_tv.tv_sec - start_tv.tv_sec), KEYS);

	return 0;
}
