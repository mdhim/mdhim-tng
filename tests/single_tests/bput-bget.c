#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include "mpi.h"
#include "mdhim.h"

#define KEYS 100
#define TOTAL 10000
int main(int argc, char **argv) {
	int ret;
	int provided;
	int i;
	struct mdhim_t *md;
	int **keys;
	int key_lens[KEYS];
	char **values;
	int value_lens[KEYS];
	int total = 0;
	struct mdhim_brm_t *brm, *brmp;
	struct mdhim_bgetrm_t *bgrm, *bgrmp;
	struct timeval start_tv, end_tv;
	char     *db_path = "/home/hugh/";
	char     *db_name = "mdhimTstDB";
	int      dbug = MLOG_CRIT; //MLOG_CRIT=1, MLOG_DBG=2
	mdhim_options_t *db_opts; // Local variable for db create options to be passed
	int db_type = LEVELDB; //(data_store.h) 
	MPI_Comm comm;
	
        // Create options for DB initialization
	db_opts = mdhim_options_init();
	mdhim_options_set_db_path(db_opts, db_path);
	mdhim_options_set_db_name(db_opts, db_name);
	mdhim_options_set_db_type(db_opts, db_type);
	mdhim_options_set_key_type(db_opts, MDHIM_INT_KEY);
	mdhim_options_set_debug_level(db_opts, dbug);

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
	comm = MPI_COMM_WORLD;
	md = mdhimInit(&comm, db_opts);
	if (!md) {
		printf("Error initializing MDHIM\n");
		MPI_Abort(MPI_COMM_WORLD, ret);
		exit(1);
	}	
	
	keys = malloc(sizeof(int *) * KEYS);
	values = malloc(sizeof(char *) * KEYS);
	while (total != TOTAL) {
		//Populate the keys and values to insert
		for (i = 0; i < KEYS; i++) {
			keys[i] = malloc(sizeof(int));
			*keys[i] = total + (i + 1) * (md->mdhim_rank + 1);
			printf("Rank: %d - Inserting key: %d\n", md->mdhim_rank, *keys[i]);
			key_lens[i] = sizeof(int);
			values[i] = malloc(1000024);
			memset(values[i], (char) i, 1000024);
			value_lens[i] = 1000024;		
		}

		//Insert the keys into MDHIM
		brm = mdhimBPut(md, (void **) keys, key_lens,  
				(void **) values, value_lens, KEYS, 
				NULL, NULL);
		brmp = brm;
                if (!brmp || brmp->error) {
                        printf("Rank - %d: Error inserting keys/values into MDHIM\n", md->mdhim_rank);
                } 
		while (brmp) {
			if (brmp->error < 0) {
				printf("Rank: %d - Error inserting key/values info MDHIM\n", md->mdhim_rank);
			}
	
			brmp = brmp->next;
			//Free the message
			mdhim_full_release_msg(brm);
			brm = brmp;
		}
	        for (i = 0; i < KEYS; i++) {
	   	     free(keys[i]);
	   	     free(values[i]);
	        }

		total += KEYS;
	}

	//Commit the database
	ret = mdhimCommit(md, md->primary_index);
	if (ret != MDHIM_SUCCESS) {
		printf("Error committing MDHIM database\n");
	} else {
		printf("Committed MDHIM database\n");
	}

	total = 0;
	while (total != TOTAL) {
		//Get the values back for each key inserted
		for (i = 0; i < KEYS; i++) {
			keys[i] = malloc(sizeof(int));
			*keys[i] = total + (i + 1) * (md->mdhim_rank + 1);
			key_lens[i] = sizeof(int);
		}

		bgrm = mdhimBGet(md, md->primary_index, (void **) keys, key_lens, 
				 KEYS, MDHIM_GET_EQ);
		bgrmp = bgrm;
		while (bgrmp) {
			if (bgrmp->error < 0) {
				printf("Rank: %d - Error retrieving values", md->mdhim_rank);
			}

			for (i = 0; i < bgrmp->num_keys && bgrmp->error >= 0; i++) {
		
				printf("Rank: %d - Got key: %d\n", md->mdhim_rank, 
				       *(int *)bgrmp->keys[i]);
			}

			bgrmp = bgrmp->next;
			//Free the message received
			mdhim_full_release_msg(bgrm);
			bgrm = bgrmp;
		}

	        for (i = 0; i < KEYS; i++) {
	   	     free(keys[i]);
	        }
		total += KEYS;
	}

	//Quit MDHIM
	ret = mdhimClose(md);
	gettimeofday(&end_tv, NULL);
	if (ret != MDHIM_SUCCESS) {
		printf("Error closing MDHIM\n");
	}
	
	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();

	free(keys);
	free(values);

	printf("Took: %u seconds to insert and get %u keys/values\n", 
	       (unsigned int) (end_tv.tv_sec - start_tv.tv_sec), TOTAL);

	return 0;
}
