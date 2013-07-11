#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include "mpi.h"
#include "mdhim.h"

#define KEYS 100
int main(int argc, char **argv) {
	int ret;
	int provided;
	int i;
	struct mdhim_t *md;
	int **keys;
	int key_lens[KEYS];
	int key_types[KEYS];
	int **values;
	int value_lens[KEYS];
	struct mdhim_brm_t *brm, *brmp;
	struct mdhim_bgetrm_t *bgrm, *bgrmp;
	struct timeval start_tv, end_tv;

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
	md = mdhimInit(MPI_COMM_WORLD);
	if (!md) {
		printf("Error initializing MDHIM\n");
		MPI_Abort(MPI_COMM_WORLD, ret);
		exit(1);
	}	
	
	//Populate the keys and values to insert
	keys = malloc(sizeof(int *) * KEYS);
        values = malloc(sizeof(int *) * KEYS);
	for (i = 0; i < KEYS; i++) {
		keys[i] = malloc(sizeof(int));
		*keys[i] = (i + 1) * (md->mdhim_rank + 1);
		printf("Rank: %d - Inserting key: %d\n", md->mdhim_rank, *keys[i]);
		key_lens[i] = sizeof(int);
		key_types[i] = MDHIM_INT_KEY;
		values[i] = malloc(sizeof(int));
		*values[i] = (i + 1) * (md->mdhim_rank + 1);
		value_lens[i] = sizeof(int);		
	}

	//Insert the keys into MDHIM
	brm = mdhimBPut(md, (void **) keys, key_lens, key_types, 
			(void **) values, value_lens, KEYS);
	brmp = brm;
	if (!brm || brm->error) {
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

	//Commit the database
	ret = mdhimCommit(md);
	if (ret != MDHIM_SUCCESS) {
		printf("Error committing MDHIM database\n");
	} else {
		printf("Committed MDHIM database\n");
	}

	//Get the values back for each key inserted
	bgrm = mdhimBGet(md, (void **) keys, key_lens, key_types, 
			 KEYS);
	bgrmp = bgrm;
	while (bgrmp) {
		if (bgrmp->error < 0) {
			printf("Rank: %d - Error retrieving values", md->mdhim_rank);
		}

		for (i = 0; i < bgrmp->num_records && bgrmp->error >= 0; i++) {
		
			printf("Rank: %d - Got key: %d value: %d\n", md->mdhim_rank, 
			       *(int *)bgrmp->keys[i], *(int *)bgrmp->values[i]);
		}

		bgrmp = bgrmp->next;
		//Free the message received
		mdhim_full_release_msg(bgrm);
		bgrm = bgrmp;
	}

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
