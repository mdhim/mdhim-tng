#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include "mpi.h"
#include "mdhim.h"

#define KEYS 50
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
	struct mdhim_bgetrm_t *bgrm, *bgrmp;

	//Initialize MPI
	ret = MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
	if (ret != MPI_SUCCESS) {
		printf("Error initializing MPI with threads\n");
		exit(1);
	}

	//Quit if the MPI doesn't support multiple thread mode
	if (provided != MPI_THREAD_MULTIPLE) {
                printf("Not able to enable MPI_THREAD_MULTIPLE mode\n");
                exit(1);
        }

	//Initialize MDHIM
	md = mdhimInit(MPI_COMM_WORLD, MDHIM_INT_KEY);
	if (!md) {
		printf("Error initializing MDHIM\n");
		MPI_Abort(MPI_COMM_WORLD, ret);
		exit(1);
	}	
	
	keys = malloc(sizeof(int *) * KEYS);
        values = malloc(sizeof(int *) * KEYS);
	for (i = 0; i < KEYS; i++) {
		keys[i] = malloc(sizeof(int));
		*keys[i] = (i + 1) * (md->mdhim_rank + 1);
		printf("Rank: %d - Inserting key: %d\n", md->mdhim_rank, *keys[i]);
		key_lens[i] = sizeof(int);
		values[i] = malloc(sizeof(int));
		*values[i] = (i + 1) * (md->mdhim_rank + 1);
		value_lens[i] = sizeof(int);		
	}

	//Insert the records
	brm = mdhimBPut(md, (void **) keys, key_lens,  
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

	//Delete the records
	brm = mdhimBDelete(md, (void **) keys, key_lens, 
			   KEYS);
	brmp = brm;
	if (!brm || brm->error) {
		printf("Rank - %d: Error deleting keys/values from MDHIM\n", md->mdhim_rank);
	} 
	while (brmp) {
		if (brmp->error < 0) {
			printf("Rank: %d - Error deleting keys\n", md->mdhim_rank);
		}
	
		brmp = brmp->next;
		//Free the message
		mdhim_full_release_msg(brm);
		brm = brmp;
	}

	//Try to get the records back - should fail
	bgrm = mdhimBGet(md, (void **) keys, key_lens, 
			 KEYS);
	bgrmp = bgrm;
	while (bgrmp) {
		if (bgrmp->error < 0) {
			printf("Rank: %d - Error retrieving values\n", md->mdhim_rank);
		}

		for (i = 0; i < bgrmp->num_records && bgrmp->error >= 0; i++) {
		
			printf("Rank: %d - Got key: %d value: %d\n", md->mdhim_rank, 
			       *(int *)bgrmp->keys[i], *(int *)bgrmp->values[i]);
		}

		bgrmp = bgrmp->next;
		//Free the message
		mdhim_full_release_msg(bgrm);
		bgrm = bgrmp;
	}

	ret = mdhimClose(md);
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

	return 0;
}
