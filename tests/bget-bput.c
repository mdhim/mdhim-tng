#include <stdio.h>
#include <stdlib.h>
#include <time.h>  
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
	int key_types[KEYS];
	int **values;
	int value_lens[KEYS];
	struct mdhim_brm_t *brm;
	struct mdhim_bgetrm_t *bgrm;

	ret = MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
	if (ret != MPI_SUCCESS) {
		printf("Error initializing MPI with threads\n");
		exit(1);
	}

	if (provided != MPI_THREAD_MULTIPLE) {
                printf("Not able to enable MPI_THREAD_MULTIPLE mode\n");
                exit(1);
        }

	md = mdhimInit(MPI_COMM_WORLD);
	if (!md) {
		printf("Error initializing MDHIM\n");
		exit(1);
	}	
	
	srand(time(NULL));	
	keys = malloc(sizeof(int *) * KEYS);
        values = malloc(sizeof(int *) * KEYS);
	for (i = 0; i < KEYS; i++) {
		keys[i] = malloc(sizeof(int));
		*keys[i] = rand();
		key_lens[i] = sizeof(int);
		key_types[i] = MDHIM_INT_KEY;
		values[i] = malloc(sizeof(int));
		*values[i] = rand();
		value_lens[i] = sizeof(int);		
	}

	brm = mdhimBput(md, (void **) keys, key_lens, key_types, 
		       (void **) values, value_lens, KEYS);
	if (!brm || brm->error) {
		printf("Rank - %d: Error inserting keys/values into MDHIM\n", md->mdhim_rank);
	} else {
		printf("Successfully inserted keys/values into MDHIM\n");
	}

	bgrm = mdhimBGet(md, (void **) keys, key_lens, key_types, 
			 KEYS);
	if (!bgrm || bgrm->error) {
		printf("Error getting values for keys\n");
	} else {
		printf("Successfully got values for keys\n");
	}

	ret = mdhimClose(md);
	if (ret != MDHIM_SUCCESS) {
		printf("Error closing MDHIM\n");
	}

	for (i = 0; i < KEYS; i++) {
		free(keys[i]);
		free(values[i]);
	}

	free(keys);
	free(values);
	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();

	return 0;
}
