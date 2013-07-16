#include <stdio.h>
#include <stdlib.h>
#include "mpi.h"
#include "mdhim.h"

int main(int argc, char **argv) {
	int ret;
	int provided = 0;
	struct mdhim_t *md;
	int key;
	int value;
	struct mdhim_rm_t *rm;
	struct mdhim_getrm_t *grm;
	int i;
	int keys_per_rank = 5;

	ret = MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
	if (ret != MPI_SUCCESS) {
		printf("Error initializing MPI with threads\n");
		exit(1);
	}

	if (provided != MPI_THREAD_MULTIPLE) {
                printf("Not able to enable MPI_THREAD_MULTIPLE mode\n");
                exit(1);
        }

	md = mdhimInit(MPI_COMM_WORLD, MDHIM_INT_KEY);
	if (!md) {
		printf("Error initializing MDHIM\n");
		exit(1);
	}	

	//Put the keys and values
	for (i = 0; i < keys_per_rank; i++) {
		key = keys_per_rank * md->mdhim_rank + i;
		value = md->mdhim_rank + i;
		rm = mdhimPut(md, &key, sizeof(key), 
			      &value, sizeof(value));
		if (!rm || rm->error) {
			printf("Error inserting key/value into MDHIM\n");
		} else {
			printf("Rank: %d put key: %d with value: %d\n", md->mdhim_rank, key, value);
		}
	}

	//Commit the database
	ret = mdhimCommit(md);
	if (ret != MDHIM_SUCCESS) {
		printf("Error committing MDHIM database\n");
	} else {
		printf("Committed MDHIM database\n");
	}

	//Get the values using get_next
	for (i = 0; i < keys_per_rank; i++) {
		value = 0;
		key = keys_per_rank * md->mdhim_rank + i - 1;
		grm = mdhimGet(md, &key, sizeof(key), MDHIM_GET_NEXT);				
		if (!grm || grm->error) {
			printf("Error getting value for key: %d from MDHIM\n", key);
		} else {
			printf("Rank: %d successfully got key: %d with value: %d from MDHIM\n", 
			       md->mdhim_rank,
			       *((int *) grm->key),
			       *((int *) grm->value));
		}
	}

	ret = mdhimClose(md);
	if (ret != MDHIM_SUCCESS) {
		printf("Error closing MDHIM\n");
	}

	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();

	return 0;
}
