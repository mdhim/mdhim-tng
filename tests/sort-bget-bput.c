#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "mpi.h"
#include "mdhim.h"

#define KEYS 100
#define KEY_SIZE 100
#define NUM_KEYS 10000
int main(int argc, char **argv) {
	int ret;
	int provided;
	int i, j, fd;
	struct mdhim_t *md;
	void **keys;
	int key_lens[KEYS];
	char **values;
	int value_lens[KEYS];
	struct mdhim_brm_t *brm, *brmp;
	struct mdhim_bgetrm_t *bgrm, *bgrmp;
	struct timeval start_tv, end_tv;
	char filename[255];
	unsigned readtime = 0;
	unsigned writetime = 0;

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


	//Initialize MDHIM
	md = mdhimInit(MPI_COMM_WORLD, MDHIM_BYTE_KEY);
	sprintf(filename, "%s%d", "input/input", md->mdhim_rank);
	if ((fd = open(filename, O_RDONLY)) < 0) {
		printf("Error opening input file");
		exit(1);
	}
	if (!md) {
		printf("Error initializing MDHIM\n");
		MPI_Abort(MPI_COMM_WORLD, ret);
		exit(1);
	}	
	
	for (j = 0; j < NUM_KEYS; j+=KEYS) {
		//Populate the keys and values to insert
		keys = malloc(sizeof(void *) * KEYS);
		values = malloc(sizeof(char *) * KEYS);
		for (i = 0; i < KEYS; i++) {
			keys[i] = malloc(KEY_SIZE);
			ret = read(fd, keys[i], KEY_SIZE);
			if (ret != KEY_SIZE) {
				printf("Error reading in key\n");
			}

			key_lens[i] = KEY_SIZE;
			values[i] = malloc(5);
			sprintf(values[i], "%s", "5555");
			value_lens[i] = 5;		
		}

		//Insert the keys into MDHIM
		gettimeofday(&start_tv, NULL);
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

		gettimeofday(&end_tv, NULL);
		writetime +=  (unsigned int) (end_tv.tv_sec - start_tv.tv_sec);
		//Commit the database
		ret = mdhimCommit(md);
		if (ret != MDHIM_SUCCESS) {
			printf("Error committing MDHIM database\n");
		}

		//Get the values back for each key inserted
		gettimeofday(&start_tv, NULL);
		bgrm = mdhimBGet(md, (void **) keys, key_lens, 
				 KEYS);
		bgrmp = bgrm;
		while (bgrmp) {
			if (bgrmp->error < 0) {
				printf("Rank: %d - Error retrieving values", md->mdhim_rank);
			}

			/*	for (i = 0; i < bgrmp->num_records && bgrmp->error >= 0; i++) {
		
				printf("Rank: %d - Got key: %d value: %c\n", md->mdhim_rank, 
				       *(int *)bgrmp->keys[i], *(char *)bgrmp->values[i]);
			}
			*/
			bgrmp = bgrmp->next;
			//Free the message received
			mdhim_full_release_msg(bgrm);
			bgrm = bgrmp;
		}

		for (i = 0; i < KEYS; i++) {
			free(keys[i]);
			free(values[i]);
		}

		free(keys);
		free(values);
		gettimeofday(&end_tv, NULL);
		readtime +=  (unsigned int) (end_tv.tv_sec - start_tv.tv_sec);

	}
	//Quit MDHIM
	ret = mdhimClose(md);
	close(fd);
	if (ret != MDHIM_SUCCESS) {
		printf("Error closing MDHIM\n");
	}
	
	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();

	printf("Took: %u seconds to insert: %u keys/values\n", 
	      writetime, NUM_KEYS);
	printf("Took: %u seconds to read: %u keys/values\n", 
	      readtime, NUM_KEYS);
	return 0;
}
