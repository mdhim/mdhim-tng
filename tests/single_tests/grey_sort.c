#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "mpi.h"
#include "mdhim.h"

#define KEYS 2000
#define KEY_SIZE 100
#define NUM_KEYS 100000
#define SLICE 100000
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
	struct mdhim_bgetrm_t *bgrm;
	char filename[255];
        mdhim_options_t *mdhim_opts;
	struct mdhim_stat *stat, *tmp;
	MPI_Comm comm;
	void *last_key;

	//Initialize MPI with multiple thread support
	ret = MPI_Init_thread(&argc, &argv, MPI_THREAD_SERIALIZED, &provided);
	if (ret != MPI_SUCCESS) {
		printf("Error initializing MPI with threads\n");
		exit(1);
	}

	//Quit if MPI didn't initialize with multiple threads
	if (provided != MPI_THREAD_SERIALIZED) {
                printf("Not able to enable MPI_THREAD_SERIALIZED mode\n");
                exit(1);
        }       

        mdhim_opts = mdhim_options_init();
        mdhim_options_set_db_path(mdhim_opts, "/panfs/pas12a/vol1/hng/mdhim/");
        mdhim_options_set_db_name(mdhim_opts, "mdhimTstDB");
        mdhim_options_set_db_type(mdhim_opts, LEVELDB);
        mdhim_options_set_key_type(mdhim_opts, MDHIM_BYTE_KEY); 
	mdhim_options_set_max_recs_per_slice(mdhim_opts, SLICE);
        mdhim_options_set_server_factor(mdhim_opts, 1);
        mdhim_options_set_debug_level(mdhim_opts, MLOG_CRIT);

	//Initialize MDHIM
	comm = MPI_COMM_WORLD;
	md = mdhimInit(&comm, mdhim_opts);
	sprintf(filename, "%s%d", "/panfs/pas12a/vol1/hng/minutesort/input", md->mdhim_rank);
	if ((fd = open(filename, O_RDONLY)) < 0) {
		printf("Error opening input file");
		exit(1);
	}
	if (!md) {
		printf("Error initializing MDHIM\n");
		MPI_Abort(MPI_COMM_WORLD, ret);
		exit(1);
	}	
	

	//Read in the keys from the files and insert them
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
			values[i] = malloc(1);
			*values[i] = 'a';
			value_lens[i] = 1;
			//			printf("Packing key: %.100s with size: %d\n", keys[i], key_lens[i]);
		}

		//Insert the keys into MDHIM
		brm = mdhimBPut(md, (void **) keys, key_lens, 
				(void **) values, value_lens, KEYS, NULL, NULL);
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

		for (i = 0; i < KEYS; i++) {
			free(keys[i]);
			
		}
		free(keys);
		for (i = 0; i < KEYS; i++) {
			free(values[i]);
			
		}
		free(values);
	}

	close(fd);

done:
	//Quit MDHIM
	ret = mdhimClose(md);
	if (ret != MDHIM_SUCCESS) {
		printf("Error closing MDHIM\n");
	}
	
	MPI_Finalize();

	return 0;
}
