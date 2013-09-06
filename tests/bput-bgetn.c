#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include "mpi.h"
#include "mdhim.h"

#define KEYS 1000
//#define TOTAL_KEYS 2083334
#define TOTAL_KEYS 10000

void start_record(struct timeval *start) {
	gettimeofday(start, NULL);

}

void end_record(struct timeval *end) {
	gettimeofday(end, NULL);
}

void add_time(struct timeval *start, struct timeval *end, long *time) {
	*time += end->tv_sec - start->tv_sec;
}

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
	int      dbug = MLOG_CRIT; //MLOG_CRIT=1, MLOG_DBG=2
	db_options_t *db_opts; // Local variable for db create options to be passed
	int db_type = 2; //UNQLITE=1, LEVELDB=2 (data_store.h) 
	int size;
	long flush_time = 0;
	long put_time = 0;
	long get_time = 0;
	int total_keys = 0;
	int round = 0;
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

	//Initialize MDHIM
	md = mdhimInit(MPI_COMM_WORLD, db_opts);
	if (!md) {
		printf("Error initializing MDHIM\n");
		MPI_Abort(MPI_COMM_WORLD, ret);
		exit(1);
	}
	
	MPI_Comm_size(md->mdhim_comm, &size);	
	while (total_keys != TOTAL_KEYS) {
		//Populate the keys and values to insert
		keys = malloc(sizeof(int *) * KEYS);
		values = malloc(sizeof(int *) * KEYS);
		for (i = 0; i < KEYS; i++) {
			keys[i] = malloc(sizeof(int));
			*keys[i] = size * i + md->mdhim_rank + 1 + size * round;
//			printf("Rank: %d - Inserting key: %d\n", md->mdhim_rank, *keys[i]);
			key_lens[i] = sizeof(int);
			values[i] = malloc(sizeof(int));
			*values[i] = md->mdhim_rank;
			value_lens[i] = sizeof(int);		
		}
		
		start_record(&start_tv);
		//Insert the keys into MDHIM
		brm = mdhimBPut(md, (void **) keys, key_lens,  
				(void **) values, value_lens, KEYS);
		end_record(&end_tv);
		add_time(&start_tv, &end_tv, &put_time);

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
//			printf("Committed MDHIM database\n");
		}
		//Get the stats
		start_record(&start_tv);
		ret = mdhimStatFlush(md);
		end_record(&end_tv);
		add_time(&start_tv, &end_tv, &flush_time);

		if (ret != MDHIM_SUCCESS) {
			printf("Error getting stats from MDHIM database\n");
		} else {
//			printf("Got stats\n");
		}
		
		//Get the values back for each key inserted
		start_record(&start_tv);

		//start at the first key
		if (*keys[0] == 1) {
			bgrm = mdhimBGetOp(md, keys[0], key_lens[0], 
					   KEYS, MDHIM_GET_FIRST);
		} else {
			bgrm = mdhimBGetOp(md, keys[0], key_lens[0], 
					   KEYS, MDHIM_GET_NEXT);
		}
		end_record(&end_tv);
		add_time(&start_tv, &end_tv, &get_time);

		if (!bgrm || bgrm->error) {
			printf("Rank: %d - Error retrieving values", md->mdhim_rank);
			goto done;
		}
	
/*		for (i = 0; i < bgrm->num_records && !bgrm->error; i++) {			
			printf("Rank: %d - Got key: %d value: %d\n", md->mdhim_rank, 
			       *(int *)bgrm->keys[i], *(int *)bgrm->values[i]);
		}
*/	
		//Free the message received
		mdhim_full_release_msg(bgrm);
		for (i = 0; i < KEYS; i++) {
			free(keys[i]);
			free(values[i]);
		}

		free(keys);
		free(values);
		total_keys += KEYS;
		round++;
	}
done:
	//Quit MDHIM
	ret = mdhimClose(md);
	gettimeofday(&end_tv, NULL);
	if (ret != MDHIM_SUCCESS) {
		printf("Error closing MDHIM\n");
	}
	
	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();

	printf("Took: %ld seconds to put %d keys\n", 
	       put_time, TOTAL_KEYS);
	printf("Took: %ld seconds to get %d keys/values\n", 
	       get_time, TOTAL_KEYS);
	printf("Took: %ld seconds to stat flush\n", 
	       flush_time);


	return 0;
}
