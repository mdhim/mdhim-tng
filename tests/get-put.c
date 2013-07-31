#include <stdio.h>
#include <stdlib.h>
#include "mpi.h"
#include "mdhim.h"
#include "db_options.h"

int main(int argc, char **argv) {
	int ret;
	int provided = 0;
	struct mdhim_t *md;
	int key;
	int value;
	struct mdhim_rm_t *rm;
	struct mdhim_getrm_t *grm;
        db_options_t *db_opts;

	ret = MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
	if (ret != MPI_SUCCESS) {
		printf("Error initializing MPI with threads\n");
		exit(1);
	}

	if (provided != MPI_THREAD_MULTIPLE) {
                printf("Not able to enable MPI_THREAD_MULTIPLE mode\n");
                exit(1);
        }
        
        db_opts = malloc(sizeof(struct db_options_t));
        db_options_set_path(db_opts, "./");
        db_options_set_name(db_opts, "mdhim_tstDB");
        db_options_set_type(db_opts, 2); // type = 2 (LevelDB)
        db_options_set_key_type(db_opts, MDHIM_INT_KEY); //Key_type = 1 (int)

	md = mdhimInit(MPI_COMM_WORLD, db_opts);
	if (!md) {
		printf("Error initializing MDHIM\n");
		exit(1);
	}	

	//Put the keys and values
	key = 100 * (md->mdhim_rank + 1);
	value = 500 * (md->mdhim_rank + 1);
	rm = mdhimPut(md, &key, sizeof(key), 
		       &value, sizeof(value));
	if (!rm || rm->error) {
		printf("Error inserting key/value into MDHIM\n");
	} else {
		printf("Successfully inserted key/value into MDHIM\n");
	}

	//Commit the database
	ret = mdhimCommit(md);
	if (ret != MDHIM_SUCCESS) {
		printf("Error committing MDHIM database\n");
	} else {
		printf("Committed MDHIM database\n");
	}

	//Get the values
	value = 0;
	grm = mdhimGet(md, &key, sizeof(key), MDHIM_GET_EQ);
	if (!grm || grm->error) {
		printf("Error getting value for key: %d from MDHIM\n", key);
	} else {
		printf("Successfully got value: %d from MDHIM\n", *((int *) grm->value));
	}

	ret = mdhimClose(md);
	if (ret != MDHIM_SUCCESS) {
		printf("Error closing MDHIM\n");
	}

	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();

	return 0;
}
