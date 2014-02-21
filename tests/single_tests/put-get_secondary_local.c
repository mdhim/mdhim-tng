#include <stdio.h>
#include <stdlib.h>
#include "mpi.h"
#include "mdhim.h"
#include "mdhim_options.h"

#define SLICE_SIZE 100
#define SECONDARY_SLICE_SIZE 5

int main(int argc, char **argv) {
	int ret;
	int provided = 0;
	struct mdhim_t *md;
	uint32_t key, secondary_key;
	int value, secondary_value;
	struct mdhim_rm_t *rm;
	struct mdhim_getrm_t *grm;
        mdhim_options_t *db_opts;
	struct index_t *secondary_index;

	ret = MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
	if (ret != MPI_SUCCESS) {
		printf("Error initializing MPI with threads\n");
		exit(1);
	}

	if (provided != MPI_THREAD_MULTIPLE) {
                printf("Not able to enable MPI_THREAD_MULTIPLE mode\n");
                exit(1);
        }
        
        db_opts = mdhim_options_init();
        mdhim_options_set_db_path(db_opts, "./");
        mdhim_options_set_db_name(db_opts, "mdhimTstDB");
        mdhim_options_set_db_type(db_opts, LEVELDB);
        mdhim_options_set_key_type(db_opts, MDHIM_INT_KEY); //Key_type = 1 (int)
	mdhim_options_set_debug_level(db_opts, MLOG_CRIT);

	md = mdhimInit(MPI_COMM_WORLD, db_opts);
	if (!md) {
		printf("Error initializing MDHIM\n");
		exit(1);
	}	

	//Put the primary keys and values
	key = 100 * (md->mdhim_rank + 1);
	value = 500 * (md->mdhim_rank + 1);
	rm = mdhimPut(md, md->primary_index, 
		      &key, sizeof(key), 
		      &value, sizeof(value));
	if (!rm || rm->error) {
		printf("Error inserting key/value into MDHIM\n");
	} else {
		printf("Successfully inserted key/value into MDHIM\n");
	}

	//Release the received message
	mdhim_full_release_msg(rm);

	//Commit the database
	ret = mdhimCommit(md, md->primary_index);
	if (ret != MDHIM_SUCCESS) {
		printf("Error committing MDHIM database\n");
	} else {
		printf("Committed MDHIM database\n");
	}

	//Create a secondary index on only one range server
	secondary_index = create_local_index(md, LEVELDB, 
					     MDHIM_INT_KEY, 4, 
					     md->primary_index->id);

	//Put the primary keys and values
	secondary_key = md->mdhim_rank + 1;
	secondary_value = key;
	rm = mdhimPut(md, secondary_index, 
		      &secondary_key, sizeof(secondary_key), 
		      &secondary_value, sizeof(secondary_value));
	if (!rm || rm->error) {
		printf("Error inserting key/value into MDHIM\n");
	} else {
		printf("Successfully inserted key/value into MDHIM\n");
	}

	//Get the primary key values from the secondary key
	value = 0;
	grm = mdhimGet(md, secondary_index, &secondary_key, sizeof(secondary_key), 
		       MDHIM_GET_PRIMARY_EQ);
	if (!grm || grm->error) {
		printf("Error getting value for key: %d from MDHIM\n", key);
	} else if (grm->value_len) {
		printf("Successfully got value: %d from MDHIM\n", *((int *) grm->value));
	}

	mdhim_full_release_msg(grm);
	ret = mdhimClose(md);
	mdhim_options_destroy(db_opts);
	if (ret != MDHIM_SUCCESS) {
		printf("Error closing MDHIM\n");
	}

	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();

	return 0;
}
