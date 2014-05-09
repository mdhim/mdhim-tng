#include <stdio.h>
#include <stdlib.h>
#include <linux/limits.h>
#include "mpi.h"
#include "mdhim.h"
#include "partitioner.h"
#include "mdhim_options.h"

#define SLICE_SIZE 1

struct plfs_record {
        unsigned int chunk_id;
	unsigned long long int logical_offset;
	unsigned long long int size;
	char dropping_file[PATH_MAX];
	unsigned long long int physical_offset;
};

int main(int argc, char **argv) {
	int ret;
	int provided = 0;
	struct mdhim_t *md;
	struct mdhim_getrm_t *grm;
        mdhim_options_t *db_opts;
	struct plfs_record *rec = NULL;
	struct plfs_record *ptr;
	unsigned long long int key;
	unsigned long long int mkey;
        int blocksize;
        int i;
        char db_name[256];
        int entry_count=0;
        int rank, size;
        int loop_cnt=0;
        //int test_int;
	MPI_Comm comm;
        //unsigned int comm;



	ret = MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
	if (ret != MPI_SUCCESS) {
		printf("Error initializing MPI with threads\n");
		exit(1);
	}

	if (provided != MPI_THREAD_MULTIPLE) {
                printf("Not able to enable MPI_THREAD_MULTIPLE mode\n");
                exit(1);
        }

        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
        MPI_Comm_size(MPI_COMM_WORLD, &size);

        if (argc != 4) {
          printf("Usage:  entry_count blocksize db_name\n");
         return 1; 
        }
        entry_count=atoi(argv[1]);
        blocksize=atoi(argv[2]);
        strcpy(&db_name[0], argv[3]);         

	//Set MDHIM options
        db_opts = mdhim_options_init();
        mdhim_options_set_db_path(db_opts, "/turquoise/users/hng/plfs-mdhim-maps/");
        mdhim_options_set_db_name(db_opts, db_name);
        mdhim_options_set_db_type(db_opts, LEVELDB);
        mdhim_options_set_key_type(db_opts, MDHIM_LONG_INT_KEY);
	mdhim_options_set_debug_level(db_opts, MLOG_CRIT);
	mdhim_options_set_max_recs_per_slice(db_opts, SLICE_SIZE);
        mdhim_options_set_server_factor(db_opts, 10);
	mdhim_options_set_value_append(db_opts, 0);

	//Initialize MDHIM
	comm = MPI_COMM_WORLD;
	md = mdhimInit(&comm, db_opts);
	if (!md) {
		printf("Error initializing MDHIM\n");
		exit(1);
	}	


       printf("Going to get value now\n");
    loop_cnt = entry_count/size;
    for (i=0; i < loop_cnt; i++) {   
       //mkey = (rank+(i*8)) * 1048576;
       //sleep(rank+2);
       mkey = (rank+(i*size)) * blocksize;
       grm = mdhimGet(md, &mkey, sizeof(mkey), MDHIM_GET_EQ);
       ptr = (struct plfs_record *)grm->value;
       if (!grm || grm->error) {
         printf("Error getting value for key: %llu from MDHIM\n", key);
	 grm = mdhimGet(md, &key, sizeof(key), MDHIM_GET_NEXT);
         ptr = (struct plfs_record *)grm->value;
	 if (!grm || grm->error) {
	   printf("Error getting value for key: %llu from MDHIM\n", key);
         } else {
           printf("key = %llu\n", (unsigned long long)grm->key);
           printf("got next key\n");
         }
   
       } else {
         printf("Successfully got value with size: %d from MDHIM\n", grm->value_len);
         printf("key %llu  chunk_id %d next dropping file is %s\n\n", *((unsigned long long *)grm->key), ptr->chunk_id, ptr->dropping_file);
         // printf("value = %d\n", *((int *)grm->value));
         printf("I WAS SUCCESSFULl\n");
       }
    }

	ret = mdhimClose(md);
        //struct mdhim_rm_t *mdhimDelete(struct mdhim_t *md, void *key, int key_len);
        //
        /**********
        for (i = 0; i < 4; i++) {
             key = i++;
             rm = mdhimDelete(md, &key, sizeof(key));
        }
        **********/
//        key = rank * 1048576;
//        rm = mdhimDelete(md, &key, sizeof(key));
 
	free(rec);
	mdhim_options_destroy(db_opts);
	if (ret != MDHIM_SUCCESS) {
		printf("Error closing MDHIM\n");
	}

	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();

	return 0;
}
