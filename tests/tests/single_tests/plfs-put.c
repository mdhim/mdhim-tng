#include <stdio.h>
#include <stdlib.h>
#include <linux/limits.h>
#include "mpi.h"
#include "mdhim.h"
#include "partitioner.h"
#include "mdhim_options.h"

#define SLICE_SIZE 1000

struct plfs_record {
        unsigned int chunk_id;
	unsigned long long int logical_offset;
	unsigned long long int size;
	char dropping_file[PATH_MAX];
	unsigned long long int physical_offset;
};

FILE *open_output(int rank, char *path, char *basename) {
	FILE *file;
	char filename[PATH_MAX];

	sprintf(filename, "%s/%s.%d", path,basename,rank);
//	printf("rank is %d file is: %s\n", rank,filename);
	file = fopen(filename, "r");
	if (!file) {
		printf("Error opening the input file");
	}

	return file;
}


int main(int argc, char **argv) {
	int ret;
	int provided = 0;
	struct mdhim_t *md;
	struct mdhim_rm_t *rm;
        mdhim_options_t *db_opts;
	struct plfs_record *rec = NULL;
	FILE *file;
	unsigned long long int key;
        int i;
        char file_path[256];
        char basename[256];
        char db_name[256];
        int file_count=0;
        int loop_cnt=0;
        //int plfs_line =0;
        int rank, size;
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

        if (argc != 5) {
          printf("Error in arguments %d\n", argc);
          printf("Usage:  plfs-put file_count datafilepath basename db_name\n");
         return 1; 
        }
        file_count=atoi(argv[1]);
        strcpy(&file_path[0], argv[2]);         
        strcpy(&basename[0], argv[3]);
        strcpy(&db_name[0], argv[4]);

	//Set MDHIM options
        db_opts = mdhim_options_init();
        mdhim_options_set_db_path(db_opts, "./");
        //mdhim_options_set_db_path(db_opts, "/users/atorrez/usr-project-test/");
        mdhim_options_set_db_name(db_opts, db_name);
        mdhim_options_set_db_type(db_opts, LEVELDB);
        mdhim_options_set_key_type(db_opts, MDHIM_LONG_INT_KEY);
	mdhim_options_set_debug_level(db_opts, MLOG_CRIT);
	mdhim_options_set_max_recs_per_slice(db_opts, SLICE_SIZE);
        mdhim_options_set_server_factor(db_opts, 4);
	mdhim_options_set_value_append(db_opts, 0);
	rec = malloc(sizeof(struct plfs_record));
        

	//Initialize MDHIM
	comm = MPI_COMM_WORLD;
	md = mdhimInit(&comm, db_opts);
	if (!md) {
		printf("Error initializing MDHIM\n");
		exit(1);
	}	

        loop_cnt =  file_count/size;
        for (i =0; i <loop_cnt; i++) { 
           file = open_output(rank+(i*size), file_path, basename);
	   if (!file) {
	      printf("Error opening file\n");
	      goto done;
	   }
           else {
              //while (!plfs_line || plfs_line != EOF) {
	         while (fscanf(file, "%d %llu %llu %s %llu", &rec->chunk_id, &rec->logical_offset, &rec->size, 
	   	           rec->dropping_file, &rec->physical_offset) != EOF) {

/*	         printf("Parsed record with logical_offset: %llu, size: %llu, dropping_file: %s," 
	                " physical_offset: %llu\n", 
	                rec->logical_offset, rec->size, 
	                rec->dropping_file, rec->physical_offset);*/

	         if (!rec) {
	            printf("Error parsing file\n");
                    goto done;
	         }
	         key = rec->logical_offset;
//	         printf("lenght of record = %lu Inserting key: %llu\n", sizeof(struct plfs_record),key);
	         rm = mdhimPut(md, &key, sizeof(key), 
	         rec, sizeof(struct plfs_record));
	         if (!rm || rm->error) {
	            printf("Error inserting key/value into MDHIM\n");
	         } else {
//	            printf("Successfully inserted key/value into MDHIM\n");
	         }
	         mdhim_full_release_msg(rm);
	
/*	         ret = mdhimCommit(md);
	         if (ret != MDHIM_SUCCESS) {
		    printf("Error committing MDHIM database\n");
	         } else {
		    printf("Committed MDHIM database\n");
	         }

                 //Get the stats
                 ret = mdhimStatFlush(md);
                 if (ret != MDHIM_SUCCESS) {
                    printf("Error getting stats\n");
                 } else {
                    printf("Got stats\n");
                 }
*/ 
              }
           }
        }

	//mdhim_full_release_msg(grm);

done:
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
