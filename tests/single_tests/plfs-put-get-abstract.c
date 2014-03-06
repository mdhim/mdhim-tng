#include <stdio.h>
#include <stdlib.h>
#include <linux/limits.h>
#include "mpi.h"
#include "mdhim.h"
#include "mdhim-abridge.h"
#include "mdhim_options.h"

#define SLICE_SIZE 1

struct plfs_record {
	unsigned long long int logical_offset;
	unsigned long long int size;
	char dropping_file[PATH_MAX];
	unsigned long long int physical_offset;
};

FILE *open_output(int rank) {
	FILE *file;
	char rank_str[4];
	char file_str[4];
        char char_str[2];	
	int i, j;
	char filename[PATH_MAX];

	//Opens the file and coverts the rank to characters
	sprintf(rank_str, "%d", rank);
	memset(file_str, 0, 4);
	memset(file_str, 'a', 3);
	j = strlen(file_str) - 1;
	for (i = strlen(rank_str) - 1; i >= 0; i--) {
		sprintf(char_str, "%c", rank_str[i]);
		file_str[j] = strtol(char_str, NULL, 10) + 'a';
		j--;
	}

	sprintf(filename, "plfs-output/plfs%s", file_str);
	printf("file string is: %s\n", filename);
	file = fopen(filename, "r");
	if (!file) {
		printf("Error opening the input file");
	}

	return file;
}

struct plfs_record *parse_input(FILE *file) {
	struct plfs_record *rec;
	int ret;

	rec = malloc(sizeof(struct plfs_record));
	ret = fscanf(file, "%llu %llu %s %llu", &rec->logical_offset, &rec->size, 
		     rec->dropping_file, &rec->physical_offset);
	if (!ret || ret == EOF) {
		printf("Error parsing file\n");
		exit(1);
	}

	printf("Parsed record with logical_offset: %llu, size: %llu, dropping_file: %s," 
	       " physical_offset: %llu\n", 
	       rec->logical_offset, rec->size, 
	       rec->dropping_file, rec->physical_offset);

	return rec;
}

long long get_key(unsigned long long int lo) {
	unsigned long long int ret = ((unsigned long long int) lo/SLICE_SIZE) * SLICE_SIZE;
	return ret;
}

int main(int argc, char **argv) {
	int ret;
	int provided = 0;
	struct mdhim_t *o_md;
	struct mdhim_rm_t *rm;
    struct mdhim_getrm_t *grm;
    //    mdhim_options_t *db_opts;
    Mdhim_Ptr *md = new_MDHIM();
	struct plfs_record *rec = NULL;
	FILE *file;
	unsigned long long int key;
	MPI_Comm comm;

	ret = MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
	if (ret != MPI_SUCCESS) {
		printf("Error initializing MPI with threads\n");
		exit(1);
	}

	if (provided != MPI_THREAD_MULTIPLE) {
                printf("Not able to enable MPI_THREAD_MULTIPLE mode\n");
                exit(1);
        }

	//Set MDHIM options
        //db_opts = mdhim_options_init();
        mdhim_options_set_db_path((mdhim_options_t *)md->mdhim_options, "./");
        mdhim_options_set_db_name((mdhim_options_t *)md->mdhim_options, "mdhimTstDB");
        mdhim_options_set_db_type((mdhim_options_t *)md->mdhim_options, LEVELDB);
        mdhim_options_set_key_type((mdhim_options_t *)md->mdhim_options, MDHIM_LONG_INT_KEY);
	mdhim_options_set_debug_level((mdhim_options_t *)md->mdhim_options, MLOG_CRIT);
	mdhim_options_set_max_recs_per_slice((mdhim_options_t *)md->mdhim_options, SLICE_SIZE);
        mdhim_options_set_server_factor((mdhim_options_t *)md->mdhim_options, 10);
	mdhim_options_set_value_append((mdhim_options_t *)md->mdhim_options, 1);

	//Initialize MDHIM
	comm = MPI_COMM_WORLD;
	//md = mdhimInit(&comm, db_opts);
	md->mdhimInit(md, &comm, md->mdhim_options);
	if (!md->mdhim) {
		printf("Error initializing MDHIM\n");
		exit(1);
	}	
    o_md = (struct mdhim_t *)md->mdhim;
	file = open_output(o_md->mdhim_rank);
	if (!file) {
		printf("Error opening file\n");
		goto done;
	}
	
	rec = parse_input(file);
	if (!rec) {
		printf("Error parsing file\n");
		goto done;
	}
	key = get_key(rec->logical_offset);
	printf("Inserting key: %llu\n", key);
	//rm = mdhimPut(md, &key, sizeof(key), 
	  //        rec, sizeof(struct plfs_record));
	md->mdhimPut(md, &key, sizeof(key), rec, sizeof(struct plfs_record));
	rm = (struct mdhim_rm_t *)md->mdhim_rm;
	if (!md->mdhim_rm || rm->error) {
		printf("Error inserting key/value into MDHIM\n");
	} else {
		printf("Successfully inserted key/value into MDHIM\n");
	}
	
	mdhim_full_release_msg(rm);
	//Commit the database
	//ret = mdhimCommit(md);
	ret = md->mdhimCommit(md);
	if (ret != MDHIM_SUCCESS) {
		printf("Error committing MDHIM database\n");
	} else {
		printf("Committed MDHIM database\n");
	}

	//Get the values
	//grm = mdhimGet(md, &key, sizeof(key), MDHIM_GET_EQ);
	md->mdhimGet(md, &key, sizeof(key), MDHIM_GET_EQ);
	grm = (struct mdhim_getrm_t *)md->mdhim_getrm;
	if (!md->mdhim_getrm || grm->error) {
		printf("Error getting value for key: %llu from MDHIM\n", key);
	} else {
		printf("Successfully got value with size: %d from MDHIM\n", grm->value_len);
	}

	mdhim_full_release_msg(grm);

done:
	//ret = mdhimClose(md);
	ret = md->mdhimClose(md);
	free(rec);
	mdhim_options_destroy(md->mdhim_options);
	if (ret != MDHIM_SUCCESS) {
		printf("Error closing MDHIM\n");
	}

	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();

	return 0;
}
