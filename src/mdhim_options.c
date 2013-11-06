/*
 * DB usage options. 
 * Location and name of DB, type of DataSotre primary key type,
 */
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "mdhim_options.h"

// Default path to a local path and name, levelDB=2, int_key_type=1, yes_create_new=1
// and debug=1 (MLOG_CRIT)
struct mdhim_options_t *mdhim_options_init()
{
	struct mdhim_options_t* opts;
	opts = malloc(sizeof(struct mdhim_options_t));
    
	opts->db_path = "./";
	opts->db_name = "mdhimTstDB-"; 
	opts->db_type = 2;
	opts->db_key_type = 1;
	opts->db_create_new = 1;
	opts->db_value_append = MDHIM_DB_OVERWRITE;
        
	opts->debug_level = 1;
        opts->rserver_factor = 4;
        opts->max_recs_per_slice = 100000;
	opts->db_paths = NULL;
	opts->num_paths = 0;
	return opts;
}

void mdhim_options_set_db_path(mdhim_options_t* opts, char *path)
{
	opts->db_path = path;
};

void mdhim_options_set_db_paths(struct mdhim_options_t* opts, char **paths, int num_paths)
{
	int i = 0;

	if (num_paths <= 0) {
		return;
	}

	opts->db_paths = malloc(sizeof(char *) * num_paths);
	for (i = 0; i < num_paths; i++) {
		if (!paths[i]) {
			continue;
		}
		opts->db_paths[i] = malloc(strlen(paths[i]) + 1);
		sprintf(opts->db_paths[i], "%s", paths[i]);
	}

	opts->num_paths = num_paths;
};

void mdhim_options_set_db_name(mdhim_options_t* opts, char *name)
{
	opts->db_name = name;
};

void mdhim_options_set_db_type(mdhim_options_t* opts, int type)
{
	opts->db_type = type;
};

void mdhim_options_set_key_type(mdhim_options_t* opts, int key_type)
{
	opts->db_key_type = key_type;
};

void mdhim_options_set_create_new_db(mdhim_options_t* opts, int create_new)
{
	opts->db_create_new = create_new;
};

void mdhim_options_set_debug_level(mdhim_options_t* opts, int dbug)
{
	opts->debug_level = dbug;
};

void mdhim_options_set_value_append(mdhim_options_t* opts, int append)
{
	opts->db_value_append = append;
};

void mdhim_options_set_server_factor(mdhim_options_t* opts, int server_factor)
{
	opts->rserver_factor = server_factor;
};

void mdhim_options_set_max_recs_per_slice(mdhim_options_t* opts, uint64_t max_recs_per_slice)
{
	opts->max_recs_per_slice = max_recs_per_slice;
};

void mdhim_options_destroy(mdhim_options_t *opts) {
	int i;

	for (i = 0; i < opts->num_paths; i++) {
		free(opts->db_paths[i]);
	}

	free(opts);
};
