/*
 * DB usage options. 
 * Location and name of DB, type of DataSotre primary key type,
 */
#include <stdlib.h>
#include "db_options.h"

// Default path to a local path and name, levelDB=2, int_key_type=1, yes_create_new=1
// and debug=1 (MLOG_CRIT)
struct db_options_t *db_options_init()
{
    struct db_options_t* opts;
    opts = malloc(sizeof(struct db_options_t));
    
    opts->db_path = "./";
    opts->db_name = "mdhimTstDB-"; 
    opts->db_type = 2;
    opts->db_key_type = 1;
    opts->debug_level = 1;
    opts->db_create_new = 1;
    
    return opts;
}

void db_options_set_path(db_options_t* opts, char *path)
{
    opts->db_path = path;
};

void db_options_set_name(db_options_t* opts, char *name)
{
    opts->db_name = name;
};

void db_options_set_type(db_options_t* opts, int type)
{
    opts->db_type = type;
};

void db_options_set_key_type(db_options_t* opts, int key_type)
{
    opts->db_key_type = key_type;
};

void db_options_set_create_new(db_options_t* opts, int create_new)
{
    opts->db_create_new = create_new;
};

void db_options_set_debug_level(db_options_t* opts, int dbug)
{
    opts->debug_level = dbug;
};

