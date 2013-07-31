/*
 * DB usage options. 
 * Location and name of DB, type of DataSotre primary key type,
 */
#include "db_options.h"

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

