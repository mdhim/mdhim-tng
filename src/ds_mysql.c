#include <my_global.h>
#include <mysql.h>
#include <stdlib.h>
#include <string.h> 
#include <stdio.h>
#include <linux/limits.h>
#include <sys/time.h>
#include "ds_mysql.h"
#include <unistd.h>
#include <mcheck.h>
#define MYSQL_BUFFER 1024
#define MYSQLDB_HANDLE 1
#define MYSQLDB_STAT_HANDLE 2

char *sb_key_copy(MYSQL *d, char *key_t, int key_len_t) {
	if (key_len_t ==0) key_len_t = strlen(key_t);
	char *k_copy =  malloc(2+key_len_t+1);
		memset(k_copy, 0, key_len_t+1);
	mysql_real_escape_string(d, k_copy, key_t, key_len_t);

	return k_copy;
	}
//Struct for MYSQL Handling
//MDI = Mysql Database Info 
typedef struct {
	MYSQL *msqdb; //Database connection 
	int msqht; //Handle's specfication, whether it's the original or stat
	} MDI; 

/*
 *Put general function
 *
 *@param pdb    in MYSQL pointer Database handle for connection and doing the escape stirnt
 *@param key_t  in void pointer Key for value
 *@param k_len  in int Key lenght 
 *@param data_t in void pointer Data to be inserted  
 *@param d_len  in int Data Length 
 *@param t_name in char pointer Table name to execute insert 

*/

char *put_value(MYSQL *pdb, void *key_t, int k_len, void *data_t, int d_len, char *t_name, int pk_type){

char chunk[2*d_len+1];
  mysql_real_escape_string(pdb, chunk, data_t, d_len);
 // mysql_real_escape_string(db, key_t_insert,key_t,k_len);
	char *key_copy;
	size_t st_len, size=0;
	char *r_query=NULL, *st;
	switch(pk_type){
			case MDHIM_STRING_KEY:
			  //mysql_real_escape_string(pdb, k_chunk, key_t, k_len);
  				st = "Insert INTO %s (Id, Value) VALUES ('%s', '%s');";
 		 		st_len = strlen(st);
  				size = 2*d_len+1 + 2*k_len+1 + strlen(t_name)+st_len;//strlen(chunk)+strlen(key_t_insert)+1;
  				r_query=malloc(sizeof(char)*(size)); 
				key_copy = malloc(k_len+1);
				memset(key_copy, 0, k_len+1);
				memcpy(key_copy, key_t, k_len);
  				snprintf(r_query, st_len + size, st, t_name, key_copy, chunk);
				free(key_copy);
			break;
       			case MDHIM_FLOAT_KEY:
				st = "Insert INTO %s (Id, Value) VALUES (%f, '%s');";
				st_len = strlen(st);
  				size = 2*d_len+1 + 2*k_len+1 + strlen(t_name)+st_len;//strlen(chunk)+strlen(key_t_insert)+1;
  				r_query=malloc(sizeof(char)*(size)); 
				snprintf(r_query, st_len + size, st, t_name, *((float*)key_t), chunk);
				break;
   			case MDHIM_DOUBLE_KEY:
				st = "Insert INTO %s (Id, Value) VALUES (%lf, '%s');";
				st_len = strlen(st);
  				size = 2*d_len+1 + 2*k_len+1 + strlen(t_name)+st_len;//strlen(chunk)+strlen(key_t_insert)+1;
  				r_query=malloc(sizeof(char)*(size)); 
				snprintf(r_query, st_len + size, st, t_name, *((double*)key_t), chunk);
			break;
      			case MDHIM_INT_KEY: 
				st = "Insert INTO %s (Id, Value) VALUES (%d, '%s');";
				st_len = strlen(st);
  				size = 2*d_len+1 + 2*k_len+1 + strlen(t_name)+st_len;//strlen(chunk)+strlen(key_t_insert)+1;
  				r_query=malloc(sizeof(char)*(size)); 
				snprintf(r_query, st_len + size, st, t_name, *((int*)key_t), chunk);
			break;
			case MDHIM_LONG_INT_KEY:
				st = "Insert INTO %s (Id, Value) VALUES (%ld, '%s');";
				st_len = strlen(st);
  				size = 2*d_len+1 + 2*k_len+1 + strlen(t_name)+st_len;//strlen(chunk)+strlen(key_t_insert)+1;
  				r_query=malloc(sizeof(char)*(size)); 
				snprintf(r_query, st_len + size, st, t_name, *((long*)key_t), chunk);
			break;
			case MDHIM_BYTE_KEY:
				//mysql_real_escape_string(pdb, k_chunk, key_t, k_len);
  				st = "Insert INTO %s (Id, Value) VALUES ('%s', '%s');";
 		 		st_len = strlen(st);
  				size = 2*d_len+1 + 2*k_len+1 + strlen(t_name)+st_len;//strlen(chunk)+strlen(key_t_insert)+1;
  				r_query=malloc(sizeof(char)*(size)); 
				memset(r_query, 0, size);
				key_copy = malloc(k_len+1);
				memset(key_copy, 0, k_len+1);
				memcpy(key_copy, key_t, k_len);
  				snprintf(r_query, size, st, t_name, key_copy, chunk);
				free(key_copy);
			break; 
		}	
	return r_query;

}


/* update_value 
 *Update the value if it didn't insert on put.
 
 *@param pdb    in MYSQL pointer Database handle for connection and doing the escape stirnt
 *@param key_t  in void pointer Key for value
 *@param k_len  in int Key lenght 
 *@param data_t in void pointer Data to be inserted  
 *@param d_len  in int Data Length 
 *@param t_name in char pointer Table name to execute insert 

*/
char *update_value(MYSQL *pdb, void *key_t, int k_len, void *data_t, int d_len, char *t_name, int pk_type){

char chunk[2*d_len+1];
  mysql_real_escape_string(pdb, chunk, data_t, d_len);
 // mysql_real_escape_string(db, key_t_insert,key_t,k_len);

	size_t st_len, size=0;
	char *r_query=NULL, *st;
	switch(pk_type){
			case MDHIM_STRING_KEY:
  				st = "Update %s set Value = '%s' where Id = '%s';";
 		 		st_len = strlen(st);
  				size = 2*d_len+1 + 2*k_len+1 + strlen(t_name)+st_len;//strlen(chunk)+strlen(key_t_insert)+1;
  				r_query=malloc(sizeof(char)*(size)); 
  				snprintf(r_query, st_len + size, st, t_name,  chunk, (char*)key_t);
			break;
       			case MDHIM_FLOAT_KEY:
				st = "Update %s set Value = '%s' where Id = %f;";
				st_len = strlen(st);
  				size = 2*d_len+1 + 2*k_len+1 + strlen(t_name)+st_len;//strlen(chunk)+strlen(key_t_insert)+1;
  				r_query=malloc(sizeof(char)*(size)); 
				snprintf(r_query, st_len + size, st, t_name, chunk, *((float*)key_t) );
				break;
   			case MDHIM_DOUBLE_KEY:
				st = "Update %s set Value = '%s' where Id = %lf;";
				st_len = strlen(st);
  				size = 2*d_len+1 + 2*k_len+1 + strlen(t_name)+st_len;//strlen(chunk)+strlen(key_t_insert)+1;
  				r_query=malloc(sizeof(char)*(size)); 
				snprintf(r_query, st_len + size, st, t_name, chunk, *((double*)key_t) );
			break;
      			case MDHIM_INT_KEY: 
				st = "Update %s set Value = '%s' where Id = %d;";
				st_len = strlen(st);
  				size = 2*d_len+1 + 2*k_len+1 + strlen(t_name)+st_len;//strlen(chunk)+strlen(key_t_insert)+1;
  				r_query=malloc(sizeof(char)*(size)); 
				snprintf(r_query, st_len + size, st, t_name,chunk, *((int*)key_t));
			break;
			case MDHIM_LONG_INT_KEY:
				st = "Update %s set Value = '%s' where Id = %ld;";
				st_len = strlen(st);
  				size = 2*d_len+1 + 2*k_len+1 + strlen(t_name)+st_len;//strlen(chunk)+strlen(key_t_insert)+1;
  				r_query=malloc(sizeof(char)*(size)); 
				snprintf(r_query, st_len + size, st, t_name, chunk, *((long*)key_t));
			break;
			case MDHIM_BYTE_KEY:
  				st = "Update %s set Value = '%s' where Id = '%s';";
 		 		st_len = strlen(st);
  				size = 2*d_len+1 + 2*k_len+1 + strlen(t_name)+st_len;//strlen(chunk)+strlen(key_t_insert)+1;
  				r_query=malloc(sizeof(char)*(size)); 
  				snprintf(r_query, size, st, t_name, chunk, (char*)key_t);
			break; 
		}	
	return r_query;

}


/* create db

* Helps to create the database for mysql if it does not exist 
* @param dbmn  Database name that will be used in the function
* @param db Database connection used to create the database
* Exit(1) is used to exit out if you don't have a connection

*/

void create_db(char *dbmn, MYSQL *db){
	char q_create[MYSQL_BUFFER];
	char *creater = "CREATE DATABASE %s";
	char *use_state= "USE %s";
	snprintf(q_create, sizeof(char)*MYSQL_BUFFER, use_state, dbmn);
	 if (mysql_query(db, q_create)) 
	  {
		memset(q_create, 0, strlen(q_create));
		snprintf(q_create, sizeof(char)*(strlen(creater)+strlen(dbmn)),creater, dbmn);
		//printf("\nCREATE DATABASE %s\n",dbmn);
		if(mysql_query(db,q_create)) {
	     		fprintf(stderr, "%s\n", mysql_error(db));
      			mysql_close(db);
	      		exit(1);
	  	} 
		memset(q_create, 0, strlen(q_create));
		snprintf(q_create, sizeof(char)*(strlen(use_state)+strlen(dbmn)),use_state, dbmn);
		if(mysql_query(db,q_create)) {
	     		fprintf(stderr, "%s\n", mysql_error(db));
      			mysql_close(db);
	      		exit(1);
			} //else printf("DATABASE IN USE");
	  }
		 

}

/* create_table fot table name 
 * @param dbmt  Database table name to create the table/check to see if table is there
 * @param db_name Database name for accessing the databaase 
 * @param k_type Key type that is used to create the table with the proper type for the key/value storage
 **/
void create_table(char *dbmt, MYSQL *db, char* db_name, int k_type){
	char name[MYSQL_BUFFER];
	//snprintf(name, "SHOW TABLES LIKE \'%s\'", dbmt);
  //Create table and if it's there stop
	  if(mysql_query(db, "SHOW TABLES LIKE 'mdhim'")){
		fprintf(stderr, "%s\n", mysql_error(db));
		}
		int check_er = mysql_errno(db);
	    //printf("\nThis is the error number: %d\n", check_er);
		if (check_er == 1050){
			if(mysql_query(db, "Drop table mdhim")){
		fprintf(stderr, "%s\n", mysql_error(db));
			}
		}
	  MYSQL_RES *table_res=mysql_store_result(db);
	  int row_count = table_res->row_count;
	  if(row_count == 0)
		{ 
		memset(name, 0, strlen(name));
		switch(k_type){
			case MDHIM_STRING_KEY:
			snprintf(name, sizeof(char)*MYSQL_BUFFER, "CREATE TABLE %s( Id VARCHAR(767) PRIMARY KEY, Value LONGBLOB)", dbmt);
			break;
       			case MDHIM_FLOAT_KEY:
				snprintf(name,  sizeof(char)*MYSQL_BUFFER, "CREATE TABLE %s( Id FLOAT PRIMARY KEY, Value LONGBLOB)", dbmt);
				break;
   			case MDHIM_DOUBLE_KEY:
				snprintf(name,  sizeof(char)*MYSQL_BUFFER, "CREATE TABLE %s( Id DOUBLE PRIMARY KEY, Value LONGBLOB)", dbmt);
			break;
      			case MDHIM_INT_KEY: 
				snprintf(name,  sizeof(char)*MYSQL_BUFFER, "CREATE TABLE %s( Id BIGINT PRIMARY KEY, Value LONGBLOB)", dbmt);
			break;
			case MDHIM_LONG_INT_KEY:
				snprintf(name,  sizeof(char)*MYSQL_BUFFER, "CREATE TABLE %s( Id BIGINT PRIMARY KEY, Value LONGBLOB)", dbmt);
			break;
			case MDHIM_BYTE_KEY:
				snprintf(name,  sizeof(char)*MYSQL_BUFFER, "CREATE TABLE %s( Id VARCHAR(767) PRIMARY KEY, Value LONGBLOB)", dbmt);
			break; 
		}

	  	if(mysql_query(db, name)){
			fprintf(stderr, "%s\n", mysql_error(db));
			mysql_close(db);
			}
		} 

}
/* str_to_key Helps convert the value in mysql_row into the proper key type and readability 
 * @param key_row Mysql_ROW type that has the key value 
 * @param key_type Value that determins what type the mysql_row info should be convereted into thus garanteting the proper key type 
 * @param size Gets size of the key type 
 **/

void * str_to_key(MYSQL_ROW key_row, int key_type, int * size){
	void * ret=NULL;
		switch(key_type){
			case MDHIM_STRING_KEY:
				*size= strlen(key_row[0]);
				ret = malloc(*size);
				ret=key_row[0];
			break;
       			case MDHIM_FLOAT_KEY:
				*size= sizeof(float);
				ret = malloc(*size);
				*(float*)ret=strtol(key_row[0],NULL,10);
				break;
   			case MDHIM_DOUBLE_KEY:
				*size= sizeof(double);
				char * endptr;
				ret = malloc(*size);
				*(double*)ret=strtod(key_row[0],&endptr);
			break;
      			case MDHIM_INT_KEY: 
				*size= sizeof(int);
				ret = malloc(*size);
				*(int*)ret=strtol(key_row[0],NULL,10);
			break;
			case MDHIM_LONG_INT_KEY:
				*size= sizeof(long);
				ret = malloc(*size);
				*(long*)ret=strtol(key_row[0],NULL,10);
			break;
			case MDHIM_BYTE_KEY:
				*size= strlen(key_row[0]);
				ret = malloc(*size);
				ret=key_row[0];
			break; 
		}

	return ret;

}

/**
 * mdhim_mysql_open
 * Opens the database
 *
 * @param dbh            in   double pointer to the mysql handle
 * @param dbs            in   double pointer to the mysql statistics db handle 
 * @param path           in   path to the database file
 * @param flags          in   flags for opening the data store
 * @param mstore_opts    in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */

	

int mdhim_mysql_open(void **dbh, void **dbs, char *path, int flags,
		       struct mdhim_store_opts_t *mstore_opts) {
	MDI *Input_DB;
	MDI *Stat_DB;
	MYSQL *db = mysql_init(NULL);
	MYSQL *sdb = mysql_init(NULL);
	if (db == NULL){
		fprintf(stderr, "%s\n", mysql_error(db));
	      	return MDHIM_DB_ERROR;
	  }
	if (sdb == NULL){
	      	fprintf(stderr, "%s\n", mysql_error(db));
	      	return MDHIM_DB_ERROR;
	  }
	/*char *path_s = malloc(sizeof(path));
	path_s = strcpy(path_s, path);	*/
	char *db_mysql_host =  (char*)mstore_opts -> db_ptr10;
	char *db_mysql_user = (char*)mstore_opts -> db_ptr11;
	char *sdb_mysql_user = (char*)mstore_opts -> db_ptr13;
	char *db_mysql_pswd = (char*)mstore_opts -> db_ptr12;
	char *sdb_mysql_pswd = (char*)mstore_opts->db_ptr14;
	//Abstracting the host, usernames, and password
	char *db_mysql_name = "maindb"; //mstore_opts -> db_ptr4; //Abstracting Database
	char *db_mysql_table = "mdhim"; 
	char *sdb_mysql_name = "statsdb";//mstore_opts -> db_ptr4; //Abstracting Statsics Database 
	char *sdb_mysql_table = "mdhim"; 

	//connect to the Database
	if (mysql_real_connect(db, db_mysql_host, db_mysql_user, db_mysql_pswd, 
          NULL, 0, NULL, 0) == NULL){
     		fprintf(stderr, "%s\n", mysql_error(db));
      		mysql_close(db);
      		return MDHIM_DB_ERROR;
  		}  
	if (mysql_real_connect(sdb, db_mysql_host, sdb_mysql_user, sdb_mysql_pswd, 
          NULL, 0, NULL, 0) == NULL){
     		fprintf(stderr, "%s\n", mysql_error(db));
      		mysql_close(sdb);
      		return MDHIM_DB_ERROR;
  		} 
	 if (mysql_library_init(0, NULL, NULL)) {
    		fprintf(stderr, "could not initialize MySQL library\n");
    		return MDHIM_DB_ERROR;
 		 }

	create_db(db_mysql_name, db);
	create_table(db_mysql_table, db, db_mysql_name, mstore_opts->key_type);
	create_db(sdb_mysql_name, sdb);
	create_table(sdb_mysql_table, sdb, sdb_mysql_name, mstore_opts->key_type);
	//Abstracting the host, usernames, and password
	
	Input_DB = malloc(sizeof(MDI));
	Stat_DB = malloc(sizeof(MDI));	
	Input_DB->msqdb = db;
	Input_DB->msqht = MYSQLDB_HANDLE;
	Stat_DB->msqdb = sdb;
	Stat_DB->msqht = MYSQLDB_STAT_HANDLE;
	*dbh = Input_DB;
	*dbs = Stat_DB;
	mstore_opts -> db_ptr5 = db_mysql_table;
	mstore_opts -> db_ptr6 = sdb_mysql_table;

	

	return MDHIM_SUCCESS;

}
/////////////////////////////////////////////////////////////////////////////////////////

/**
 * mdhim_mysql_put
 * Stores a single key in the data store
 *
 * @param dbh         in   pointer to the mysql struct which points to the handle
 * @param key         in   void * to the key to store
 * @param key_len     in   length of the key
 * @param data        in   void * to the value of the key
 * @param data_len    in   length of the value data 
 * @param mstore_opts in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_mysql_put(void *dbh, void *key, int key_len, void *data, int32_t data_len,  
	struct mdhim_store_opts_t *mstore_opts) {
	
	struct timeval start, end;
	MYSQL_RES *p_res;
	//printf("In put function\n");
	MDI *x = (MDI *)(dbh);
	MYSQL *db = x->msqdb;
	 if (db == NULL) 
	  {
	      fprintf(stderr, "%s\n", mysql_error(db));
	      return MDHIM_DB_ERROR;
	  }
	char *table_name;

	switch(x->msqht){
	case MYSQLDB_HANDLE:
		table_name = (char*)mstore_opts -> db_ptr5;
		break;
	case MYSQLDB_STAT_HANDLE:
		table_name = (char*)mstore_opts -> db_ptr3;
		break;
	default:
		return MDHIM_DB_ERROR;
		break;
	}
	gettimeofday(&start, NULL);
	char *query;    
	//Insert key and value into table
	query = put_value(db, key, key_len, data, data_len, table_name, mstore_opts->key_type);
	//printf("\nThis is the query: \n%s\n", query);

    if  (mysql_real_query(db, query, sizeof(char)*strlen(query)))  {
		int check_er = mysql_errno(db);
	    //printf("\nThis is the error number: %d\n", check_er);
		if (check_er == 1062){
			memset(query, 0, sizeof(char)*strlen(query));
			query = update_value(db, key, key_len, data, data_len, table_name, mstore_opts->key_type);
		//printf("\nThis is the query: \n%s\n", query);


		if  (mysql_real_query(db, query, sizeof(char)*strlen(query)))  {
			//printf("This is the query: %s\n", query);
	   		mlog(MDHIM_SERVER_CRIT, "Error updating key/value in mysql\n");
	    		fprintf(stderr, "%s\n", mysql_error(db));
		return MDHIM_DB_ERROR;
				}
		//else printf("Sucessfully updated key/value in mdhim\n");
		}
	else {
		mlog(MDHIM_SERVER_CRIT, "Error putting key/value in mysql\n");
		    fprintf(stderr, "%s\n", mysql_error(db));
			return MDHIM_DB_ERROR;
			}
    }
	p_res=mysql_store_result(db);
	mysql_free_result(p_res);
	//Report timing
   	gettimeofday(&end, NULL);
    	mlog(MDHIM_SERVER_DBG, "Took: %d seconds to put the record", 
	(int) (end.tv_sec - start.tv_sec));
    return MDHIM_SUCCESS;
}

/////////////////////////////////////////////////////////////////////////////////////////

/**
 * mdhim_mysql_batch_put
 * Stores a multiple keys in the data store
 *
 * @param dbh          in   pointer to the mysql struct which points to the handle
 * @param keys         in   void ** to the key to store
 * @param key_lens     in   int * to the lengths of the keys
 * @param data         in   void ** to the values of the keys
 * @param data_lens    in   int * to the lengths of the value data 
 * @param num_records  in   int for the number of records to insert 
 * @param mstore_opts  in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure */
 
int mdhim_mysql_batch_put(void *dbh, void **keys, int32_t *key_lens, 
			    void **data, int32_t *data_lens, int num_records,
			    struct mdhim_store_opts_t *mstore_opts) {
	MDI *x = (MDI *)(dbh);
	MYSQL *db = (MYSQL *) x->msqdb;
	 if (db == NULL) 
	  {
	      fprintf(stderr, "%s\n", mysql_error(db));
	      return MDHIM_DB_ERROR;
	  }
	int i;
	struct timeval start, end;
		gettimeofday(&start, NULL);	
	char *table_name;
	switch(x->msqht){
	case MYSQLDB_HANDLE:
		table_name = (char*)mstore_opts -> db_ptr5;
		break;
	case MYSQLDB_STAT_HANDLE:
		table_name = (char*)mstore_opts -> db_ptr6;
		break;
	default:
		return MDHIM_DB_ERROR;
		break;
	}
	char *query=NULL;
	//Insert X amount of Keys and Values
	printf("Number records: %d\n", num_records);
	for (i = 0; i < num_records; i++) {
		query = put_value(db, keys[i], key_lens[i], data[i], data_lens[i], table_name, mstore_opts->key_type);
	//printf("\nThis is the query: \n%s\n", query);

	    if  (mysql_real_query(db, query, sizeof(char)*strlen(query)))  {
			int check_er = mysql_errno(db);
		    //printf("\nThis is the error number: %d\n", check_er);
			if (check_er == 1062){
				memset(query, 0, sizeof(char)*strlen(query));
				query = update_value(db, keys[i], key_lens[i], data[i], data_lens[i], table_name, mstore_opts->key_type);
				//printf("\nThis is the query: \n%s\n", query);

			if  (mysql_real_query(db, query, sizeof(char)*strlen(query)))  {
				//printf("This is the query: %s\n", query);
		   		mlog(MDHIM_SERVER_CRIT, "Error updating key/value in mysql\n");
		    		fprintf(stderr, "%s\n", mysql_error(db));
			return MDHIM_DB_ERROR;
					}
			//else printf("Sucessfully updated key/value in mdhim\n");
			}
		else {
		mlog(MDHIM_SERVER_CRIT, "Error putting key/value in mysql\n");
		    fprintf(stderr, "%s\n", mysql_error(db));
			return MDHIM_DB_ERROR;
			}
	    }

		memset(query, 0, sizeof(query));
	}

	//Report timing
	gettimeofday(&end, NULL);
	mlog(MDHIM_SERVER_DBG, "Took: %d seconds to put %d records", 
	     (int) (end.tv_sec - start.tv_sec), num_records);
	return MDHIM_SUCCESS; 
} 

/////////////////////////////////////////////////////////////////////////////////////////


/**
 * mdhim_mysql_get
 * Gets a value, given a key, from the data store
 *
 * @param dbh          in   pointer to the mysql struct which points to the handle
 * @param key          in   void * to the key to retrieve the value of
 * @param key_len      in   length of the key
 * @param data         out  void * to the value of the key
 * @param data_len     out  pointer to length of the value data 
 * @param mstore_opts  in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_mysql_get(void *dbh, void *key, int key_len, void **data, int32_t 	*data_len, struct mdhim_store_opts_t *mstore_opts){	
	MYSQL_RES *data_res;
	int ret = MDHIM_SUCCESS;
 	char get_value[MYSQL_BUFFER+key_len];
	MYSQL_ROW row; 
	void *msl_data;
	char *table_name;
	MDI *x = (MDI *)(dbh);
	MYSQL *db = x->msqdb;
	char *key_copy;

	switch(x->msqht){
	case MYSQLDB_HANDLE:
		table_name = (char*)mstore_opts -> db_ptr5;
		break;
	case MYSQLDB_STAT_HANDLE:
		table_name = (char*)mstore_opts -> db_ptr3;
		break;
	default:
		goto error;
		break;
	}

	 if (db == NULL) 
	  {
	      fprintf(stderr, "%s\n", mysql_error(db));
	      goto error;
	  }

	*data = NULL;
	if (mstore_opts->key_type == MDHIM_STRING_KEY || 
		mstore_opts->key_type == MDHIM_BYTE_KEY) {
		key_copy = sb_key_copy(db, key, key_len);
	}
	
//Create statement to go through and get the value based upon the key

		switch(mstore_opts -> key_type){
			case MDHIM_STRING_KEY:
				snprintf(get_value, sizeof(char)*(MYSQL_BUFFER+ key_len), "Select Value FROM %s WHERE Id = '%s'",table_name, key_copy);
			free(key_copy);
			break;
       			case MDHIM_FLOAT_KEY:
				snprintf(get_value, sizeof(char)*MYSQL_BUFFER, "Select Value FROM %s WHERE Id = %f",table_name, *((float*)key));
				break;
   			case MDHIM_DOUBLE_KEY:
				snprintf(get_value, sizeof(char)*MYSQL_BUFFER, "Select Value FROM %s WHERE Id = %lf",table_name, *((double*)key));
			break;
      			case MDHIM_INT_KEY: 
				snprintf(get_value, sizeof(char)*MYSQL_BUFFER, "Select Value FROM %s WHERE Id = %d",table_name, *((int*)key));
			break;
			case MDHIM_LONG_INT_KEY:
				snprintf(get_value, sizeof(char)*MYSQL_BUFFER, "Select Value FROM %s WHERE Id = %ld",table_name, *((long*)key));
			break;
			case MDHIM_BYTE_KEY:
				snprintf(get_value, sizeof(char)*(MYSQL_BUFFER+ key_len), "Select Value FROM %s WHERE Id = '%s'",table_name, key_copy);
			free(key_copy);
			break; 
		}
//Query and get results if no resuls get an error or else get the value
	//printf("\nThis is the query: \n%s\n", get_value);
	if (mysql_query(db,get_value)) {
		mlog(MDHIM_SERVER_CRIT, "Error getting value in mysql");
		printf("This is the error, get_value failed\n");
		goto error;
	}
	data_res = mysql_store_result(db);
	if (data_res->row_count == 0){
		//mlog(MDHIM_SERVER_CRIT, "No row data selected");
		//printf("This is the error, store row has nothing.\n");
		goto error;
	}
	
	row = mysql_fetch_row(data_res);
	unsigned long *rl = mysql_fetch_lengths(data_res); 
	*data_len = *rl;
	*data = malloc(*data_len);
	msl_data = row[0];
	//printf("\nThis is the row : \n%s\n", (char*)row[0]);
	if (!memcpy(*data, msl_data, *data_len)) {
		mlog(MDHIM_SERVER_CRIT, "Error failed memory copy");
		printf("This is the error, get_value failed\n");
		goto error;

}
	mysql_free_result(data_res);
	return ret;

error:	
	*data=NULL;
	*data_len = 0;
	return MDHIM_DB_ERROR;
}

/////////////////////////////////////////////////////////////////////////////////////////

/**
 * mdhim_mysql_del
 * delete the given key
 *
 * @param dbh         in   pointer to the mysql struct which points to the handle
 * @param key         in   void * for the key to delete
 * @param key_len     in   int for the length of the key
 * @param mstore_opts in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_mysql_del(void *dbh, void *key, int key_len, 
		      struct mdhim_store_opts_t *mstore_opts) {

	MDI *x = (MDI *)(dbh);
	MYSQL *db = x->msqdb;
	 if (db == NULL) 
	  {
	      fprintf(stderr, "%s\n", mysql_error(db));
	      return MDHIM_DB_ERROR;
	  }
	char key_delete[MYSQL_BUFFER+key_len];
	//Delete the Key
	char *table_name;
	char *key_copy;
	switch(x->msqht){
	case MYSQLDB_HANDLE:
		table_name = (char*)mstore_opts -> db_ptr5;
		break;
	case MYSQLDB_STAT_HANDLE:
		table_name = (char*)mstore_opts -> db_ptr3;
		break;
	default:
		return MDHIM_DB_ERROR;
		break;
	}

		if (mstore_opts->key_type == MDHIM_STRING_KEY || 
		mstore_opts->key_type == MDHIM_BYTE_KEY) {
		key_copy = sb_key_copy(db, key, key_len);
	}

			switch(mstore_opts -> key_type){
			case MDHIM_STRING_KEY:
				snprintf(key_delete, sizeof(char)*(MYSQL_BUFFER+key_len),"Delete FROM %s WHERE Id = '%s'",table_name, (char*)key_copy);
			break;
       			case MDHIM_FLOAT_KEY:
				snprintf(key_delete, sizeof(char)*(MYSQL_BUFFER+key_len),"Delete FROM %s WHERE Id = %f",table_name, *((float*)key));
				break;
   			case MDHIM_DOUBLE_KEY:
				snprintf(key_delete, sizeof(char)*(MYSQL_BUFFER+key_len),"Delete FROM %s WHERE Id = %lf",table_name, *((double*)key));
			break;
      			case MDHIM_INT_KEY: 
				snprintf(key_delete, sizeof(char)*(MYSQL_BUFFER+key_len),"Delete FROM %s WHERE Id = %d",table_name, *((int*)key));
			break;
			case MDHIM_LONG_INT_KEY:
				snprintf(key_delete, sizeof(char)*(MYSQL_BUFFER+key_len),"Delete FROM %s WHERE Id = %ld",table_name, *((long*)key));
			break;
			case MDHIM_BYTE_KEY:
				snprintf(key_delete, sizeof(char)*(MYSQL_BUFFER+key_len), "Delete FROM %s WHERE Id = '%s'",table_name,  (char*)key_copy);
			break; 
		}
	if (mysql_query(db,key_delete)) {
		mlog(MDHIM_SERVER_CRIT, "Error deleting key in mysql");
		return MDHIM_DB_ERROR;
	}
	//Reset error variable
	return MDHIM_SUCCESS;
}


/**
 * mdhim_mysql_close
 * Closes the data store
 *
 * @param dbh         in   pointer to the mysql struct which points to the handle
 * @param dbs         in   pointer to the statistics mysql struct which points to the statistics handle
 * @param mstore_opts in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_mysql_close(void *dbh, void *dbs, struct mdhim_store_opts_t *mstore_opts) {
	
	MDI *x = (MDI *)(dbh);
	MDI *y = (MDI *)(dbs);
	MYSQL *db = x->msqdb;
	MYSQL *sdb = y->msqdb;
	/*if(mysql_query(db, "Drop table maindb.mdhim")){
		mlog(MDHIM_SERVER_CRIT, "Error deleting key in mysql");
	}
	if (mysql_query(sdb, "Drop table statsdb.mdhim")){
		mlog(MDHIM_SERVER_CRIT, "Error deleting key in mysql");
	}*/
	mysql_close(db);
	mysql_close(sdb);
	free(x);
	free(y);
	return MDHIM_SUCCESS;
}

/////////////////////////////////////////////////////////////////////////////////////////


/**
 * mdhim_mysql_get_next
 * Gets the next key/value from the data store
 *
 * @param dbh             in   pointer to the mysql struct which points to the handle
 * @param key             out  void ** to the key that we get
 * @param key_len         out  int * to the length of the key 
 * @param data            out  void ** to the value belonging to the key
 * @param data_len        out  int * to the length of the value data 
 * @param mstore_opts in   additional cursor options for the data store layer 
 * 
 */
int mdhim_mysql_get_next(void *dbh, void **key, int *key_len, 
			   void **data, int32_t *data_len, 
			   struct mdhim_store_opts_t *mstore_opts) {
	MDI *x = (MDI *)(dbh);
	MYSQL *db = x->msqdb;
	 if (db == NULL) 
	  {
	      fprintf(stderr, "%s\n", mysql_error(db));
	      return MDHIM_DB_ERROR;
	  }
	int ret = MDHIM_SUCCESS;
	void *old_key, *msl_key, *msl_data;
	struct timeval start, end;
	char get_next[MYSQL_BUFFER+*key_len];
	MYSQL_RES *key_result;
	MYSQL_ROW key_row;
	char *table_name;

	    /*int i = 0;
    char hostname[256];
    gethostname(hostname, sizeof(hostname));
    printf("PID %d on %s ready for attach\n", getpid(), hostname);
    fflush(stdout);
    while (0 == i)
        sleep(5); */


	switch(x->msqht){
	case MYSQLDB_HANDLE:
		table_name = (char*)mstore_opts -> db_ptr5;
		break;
	case MYSQLDB_STAT_HANDLE:
		table_name = "mdhim";//(char*)mstore_opts -> db_ptr6;
		break;
	default:
		goto error;
		break;
	}
	
	gettimeofday(&start, NULL);	
	old_key = *key;
		char *key_copy;
	if (mstore_opts->key_type == MDHIM_STRING_KEY || 
		mstore_opts->key_type == MDHIM_BYTE_KEY ) {
		if (old_key) key_copy = sb_key_copy(db, (char*)old_key, *key_len);
	} 
	*key = NULL;
	*key_len = 0;
	*data = NULL;
	*data_len = 0;
	
	//Get the Key from the tables and if there was no old key, use the first one.
	if (!old_key){
		snprintf(get_next, sizeof(char)*(MYSQL_BUFFER+*key_len), "Select * From %s where Id = (Select min(Id) from %s)", table_name, table_name);
		if(mysql_query(db, get_next)) { 
			mlog(MDHIM_SERVER_DBG2, "Could not get the next key/value");
			goto error;
		}
	
	} else {
			switch(mstore_opts -> key_type){
			case MDHIM_STRING_KEY:
				snprintf(get_next, sizeof(char)*(MYSQL_BUFFER+*key_len),"Select * From %s where Id = (Select min(Id) from %s where Id >'%s')", table_name,table_name, key_copy);
			free(key_copy);
			break;
       			case MDHIM_FLOAT_KEY:
				snprintf(get_next,  sizeof(char)*(MYSQL_BUFFER+*key_len), "Select * From %s where Id = (Select min(Id) from %s where Id >%f)", table_name,table_name, *((float*)old_key));
				break;
   			case MDHIM_DOUBLE_KEY:
				snprintf(get_next, sizeof(char)*(MYSQL_BUFFER+*key_len), "Select * From %s where Id = (Select min(Id) from %s where Id >%lf)", table_name,table_name, *((double*)old_key));
			break;
      			case MDHIM_INT_KEY: 
				snprintf(get_next, sizeof(char)*(MYSQL_BUFFER+*key_len), "Select * From %s where Id = (Select min(Id) from %s where Id >%d)", table_name,table_name, *((int*)old_key));
			break;
			case MDHIM_LONG_INT_KEY:
				snprintf(get_next, sizeof(char)*(MYSQL_BUFFER+*key_len), "Select * From %s where Id = (Select min(Id) from %s where Id >%ld)", table_name,table_name, *((long*)old_key));
			break;
			case MDHIM_BYTE_KEY:
				snprintf(get_next, sizeof(char)*(MYSQL_BUFFER+*key_len), "Select * From %s where Id = (Select min(Id) from %s where Id > '%s')", table_name,table_name,  key_copy);
			free(key_copy);
			break; 
		}
		//snprintf(get_next, "Select * From %s where Id = (Select min(Id) from %s where Id >%d)", table_name, table_name, *(int*)old_key);
	if(mysql_query(db, get_next)) {  
			mlog(MDHIM_SERVER_DBG2, "Could not get the next key/value");
			goto error;
			}
	}


	//STore the result, you MUST use mysql_store_result because of it being parallel 
	key_result = mysql_store_result(db);
	
	if (key_result->row_count == 0) {
      		mlog(MDHIM_SERVER_DBG2, "Could not get mysql result");
		goto error;
 			 }
	key_row = mysql_fetch_row(key_result);
	unsigned long *dl = mysql_fetch_lengths(key_result);
	int r_size;
	msl_key = str_to_key(key_row, mstore_opts->key_type, &r_size);
	*key_len = r_size; 
	*data_len = dl[1];
	msl_data = key_row[1];

	//Allocate data and key to mdhim program
	if (key_row && *key_row) {
		*key = malloc(*key_len+1);
		memset(*key, 0, *key_len+1);
		memcpy(*key, msl_key, *key_len);
		*data = malloc(*data_len);
		memset(*data, 0, *data_len);
		memcpy(*data, msl_data, *data_len);
		//printf("\nCopied here\n");
		
	} else {
		*key = NULL;
		*key_len = 0;
		*data = NULL;
		*data_len = 0;
		printf("\nNot Copied here\n");
	}
	gettimeofday(&end, NULL);
	mlog(MDHIM_SERVER_DBG, "Took: %d seconds to get the next record", 
	(int) (end.tv_sec - start.tv_sec));
	

	return ret;

error:
	*key = NULL;
	*key_len = 0;
	*data = NULL;
	*data_len = 0;
	return MDHIM_DB_ERROR;

}


/////////////////////////////////////////////////////////////////////////////////////////
/**
 * mdhim_mysql_get_prev
 * Gets the previous key/value from the data store
 *
 * @param dbh             in   pointer to the unqlite db handle
 * @param key             out  void ** to the key that we get
 * @param key_len         out  int * to the length of the key 
 * @param data            out  void ** to the value belonging to the key
 * @param data_len        out  int * to the length of the value data 
 * @param mstore_opts in   additional cursor options for the data store layer 
 * 
 */

int mdhim_mysql_get_prev(void *dbh, void **key, int *key_len, 
			   void **data, int32_t *data_len,
			   struct mdhim_store_opts_t *mstore_opts){
	MDI *x = (MDI *)(dbh);
	MYSQL *db = x->msqdb;
	 if (db == NULL) 
	  {
	      fprintf(stderr, "%s\n", mysql_error(db));
	      return MDHIM_DB_ERROR;
	  }
	int ret = MDHIM_SUCCESS;
	void *old_key;
	struct timeval start, end;
	char get_prev[MYSQL_BUFFER+*key_len];
	MYSQL_RES *key_result;
	MYSQL_ROW key_row;
	void *msl_data;
	void *msl_key;
	char *table_name;
	//Init the data to return
	gettimeofday(&start, NULL);
	old_key = *key;
	char *key_copy;
	if (mstore_opts->key_type == MDHIM_STRING_KEY || 
		mstore_opts->key_type == MDHIM_BYTE_KEY) {
		if (old_key) key_copy = sb_key_copy(db, (char*)old_key, *key_len);
	}
	//Start with Keys/data being null 
	*key = NULL;
	*key_len = 0;
	*data = NULL;
	*data_len = 0;

	switch(x->msqht){
	case MYSQLDB_HANDLE:
		table_name = (char*)mstore_opts -> db_ptr5;
		break;
	case MYSQLDB_STAT_HANDLE:
		table_name = (char*)mstore_opts -> db_ptr6;
		break;
	default:
		goto error;
		break;
	}

	
	//Get the Key/Value from the tables and if there was no old key, use the last one.

	if (!old_key){
		snprintf(get_prev, sizeof(char)*(MYSQL_BUFFER+*key_len),"Select * from %s where Id = (Select max(Id) From %s)", table_name,table_name);
		if(mysql_query(db, get_prev)) { 
			mlog(MDHIM_SERVER_DBG2, "Could not get the previous key/value");
			goto error;
		}
	
	} else {

		switch(mstore_opts -> key_type){
			case MDHIM_STRING_KEY:
				snprintf(get_prev, sizeof(char)*(MYSQL_BUFFER+*key_len), "Select * From %s where Id = (Select max(Id) from %s where Id < '%s')", table_name,table_name, key_copy);
			free(key_copy);
			break;
       			case MDHIM_FLOAT_KEY:
				snprintf(get_prev, sizeof(char)*(MYSQL_BUFFER+*key_len),"Select * From %s where Id = (Select max(Id) from %s where Id <%f)", table_name,table_name, *((float*)old_key));
				break;
   			case MDHIM_DOUBLE_KEY:
				snprintf(get_prev, sizeof(char)*(MYSQL_BUFFER+*key_len),"Select * From %s where Id = (Select max(Id) from %s where Id <%lf)", table_name,table_name, *((double*)old_key));
			break;
      			case MDHIM_INT_KEY: 
				snprintf(get_prev, sizeof(char)*(MYSQL_BUFFER+*key_len),"Select * From %s where Id = (Select max(Id) from %s where Id <%d)", table_name,table_name, *((int*)old_key));
			break;
			case MDHIM_LONG_INT_KEY:
				snprintf(get_prev, sizeof(char)*(MYSQL_BUFFER+*key_len),"Select * From %s where Id = (Select max(Id) from %s where Id <%ld)", table_name,table_name, *((long*)old_key));
			break;
			case MDHIM_BYTE_KEY:
				snprintf(get_prev, sizeof(char)*(MYSQL_BUFFER+*key_len),"Select * From %s where Id = (Select max(Id) from %s where Id < '%s')", table_name,table_name,  key_copy);
			free(key_copy);
			break; 
		}

	
	//Query the database 
	if(mysql_query(db, get_prev)) {  
			mlog(MDHIM_SERVER_DBG2, "Could not get the previous key/value");
			goto error;
			}
	}
	//STore the result, you MUST use mysql_store_result because of it being parallel 
	key_result = mysql_store_result(db);
	
	if (key_result->row_count == 0) {
      		mlog(MDHIM_SERVER_DBG2, "Could not get mysql result");
		goto error;
 			 }
	//Fetch row and get data from database
	key_row = mysql_fetch_row(key_result);
	unsigned long *dl = mysql_fetch_lengths(key_result);
	int r_size;
	msl_key = str_to_key(key_row, mstore_opts->key_type, &r_size);
	*key_len = r_size; 
	*data_len = dl[1];
	msl_data = key_row[1];

	//Allocate data and key to mdhim program
	if (key_row && *key_row) {
		*key = malloc(*key_len+1);
		memset(*key, 0, *key_len+1);
		memcpy(*key, msl_key, *key_len);
		*data = malloc(*data_len);
		memset(*data, 0, *data_len);
		memcpy(*data, msl_data, *data_len);
		//printf("\nCopied here\n");
		
	}  else {
		*key = NULL;
		*key_len = 0;
		*data = NULL;
		*data_len = 0;
	}

	mysql_free_result(key_result);
	//End timing 
	gettimeofday(&end, NULL);
	mlog(MDHIM_SERVER_DBG, "Took: %d seconds to get the prev record", 
	     (int) (end.tv_sec - start.tv_sec));
	return ret;
error:
	*key = NULL;
	*key_len = 0;
	*data = NULL;
	*data_len = 0;
	return MDHIM_DB_ERROR;
}


int mdhim_mysql_commit(void *dbh) {
	return MDHIM_SUCCESS;
}
