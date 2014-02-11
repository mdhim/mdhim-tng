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

/* create db

* Helps to create the database for mysql if it does not exist 
* @param dbmn  Database name that will be used in the function
* @param db Database connection used to create the database
* Exit(1) is used to exit out if you don't have a connection

*/

void create_db(char *dbmn, MYSQL *db){
	char q_create[256];
		sprintf(q_create, "USE %s", dbmn);
	
	 if (mysql_query(db, q_create)) 
	  {
		memset(q_create, 0, strlen(q_create));
		sprintf(q_create, "CREATE DATABASE %s", dbmn);
		printf("\nCREATE DATABASE %s\n",dbmn);
		if(mysql_query(db,q_create)) {
	     		fprintf(stderr, "%s\n", mysql_error(db));
      			mysql_close(db);
	      		exit(1);
	  	} 
		memset(q_create, 0, strlen(q_create));
		sprintf(q_create, "USE %s", dbmn);
		if(mysql_query(db,q_create)) {
	     		fprintf(stderr, "%s\n", mysql_error(db));
      			mysql_close(db);
	      		exit(1);
		} else printf("DATABASE IN USE");
	  }
		 

}

/* create_table fot table name 
 * @parama dbmt  Database table name to create the table/check to see if table is there
 * @param db_name Database name for accessing the databaase 
 * @param k_type Key type that is used to create the table with the proper type for the key/value storage
 **/
void create_table(char *dbmt, MYSQL *db, char* db_name, int k_type){
	char name[256];
	sprintf(name, "SHOW TABLES LIKE \'%s\'", dbmt);
  //Create table and if it's there stop
	  if(mysql_query(db, name)){
		fprintf(stderr, "%s\n", mysql_error(db));
		}
	  MYSQL_RES *table_res=mysql_store_result(db);
	  int row_count = table_res->row_count;
	  if(row_count == 0)
		{ 
		memset(name, 0, strlen(name));
		switch(k_type){
			case MDHIM_STRING_KEY:
			sprintf(name, "CREATE TABLE %s( Id VARCHAR(700) PRIMARY KEY, Value LONGBLOB)", dbmt);
			break;
       			case MDHIM_FLOAT_KEY:
				sprintf(name, "CREATE TABLE %s( Id FLOAT PRIMARY KEY, Value LONGBLOB)", dbmt);
				break;
   			case MDHIM_DOUBLE_KEY:
				sprintf(name, "CREATE TABLE %s( Id DOUBLE PRIMARY KEY, Value LONGBLOB)", dbmt);
			break;
      			case MDHIM_INT_KEY: 
				sprintf(name, "CREATE TABLE %s( Id BIGINT PRIMARY KEY, Value LONGBLOB)", dbmt);
			break;
			case MDHIM_LONG_INT_KEY:
				sprintf(name, "CREATE TABLE %s( Id BIGINT PRIMARY KEY, Value LONGBLOB)", dbmt);
			break;
			case MDHIM_BYTE_KEY:
				sprintf(name, "CREATE TABLE %s( Id VARCHAR(700) PRIMARY KEY, Value LONGBLOB)", dbmt);
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
 */

void * str_to_key(MYSQL_ROW key_row, int key_type, int * size){
	void * ret;
		switch(key_type){
			case MDHIM_STRING_KEY:
				*size= sizeof(key_row[0]);
				ret = malloc(*size);
				*(char*)ret=key_row[0];
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
				*size= sizeof(key_row[0]);
				ret = malloc(*size);
				*(char*)ret=key_row[0];
			break; 
		}

	return ret;

}

/**
 * mdhim_leveldb_open
 * Opens the database
 *
 * @param dbh            in   double pointer to the leveldb handle
 * @param dbs            in   double pointer to the leveldb statistics db handle 
 * @param path           in   path to the database file
 * @param flags          in   flags for opening the data store
 * @param mstore_opts    in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */

	

int mdhim_mysql_open(void **dbh, void **dbs, char *path, int flags,
		       struct mdhim_store_opts_t *mstore_opts) {

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
	char *db_mysql_host = "localhost";//mstore_opts -> db_ptr1;
	char *db_mysql_user = "root";//mstore_opts -> db_ptr2;
	char *sdb_mysql_user = "stater";
	char *db_mysql_pswd = "pass";//mstore_opts -> db_ptr3;
	//Abstracting the host, usernames, and password
	char *db_mysql_name = "testdb";//mstore_opts -> db_ptr4; //Abstracting Database
	char *db_mysql_table = "mdhim";
	char *sdb_mysql_name = "stestdb";//mstore_opts -> db_ptr4; //Abstracting Statsics Database 
	char *sdb_mysql_table = "mdhim"; 


	//connect to the Database
	if (mysql_real_connect(db, db_mysql_host, db_mysql_user, db_mysql_pswd, 
          NULL, 0, NULL, 0) == NULL){
     		fprintf(stderr, "%s\n", mysql_error(db));
      		mysql_close(db);
      		return MDHIM_DB_ERROR;
  		}  
		//connect to the Database
	if (mysql_real_connect(sdb, db_mysql_host, sdb_mysql_user, db_mysql_pswd, 
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
	*((MYSQL**)dbh) = db;
	*((MYSQL**)dbs) = sdb;


		return MDHIM_SUCCESS;

}
/////////////////////////////////////////////////////////////////////////////////////////

/**
 * mdhim_leveldb_put
 * Stores a single key in the data store
 *
 * @param dbh         in   pointer to the leveldb handle
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

	MYSQL *db = (MYSQL *)dbh;
	 if (db == NULL) 
	  {
	      fprintf(stderr, "%s\n", mysql_error(db));
	      return MDHIM_DB_ERROR;
	  }
	gettimeofday(&start, NULL);	      
	
		  char chunk[2*data_len+1];
  mysql_real_escape_string(db, chunk, data, data_len);
 // mysql_real_escape_string(db, key_insert,key,key_len);
	
  char *st = "Insert INTO mdhim (Id, Value) VALUES (%d, '%s');";
  size_t st_len = strlen(st);
  size_t size = 2*data_len+1 + 2*key_len+1;//strlen(chunk)+strlen(key_insert)+1;
  char query[st_len + size]; 
  int len = snprintf(query, st_len + size, st, *((int*)key), chunk);
	
	//printf("Here is the query:\n%s\n", query);
	//Insert key and value into table
    if  (mysql_real_query(db, query, len))  {
	    mlog(MDHIM_SERVER_CRIT, "Error putting key/value in mysql\n");
	    fprintf(stderr, "%s\n", mysql_error(db));
	    return MDHIM_DB_ERROR;
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
 * mdhim_leveldb_batch_put
 * Stores a multiple keys in the data store
 *
 * @param dbh          in   pointer to the leveldb handle
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
	MYSQL *db = (MYSQL *) dbh;
	 if (db == NULL) 
	  {
	      fprintf(stderr, "%s\n", mysql_error(db));
	      return MDHIM_DB_ERROR;
	  }
	char key_insert[256];
	int i;
	struct timeval start, end;
		gettimeofday(&start, NULL);	      

	//Insert X amount of Keys and Values
	printf("Number records: %d\n", num_records);
	sleep(5);
	for (i = 0; i < num_records; i++) {
	sprintf(key_insert, "Insert INTO mdhim (Id, Value) VALUES (%d, %d) ", (int)keys[i], (int)data[i]);
	
    if  (mysql_query(db,key_insert))  {
	    mlog(MDHIM_SERVER_CRIT, "Error putting key in mysql");
	    return MDHIM_DB_ERROR;
    		}
	 memset(key_insert, 0, sizeof(key_insert));
	}

	//Report timing
	gettimeofday(&end, NULL);
	mlog(MDHIM_SERVER_DBG, "Took: %d seconds to put %d records", 
	     (int) (end.tv_sec - start.tv_sec), num_records);
	return MDHIM_SUCCESS; 
} 

/////////////////////////////////////////////////////////////////////////////////////////


/**
 * mdhim_leveldb_get
 * Gets a value, given a key, from the data store
 *
 * @param dbh          in   pointer to the leveldb db handle
 * @param key          in   void * to the key to retrieve the value of
 * @param key_len      in   length of the key
 * @param data         out  void * to the value of the key
 * @param data_len     out  pointer to length of the value data 
 * @param mstore_opts  in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_mysql_get(void *dbh, void *key, int key_len, void **data, int32_t 	*data_len, struct mdhim_store_opts_t *mstore_opts){
		MYSQL *db = (MYSQL *)dbh;	
	MYSQL_RES *data_res;
	int ret = MDHIM_SUCCESS;
 	char get_value[256];
	MYSQL_ROW row; 
	void *msl_data;

	 if (dbh == NULL) 
	  {
	      fprintf(stderr, "%s\n", mysql_error(db));
	      goto error;
	  }

//Go ahead and make the check for the primary key's data type and length
//Then check the key's Data type and lenght
//If mismatch occurs exit out!
	*data = NULL;
	sprintf(get_value, "Select Value FROM mdhim WHERE Id = %d", *((int*)key));
	//printf("Here is get_value: \n%s\n", get_value);
	if (mysql_query(db,get_value)) {
		mlog(MDHIM_SERVER_CRIT, "Error getting value in mysql");
		goto error;
	}
	data_res = mysql_store_result(db);
	if (data_res->row_count == 0){
		mlog(MDHIM_SERVER_CRIT, "No row data selected");
		goto error;
	}
	
	row = mysql_fetch_row(data_res);
	*data_len = sizeof(row[0]);
	*data = malloc(*data_len);
	msl_data = row[0];
	memcpy(*data, msl_data, *data_len);
	mysql_free_result(data_res);
	return ret;

error:	
	*data=NULL;
	*data_len = 0;
	return MDHIM_DB_ERROR;
}

/////////////////////////////////////////////////////////////////////////////////////////

/**
 * mdhim_leveldb_del
 * delete the given key
 *
 * @param dbh         in   pointer to the leveldb db handle
 * @param key         in   void * for the key to delete
 * @param key_len     in   int for the length of the key
 * @param mstore_opts in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_mysql_del(void *dbh, void *key, int key_len, 
		      struct mdhim_store_opts_t *mstore_opts) {

	MYSQL *db = (MYSQL *) dbh;
	 if (db == NULL) 
	  {
	      fprintf(stderr, "%s\n", mysql_error(db));
	      return MDHIM_DB_ERROR;
	  }
	char key_delete[256];
	//Delete the Key
	sprintf(key_delete, "Delete FROM mdhim WHERE Id = %d",*((int*)key));
	if (mysql_query(db,key_delete)) {
		mlog(MDHIM_SERVER_CRIT, "Error deleting key in mysql");
		return MDHIM_DB_ERROR;
	}
	//Reset error variable
	return MDHIM_SUCCESS;
}


/**
 * mdhim_leveldb_close
 * Closes the data store
 *
 * @param dbh         in   pointer to the leveldb db handle 
 * @param dbs         in   pointer to the leveldb statistics db handle 
 * @param mstore_opts in   additional options for the data store layer 
 * 
 * @return MDHIM_SUCCESS on success or MDHIM_DB_ERROR on failure
 */
int mdhim_mysql_close(void *dbh, void *dbs, struct mdhim_store_opts_t *mstore_opts) {
	MYSQL *db = (MYSQL*) dbh;
	MYSQL *sdb = (MYSQL *) dbs;
	  mysql_close(db);
	mysql_close(sdb);
	return MDHIM_SUCCESS;
}

/////////////////////////////////////////////////////////////////////////////////////////


/**
 * mdhim_leveldb_get_next
 * Gets the next key/value from the data store
 *
 * @param dbh             in   pointer to the unqlite db handle
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
	MYSQL *db = (MYSQL*)dbh;
	 if (db == NULL) 
	  {
	      fprintf(stderr, "%s\n", mysql_error(db));
	      return MDHIM_DB_ERROR;
	  }
	int ret = MDHIM_SUCCESS;
	int key_t;
	void *old_key, *msl_key, *msl_data;
	struct timeval start, end;
	char get_next[256];
	MYSQL_RES *key_result;
	MYSQL_ROW key_row;
	
	gettimeofday(&start, NULL);	
	old_key = *key;
	*key = NULL;
	*key_len = 0;
	*data = NULL;
	*data_len = 0;

	//Get the Key from the tables and if there was no old key, use the first one.
	if (!old_key){
		if(mysql_query(db, "Select * From mdhim where Id = (Select min(Id) from mdhim)")) { 
			mlog(MDHIM_SERVER_DBG2, "Could not get the next key/value");
			goto error;
		}
	
	} else {
		sprintf(get_next, "Select * From mdhim where Id = (Select min(Id) from mdhim where Id >%d)", *(int*)old_key);
	
	//If the user didn't supply a key, then seek to the first
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
	//printf("Feting row and lenghts");
	key_row = mysql_fetch_row(key_result);
	int r_size;
	msl_key = str_to_key(key_row, mstore_opts->key_type, &r_size);
	//key_t =  strtol(key_row[0],NULL,10);
	*key_len = r_size; //sizeof(strtol(key_row[0],NULL,10));
	*data_len = sizeof(key_row[1]);
	//msl_key = &key_t;
	msl_data = key_row[1];

	//Allocate data and key to mdhim program
	if (key_row && *key_row) {
		*key = malloc(*key_len);
		memcpy(*key, msl_key, 4);
		*data = malloc(*data_len);
		memcpy(*data, msl_data, *data_len);
		
	} else {
		*key = NULL;
		*key_len = 0;
		*data = NULL;
		*data_len = 0;
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
 * mdhim_leveldb_get_prev
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
	MYSQL *db = (MYSQL*)dbh;
	 if (db == NULL) 
	  {
	      fprintf(stderr, "%s\n", mysql_error(db));
	      return MDHIM_DB_ERROR;
	  }
	int ret = MDHIM_SUCCESS;
	void *old_key;
	struct timeval start, end;
	char get_prev[256];
	MYSQL_RES *key_result;
	MYSQL_ROW key_row;
	void *msl_data;
	void *msl_key;
	int key_t;
	//Init the data to return

	gettimeofday(&start, NULL);
	old_key = *key;
	
	//Start with Keys/data being null 
	*key = NULL;
	*key_len = 0;
	*data = NULL;
	*data_len = 0;

	
	//Get the Key/Value from the tables and if there was no old key, use the last one.

	if (!old_key){
		if(mysql_query(db, "Select * from mdhim where Id = (Select max(Id) From mdhim)")) { 
			mlog(MDHIM_SERVER_DBG2, "Could not get the previous key/value");
			goto error;
		}
	
	} else {
		sprintf(get_prev, "Select * From mdhim where Id = (Select max(Id) from mdhim where Id <%d)", *(int*)old_key);
	
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
	int r_size;
	msl_key = str_to_key(key_row, mstore_opts->key_type, &r_size);
	//key_t =  strtol(key_row[0],NULL,10);
	*key_len = r_size; //sizeof(strtol(key_row[0],NULL,10));
	*data_len = sizeof(key_row[1]);
	//msl_key = &key_t;
	msl_data = key_row[1];

	//Allocate data and key to mdhim program
	if (key_row && *key_row) {
		*key = malloc(*key_len);
		memcpy(*key, msl_key,*key_len);
		*data = malloc(*data_len);
		memcpy(*data, msl_data, *data_len);
	} else {
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
/*int mdhim_mysql_iter_free(void **iterator) {
	return MDHIM_SUCCESS;
}
*/


int mdhim_mysql_commit(void *dbh) {
	return MDHIM_SUCCESS;
}
