#ifndef      __MESSAGES_H
#define      __MESSAGES_H

/* Message Types */

//Put a single key in the data store
#define MDHIM_PUT 1
//Put multiple keys in the data store at one time
#define MDHIM_BULK_PUT 2
//Get a single key from the data store
#define MDHIM_GET 3
//Get multiple keys from the data store at one time
#define MDHIM_BULK_GET 4
//Delete a single key from the data store
#define MDHIM_DEL 5
//Delete multiple keys from the data store at once
#define MDHIM_BULK_DEL 6
//Generic receive message
#define MDHIM_RECV 7
//Receive message for a get request
#define MDHIM_RECV_GET 8
//Receive message for a bulk get request
#define MDHIM_RECV_BULK_GET 9

/* Operations for getting a key/value */
//Get the value for the specified key
#define MDHIM_GET_VAL       0
//Get the next key and value
#define MDHIM_GET_NEXT   1
//Get the previous key and value
#define MDHIM_GET_PREV   2
//Get the first key and value
#define MDHIM_GET_FIRST  3
//Get the last key and value
#define MDHIM_GET_LAST   4

#define RANGESRV_WORK 1
#define RANGESRV_INFO 2

//The maximum bulk operations we will allowed at one time
#define MAX_BULK_OPS 5

struct mdhim_t;

/* Base message */
struct mdhim_basem_t {
	//Message type
	int mtype; 
};

/* Put message */
struct mdhim_putm_t {
	int mtype;
	int key_len;
	int64_t data_len;
	int server_rank;
	void *key;
	void *data;
};

/*Bulk put message */
struct mdhim_bputm_t {
	int mtype;
	int num_records;
	int server_rank;
	int key1_len;
	int64_t data1_len;
	int key2_len;
	int64_t data2_len;
	int key3_len;
	int64_t data3_len;
	int key4_len;
	int64_t data4_len;
	int key5_len;
	int64_t data5_len;
	void *key1;
	void *data1;
	void *key2;
	void *data2;
	void *key3;
	void *data3;
	void *key4;
	void *data4;
	void *key5;
	void *data5;
};

/*Get record message */
struct mdhim_getm_t {
	int mtype;  
	//Operation type e.g., MDHIM_GET_VAL, MDHIM_GET_NEXT, MDHIM_GET_PREV
	int op;  
	int key_len;
	int server_rank;
	void *key;
};

/*Bulk get record message */
struct mdhim_bgetm_t {
	int mtype;  
	//Operation type e.g., MDHIM_GET_VAL, MDHIM_GET_NEXT, MDHIM_GET_PREV
	int op;
	//Number of records to retreive
	int num_keys;
	int server_rank;
	int key1_len;
	int key2_len;
	int key3_len;
	int key4_len;
	int key5_len;
	void *key1;
	void *key2;
	void *key3;
	void *key4;
	void *key5;
};

/* delete message */
struct mdhim_delm_t {
	int mtype;
	int key_len; 
	int server_rank;
	void *key;
};

/*bulk delete record message */
struct mdhim_bdelm_t {
	int mtype;  
	int num_keys;
	int server_rank;
	int key1_len;
	int key2_len;
	int key3_len;
	int key4_len;
	int key5_len;
	void *key1;
	void *key2;
	void *key3;
	void *key4;
	void *key5;
};

/*Get receive message */
struct mdhim_rm_t {
	int mtype;  
	int error;
};

/*Get receive message */
struct mdhim_getrm_t {
	int mtype;
	int error;
	int key_len;
	int value_len;
	void *key;
	void *value;
};

/*Bulk get receive message */
struct mdhim_bgetrm_t {
	int mtype;
	int error;
	int num_records;
	int server_rank;
	int key1_len;
	int64_t data1_len;
	int key2_len;
	int64_t data2_len;
	int key3_len;
	int64_t data3_len;
	int key4_len;
	int64_t data4_len;
	int key5_len;
	int64_t data5_len;
	void *key1;
	void *data1;
	void *key2;
	void *data2;
	void *key3;
	void *data3;
	void *key4;
	void *data4;
	void *key5;
	void *data5;
};

int send_message(struct mdhim_t *md, int dest, void *message);
int receive_message(struct mdhim_t *md, int src, void *message);
#endif
