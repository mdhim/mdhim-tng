#ifndef      __MESSAGES_H
#define      __MESSAGES_H

#include "range_server.h"

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
//Generic bulk receive message
#define MDHIM_RECV_BULK 10

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

#define MAX_BULK_OPS 100

//Maximum size of messages allowed
#define MDHIM_MAX_MSG_SIZE 2147483647 //At most send/receive 2GB
struct mdhim_t;

/* Base message */
struct mdhim_basem_t {
	//Message type
	int mtype; 
};

/* Put message */
struct mdhim_putm_t {
	int mtype;
	void *key;
	int key_len;
	void *data;
	int64_t data_len;
	int server_rank;
};

/* Bulk put message */
struct mdhim_bputm_t {
	int mtype;
	void **keys;
	int *key_lens;
	void **data;
	int64_t *data_lens;
	int num_keys;
	int server_rank;
};

/* Get record message */
struct mdhim_getm_t {
	int mtype;  
	//Operation type e.g., MDHIM_GET_VAL, MDHIM_GET_NEXT, MDHIM_GET_PREV
	int op;  
	void *key;
	int key_len;
	int server_rank;
};

/* Bulk get record message */
struct mdhim_bgetm_t {
	int mtype;  
	//Operation type e.g., MDHIM_GET_VAL, MDHIM_GET_NEXT, MDHIM_GET_PREV
	int op;
	void **keys;
	int *key_lens;
	int num_keys;
	int server_rank;
};

/* Delete message */
struct mdhim_delm_t {
	int mtype;
	void *key;
	int key_len; 
	int server_rank;
};

/* Bulk delete record message */
struct mdhim_bdelm_t {
	int mtype;  
	void **keys;
	int *key_lens;
	int num_keys;
	int server_rank;
};

/* Range server info message */
struct mdhim_rsi_t {
	//The start range (inclusive)
	uint64_t start_range;
	//The end range (inclusive)
	uint64_t end_range;
};

/* Generic receive message */
struct mdhim_rm_t {
	int mtype;  
	int error;
	int server_rank;
};

/* Get receive message */
struct mdhim_getrm_t {
	int mtype;
	int error;
	int server_rank;
	void *key;
	int key_len;
	void *value;
	int value_len;
};

/* Bulk get receive message */
struct mdhim_bgetrm_t {
	int mtype;
	int error;
	int server_rank;
	void **keys;
	int *key_lens;
	void **values;
	int *value_lens;
	int num_records;
	struct mdhim_bgetrm_t *next;
};

/* Bulk generic receive message */
struct mdhim_brm_t {
	int mtype;
	int error;
	int server_rank;
	struct mdhim_brm_t *next;
};


int send_message(struct mdhim_t *md, int dest, void *message);
int receive_message(struct mdhim_t *md, int src, void *message);

int pack_put_message(struct mdhim_t *md, struct mdhim_putm_t *pm, void *message);
int pack_bput_message(struct mdhim_t *md, struct mdhim_bputm_t *bpm, void *message);
int unpack_put_message(struct mdhim_t *md, void *message, int mesg_size, struct mdhim_putm_t *pm);
int unpack_bput_message(struct mdhim_t *md, void *message, int mesg_size, struct mdhim_bputm_t *bpm);

int pack_get_message(struct mdhim_t *md, struct mdhim_getm_t *gm, void *message);
int pack_bget_message(struct mdhim_t *md, struct mdhim_bgetm_t *bgm, void *message);
int unpack_get_message(struct mdhim_t *md, void *message, int mesg_size, struct mdhim_getm_t *gm);
int unpack_bget_message(struct mdhim_t *md, void *message, int mesg_size, struct mdhim_bgetm_t *bgm);

int pack_getrm_message(struct mdhim_t *md, struct mdhim_getrm_t *grm, void *message);
int pack_bgetrm_message(struct mdhim_t *md, struct mdhim_bgetrm_t *bgrm, void *message);
int unpack_getrm_message(struct mdhim_t *md, void *message, int mesg_size, struct mdhim_getrm_t *grm);
int unpack_bgetrm_message(struct mdhim_t *md, void *message, int mesg_size, struct mdhim_bgetrm_t *bgrm);

int pack_del_message(struct mdhim_t *md, struct mdhim_delm_t *dm, void *message);
int pack_bdel_message(struct mdhim_t *md, struct mdhim_bdelm_t *bdm, void *message);
int unpack_del_message(struct mdhim_t *md, void *message, int mesg_size, struct mdhim_delm_t *dm);
int unpack_bdel_message(struct mdhim_t *md, void *message, int mesg_size, struct mdhim_bdelm_t *bdm);

int pack_return_message(struct mdhim_t *md, struct mdhim_rm_t *rm, void *message);
int unpack_return_message(struct mdhim_t *md, void *message, struct mdhim_rm_t *rm);

struct rangesrv_info *get_rangesrvs(struct mdhim_t *md);

#endif
