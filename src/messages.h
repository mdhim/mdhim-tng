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

/* Base message */
struct mdhim_basem_t {
  //Message type
  int mtype; 
};

/* Put message */
struct mdhim_putm_t {
  int mtype;
  char *key;
  int key_len;
  int key_type;
  char *data;
  int data_len;
  int data_type;
};

/*Bulk put message */
struct mdhim_bputm_t {
  int mtype;
  char **keys;
  int *key_lens;
};

/*Get record message */
struct mdhim_getm_t {
  int mtype;  
  //Operation type e.g., MDHIM_GET_VAL, MDHIM_GET_NEXT, MDHIM_GET_PREV
  int op;  
};

/*Bulk get record message */
struct mdhim_bgetm_t {
  int mtype;  
  //Operation type e.g., MDHIM_GET_VAL, MDHIM_GET_NEXT, MDHIM_GET_PREV
  int op;
  //Number of records to retreive
  int num_records;
};

/* delete message */
struct mdhim_delm_t {
  int mtype;
  char *key;
  int key_len; 
};

/*bulk delete record message */
struct mdhim_bdelm_t {
  int mtype;  
  char **keys;
  int *key_lens;
};

int send_message(struct mdhim_t *md, int dest, void *message);
void *receive_message(struct mdhim_t *md, int src);
#endif
