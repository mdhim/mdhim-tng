/* Message Types */
#define MDHIM_PUT 1
#define MDHIM_BULK_PUT 2
#define MDHIM_GET 3
#define MDHIM_BULK_GET 4

#define MDHIM_GET        0
#define MDHIM_GET_NEXT   1
#define MDHIM_GET_PREV   2
#define MDHIM_GET_FIRST  3
#define MDHIM_GET_LAST   4

/* Base message */
typedef struct mdhim_basem_t {
  //Message type
  int mtype; 
} mdhim_basem_t;

/* Put message */
typedef struct mdhim_putm_t {
  int mtype;
  char *key;
  int key_len;
  int key_type;
  char *data;
  int data_len;
  int data_type;
} mdhim_putm_t;

/*Bulk put message */
typedef struct mdhim_bputm_t {
  int mtype;
  char **keys;
  int *key_lens;
} mdhim_bputm_t;

/*Get record message */
typedef struct mdhim_getm_t {
  int mtype;  
  //Operation type i.e., MDHIM_GET, MDHIM_GET_NEXT, MDHIM_GET_PREV
  int op;  
} mdhim_getm_t;

/*Bulk get record message */
typedef struct mdhim_bgetm_t {
  int mtype;  
  //Operation type i.e., MDHIM_GET, MDHIM_GET_NEXT, MDHIM_GET_PREV
  int op;
  //Number of records to retreive
  int num_records;
} mdhim_bgetm_t;

/*bulk delete record message */
typedef struct mdhim_bdelm_t {
  int mtype;  
  char **keys;
  int *key_lens;
} mdhim_bdelm_t;


int send_message(mdhim_t *md, int dest, void *message);
void *receive_message(mdhim_t *md, int src);
