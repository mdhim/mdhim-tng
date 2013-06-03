#include <pthread.h>
#include "hash"

typedef struct work_item work_item;
struct work_item {
  work_item *next;
  work_item *prev;
  void *message;
};

typedef struct work_queue {
  work_item *head;
  work_item *tail;
} work_queue;

/* Range server specific data */
typedef struct mdhim_rs_t {
  //The abstracted data store layer that mdhim uses to store and retrieve records
  mdhim_store_t *mdhim_store;
  work_queue *work_queue;
  pthread_mutex_t *work_queue_mutex;
  pthread_cond_t *work_ready_cv;
  pthread_t *listener;
  pthread_t *worker;
  rangesrv_info *info;
} mdhim_rs_t;

int range_server_init(mdhim_t *md);
int range_server_add_work(work_item *item);
