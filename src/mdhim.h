/*
 * MDHIM TNG
 * 
 * External API and data structures
 */

#include <mpi.h>
#include <sys/types.h>
#include <pthread.h>
#include "data_store.h"
#include "range_server.h"

#define MDHIM_SUCCESS 0
#define MDHIM_ERROR -1
#define MDHIM_DB_ERROR -2

/* 
 * mdhim data 
 * Contains client communicator
 * Contains a list of range servers
 * Contains a pointer to mdhim_rs_t if rank is a range server
 */
typedef struct mdhim_t {
  //This communicator will include every process in the application, but is separate from the app
  MPI_Comm mdhim_comm;   
  //The number of range servers in the rangesrvs list
  uint32_t num_servers;
  //A linked list of range servers
  rangesrv_info *rangesrvs;
  mdhim_rs_t *mdhim_rs; 
} mdhim_t;

/* 
 * Range server info  
 * Contains information about each range server
 */
typedef struct rangesrv_info rangesrv_info;
struct rangesrv_info {
  //The range server's rank in the mdhim_comm
  uint32_t rank;
  //The start range this server is serving (inclusive)
  uint64_t start_range;
  //The end range this server is serving (inclusive)
  uint64_t end_range;
  //The next range server in the list
  rangesrv_info *next;
  //The previous range server in the list
  rangesrv_info *prev;
};

mdhim_t *mdhimInit(MPI_Comm appComm);
int mdimClose(mdhim_t *md);
int mdhimPut(mdhim_t *md, mdhim_putm_t *pm);
int mdhimBput(mdhim_t *md, mdhim_bputm_t *bpm);
int mdhimGet(mdhim_t *md, mdhim_getm_t *gm);
int mdhimBGet(mdhim_t *md, mdhim_bgetm_t *bgm);
int mdhimDelete(mdhim_t *md, mdhim_deletem_t *dm);
int mdhimBdelete(mdhim_t *md, mdhim_deletem_t *dm);

