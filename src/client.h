/*
 * MDHIM TNG
 * 
 * Client specific implementation
 */

#include "messages.h"


int client_put(mdhim_t *md, mdhim_putm_t *pm);
int client_bput(mdhim_t *md, mdhim_bputm_t *bpm);
int client_get(mdhim_t *md, mdhim_getm_t *gm);
int client_bget(mdhim_t *md, mdhim_bgetm_t *bgm);
int client_delete(mdhim_t *md, mdhim_deletem_t *dm);
int client_bdelete(mdhim_t *md, mdhim_deletem_t *dm);
int client_add_rangesrv(mdhim_t *md);
