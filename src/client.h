/*
 * MDHIM TNG
 * 
 * Client specific implementation
 */

#ifndef      __CLIENT_H
#define      __CLIENT_H

#include "messages.h"

int client_put(struct mdhim_t *md, struct mdhim_putm_t *pm);
int client_bput(struct mdhim_t *md, struct mdhim_bputm_t *bpm);
int client_get(struct mdhim_t *md, struct mdhim_getm_t *gm);
int client_bget(struct mdhim_t *md, struct mdhim_bgetm_t *bgm);
int client_delete(struct mdhim_t *md, struct mdhim_delm_t *dm);
int client_bdelete(struct mdhim_t *md, struct mdhim_bdelm_t *dm);
int client_add_rangesrv(struct mdhim_t *md);

#endif
