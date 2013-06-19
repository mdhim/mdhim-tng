/*
 * MDHIM TNG
 * 
 * Local client implementation
 */

#ifndef      __CLIENT_H
#define      __CLIENT_H

#include "messages.h"

struct mdhim_rm_t *local_client_put(struct mdhim_t *md, struct mdhim_putm_t *pm);
struct mdhim_rm_t *local_client_bput(struct mdhim_t *md, struct mdhim_bputm_t *bpm);
struct mdhim_getrm_t *local_client_get(struct mdhim_t *md, struct mdhim_getm_t *gm);
struct mdhim_bgetrm_t *local_client_bget(struct mdhim_t *md, struct mdhim_bgetm_t *bgm);
struct mdhim_rm_t *local_client_delete(struct mdhim_t *md, struct mdhim_delm_t *dm);
struct mdhim_rm_t *local_client_bdelete(struct mdhim_t *md, struct mdhim_bdelm_t *dm);

#endif
