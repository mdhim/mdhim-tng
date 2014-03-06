/*
 * =====================================================================================
 *
 *       Filename:  mdhim-abstract.c
 *
 *    Description:  An abstraction of mdhim. Exposes the API of mdhim to applications
 *                  that do not use mpi.
 *
 *        Version:  1.0
 *        Created:  03/06/2014 08:45:23 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Aaron Caldwell
 *
 * =====================================================================================
 */

#include "mdhim-abridge.h"
#include "mdhim.h"
#include "mdhim_options.h"
#include "data_store.h"
#include <stdio.h>
#include <stdlib.h>


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  new_MDHIM
 *  Description:  Mdhim_Ptr constructor, take no arguments.
 *                Will also initialize mdhim_options.
 * =====================================================================================
 */
Mdhim_Ptr *
new_MDHIM()
{

    Mdhim_Ptr * mdhimObj = NULL;

    // allocate memory
    mdhimObj = (Mdhim_Ptr*)malloc(sizeof(Mdhim_Ptr));
    if (mdhimObj == NULL) {
        return NULL;
    }

    // point to itself 
    mdhimObj->pDerivedObj = mdhimObj;

    // allocate memory 

    mdhimObj->mdhim_options = (void *)mdhim_options_init();

    // Initialize Interface
    mdhimObj->mdhimInit =               Mdhim_Initialize;
    mdhimObj->mdhimClose =              Mdhim_Close;
    mdhimObj->mdhimCommit =             Mdhim_Commit;
    mdhimObj->mdhimStatFlush =          Mdhim_StatFlush;
    mdhimObj->mdhimPut =                Mdhim_Put;
    mdhimObj->mdhimBPut =               Mdhim_BPut;
    mdhimObj->mdhimGet =                Mdhim_Get;
    mdhimObj->mdhimBGet =               Mdhim_BGet;
    mdhimObj->mdhimBGetOp =             Mdhim_BGetOp;
    mdhimObj->mdhimDelete =             Mdhim_Delete;
    mdhimObj->mdhimBDelete =            Mdhim_BDelete;
    mdhimObj->mdhim_Release_Recv_Msg =  MdhimReleaseRecvMsg;

    return mdhimObj;
}		/* -----  end of function new_MDHIM  ----- */


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  delete_MDHIM
 *  Description:  Delete instance of Mdhim_Ptr
 * =====================================================================================
 */
void
delete_MDHIM ( Mdhim_Ptr* const pmdhimAbObj )
{
}		/* -----  end of function delete_MDHIM  ----- */


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  Mdhim_Initialize
 *  Description:  Initialize a MDHIM instance
 *                @pmdhim : pointer to a Mdhim_Ptr 
 *                @comm   : void pointer to a Communicator for MPI
 *                @popts  : pointer to MDHIM_Options
 *
 * =====================================================================================
 */
void
Mdhim_Initialize ( Mdhim_Ptr *pmdhim, void *comm, void *popts )
{
   // Initalize MDHIM
   pmdhim->mdhim = (void *)mdhimInit(comm, (mdhim_options_t *)popts);

}		/* -----  end of function Mdhim_Initialize  ----- */


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  Mdhim_Close
 *  Description:  Close MDHIM Instance
 *                @pmdhim : pointer to a Mdhim_Ptr
 * =====================================================================================
 */
int
Mdhim_Close ( Mdhim_Ptr *pmdhim )
{
    int ret;
    ret = mdhimClose((struct mdhim_t *)pmdhim->mdhim);
    return ret;
}		/* -----  end of function Mdhim_Close  ----- */


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  Mdhim_Commit
 *  Description:  Commit any outstanding MDHIM changes
 *                @pmdhim : pointer to a Mdhim_Ptr
 * =====================================================================================
 */
int
Mdhim_Commit ( Mdhim_Ptr *pmdhim )
{
    int ret;
    ret = mdhimCommit((struct mdhim_t *)pmdhim->mdhim);
    return ret;
}		/* -----  end of function Mdhim_Commit  ----- */


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  Mdhim_StatFlush
 *  Description:  Flush any outstanding stats
 *                @pmdhim : pointer to a Mdhim_Ptr
 * =====================================================================================
 */
int
Mdhim_StatFlush ( Mdhim_Ptr *pmdhim )
{
    int ret;
    ret = mdhimStatFlush((struct mdhim_t *)pmdhim->mdhim);
    return ret;
}		/* -----  end of function Mdhim_StatFlush  ----- */


/* 
 * ===  FUNCTION  ======================================================================
 *         Name: Mdhim_Put
 *  Description: Put key, value pair into the database
 *                @pmdhim  : pointer to a Mdhim_Ptr
 *                @pkey    : key
 *                @key_len : how long the key is
 *                @pvalue  : value
 *                @val_len : how long the value is
 * =====================================================================================
 */
void
Mdhim_Put ( Mdhim_Ptr *pmdhim, void *pkey, int key_len, void *pvalue, int val_len )
{
    pmdhim->mdhim_rm = (void *)mdhimPut((struct mdhim_t *)pmdhim->mdhim, pkey, key_len, pvalue, val_len);
}		/* -----  end of function Mdhim_Put  ----- */


/* 
 * ===  FUNCTION  ======================================================================
 *         Name: Mdhim_BPut
 *  Description: Put multiple key, value pairs into the database
 *                @pmdhim      : pointer to a Mdhim_Ptr
 *                @pkeys       : pointer to the keys
 *                @pkey_lens   : pointer to the key lengths
 *                @pvalues     : pointer to the values
 *                @pvalue_lens : pointer to the value lengths
 *                @num_rec     : Number of key, value pairs to be stored
 * =====================================================================================
 */
void
Mdhim_BPut ( Mdhim_Ptr *pmdhim, void **pkeys, int *pkey_lens, void **pvalues, int *pvalue_lens, int num_rec )
{
    pmdhim->mdhim_brm = (void *)mdhimBPut((struct mdhim_t *)pmdhim->mdhim, pkeys, pkey_lens, pvalues, pvalue_lens, num_rec);
}		/* -----  end of function Mdhim_BPut  ----- */


/* 
 * ===  FUNCTION  ======================================================================
 *         Name: Mdhim_Get
 *  Description: Get a key, value pair from the database
 *                @pmdhim  : pointer to a Mdhim_Ptr
 *                @pkey    : Key, you are looking for
 *                @key_len : Length of the key
 *                @op      : How you want to look for the key
 * =====================================================================================
 */
void
Mdhim_Get ( Mdhim_Ptr *pmdhim, void *pkey, int key_len, int op )
{
    pmdhim->mdhim_getrm = (void *)mdhimGet((struct mdhim_t *)pmdhim->mdhim, pkey, key_len, op);
}		/* -----  end of function Mdhim_Get  ----- */


/* 
 * ===  FUNCTION  ======================================================================
 *         Name: Mdhim_BGet
 *  Description: Get multiple key, value pairs from the database
 *                @pmdhim      : pointer to a Mdhim_Ptr
 *                @pkeys       : pointer to the keys to be searched on
 *                @pkey_lens   : pointer to length of the keys
 *                @num_records : number of records to retrieve
 * =====================================================================================
 */
void
Mdhim_BGet ( Mdhim_Ptr *pmdhim, void **pkeys, int *pkey_lens, int num_records )
{
    pmdhim->mdhim_bgetrm = (void *)mdhimBGet((struct mdhim_t *)pmdhim->mdhim, pkeys, pkey_lens, num_records);
}	/* -----  end of function Mdhim_BGet  ----- */


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  Mdhim_BGetOp
 *  Description:  Get multiple key, value pairs from the database by option
 *                @pdmhim : pointer to a Mdhim_Ptr
 *                @pkey   : pointer to the keys to be search on
 *                @pkey_len : pointer to length of keys
 *                @num_records : number of records to retrieve
 *                @op : option to search on
 * =====================================================================================
 */
void
Mdhim_BGetOp ( Mdhim_Ptr *pmdhim, void *pkey, int key_len, int num_records, int op )
{
    pmdhim->mdhim_bgetrm = (void *)mdhimBGetOp((struct mdhim_t *)pmdhim->mdhim, pkey, key_len, num_records, op);
}		/* -----  end of function Mdhim_BGetOp  ----- */


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  Mdhim_Delete
 *  Description:  Delete key, value from database
 *                @pmdhim : pointer to a Mdhim_Ptr
 *                @pkey : key to search for
 *                @key_len : Length of the key
 * =====================================================================================
 */
void
Mdhim_Delete ( Mdhim_Ptr *pmdhim, void *pkey, int key_len )
{
    pmdhim->mdhim_rm = (void *)mdhimDelete((struct mdhim_t *)pmdhim->mdhim, pkey, key_len);
}		/* -----  end of function Mdhim_Delete  ----- */


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  Mdhim_BDelete
 *  Description:  Delete multiple key, value pairs from the database
 *                @pmdhim : pointer to a Mdhim_Ptr
 *                @pkeys : keys to search on
 *                @pkey_len : lenght of the keys
 *                @num_keys : number of keys being search on
 * =====================================================================================
 */
void
Mdhim_BDelete ( Mdhim_Ptr *pmdhim, void **pkeys, int *pkey_len, int num_keys )
{
    pmdhim->mdhim_brm = (void *)mdhimBDelete((struct mdhim_t *)pmdhim->mdhim, pkeys, pkey_len, num_keys);
}		/* -----  end of function Mdhim_BDelete  ----- */


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  MdhimReleaseRecvMsg
 *  Description:  Not sure what this does, has not been implemented in MDHIM.
 *                Including it for completeness
 * =====================================================================================
 */
void
MdhimReleaseRecvMsg ( void *pmsg )
{
    //mdhim_release_recv_msg(pmsg);
}		/* -----  end of function MdhimReleaseRecvMsg  ----- */
