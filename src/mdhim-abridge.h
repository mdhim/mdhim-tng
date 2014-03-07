/*
 * =====================================================================================
 *
 *       Filename:  mdhim-abridge.h
 *
 *    Description:  An abstraction of mdhim. Exposes the API of mdhim to applications 
 *                  that do not use mpi.
 *
 *        Version:  1.0
 *        Created:  03/04/2014 12:06:00 
 *       Revision:  None
 *       Compiler:
 *
 *         Author: Aaron Caldwell
 *
 * =====================================================================================
 */

#ifndef __MDHIMAB_H
#define __MDHIMAB_H

#include <stdint.h>
#include <stdio.h>
#include "mdhim_options.h"
#include "partitioner_define.h"
#include "messages_define.h"
#include "mdhim_define.h"
#include "data_store_define.h"
#include "Mlog/mlog.h"
#include "Mlog/mlogfacs.h"

typedef struct _Mdhim_Ptr Mdhim_Ptr;

struct mdhim_t;

// pointers to functions
// MDHIM FUNCTIONS
typedef void  (*fptrMdhimInit)(Mdhim_Ptr *, void *, void *);
typedef int   (*fptrMdhimClose)(Mdhim_Ptr *);
typedef int   (*fptrMdhimCommit)(Mdhim_Ptr *);
typedef int   (*fptrMdhimStatFlush)(Mdhim_Ptr *);
typedef void  (*fptrMdhimPut)(Mdhim_Ptr *, void *, int, void *, int);
typedef void  (*fptrMdhimBPut)(Mdhim_Ptr *, void **, int *, void **, int *, int);
typedef void  (*fptrMdhimGet)(Mdhim_Ptr *, void *, int, int);
typedef void  (*fptrMdhimBGet)(Mdhim_Ptr *, void **, int *, int);
typedef void  (*fptrMdhimBGetOp)(Mdhim_Ptr *, void *, int, int, int);
typedef void  (*fptrMdhimDelete)(Mdhim_Ptr *, void *, int);
typedef void  (*fptrMdhimBDelete)(Mdhim_Ptr *, void **, int *, int);
typedef void  (*fptrMdhim_Release_Recv_Msg)(void *);

// MDHIM_T ACCESSOR FUNCTIONS
typedef int      (*fptrGetMdhimRank)(Mdhim_Ptr *);
typedef int      (*fptrGetMdhimCommSize)(Mdhim_Ptr *);
typedef int      (*fptrGetKeyType)(Mdhim_Ptr *);
typedef uint32_t (*fptrGetNumRangeServers)(Mdhim_Ptr *);
typedef void    *(*fptrGetRangeServers)(Mdhim_Ptr *);
typedef int      (*fptrGetRangeServerMaster)(Mdhim_Ptr *);
typedef void    *(*fptrGetMdhimRs)(Mdhim_Ptr *);

// MDHIM_RM_T ACCESSOR FUNCTIONS
typedef int  (*fptrGetRmMType)(Mdhim_Ptr *);
typedef int  (*fptrGetRmServerRank)(Mdhim_Ptr *);
typedef int  (*fptrGetRmSize)(Mdhim_Ptr *);
typedef int  (*fptrGetRmError)(Mdhim_Ptr *);

// MDHIM_GETRM_T ACCESSOR FUNCTIONS
typedef int   (*fptrGetGrmMType)(Mdhim_Ptr *);
typedef int   (*fptrGetGrmServerRank)(Mdhim_Ptr *);
typedef int   (*fptrGetGrmSize)(Mdhim_Ptr *);
typedef int   (*fptrGetGrmError)(Mdhim_Ptr *);
typedef void *(*fptrGetGrmKey)(Mdhim_Ptr *);
typedef int   (*fptrGetGrmKeyLen)(Mdhim_Ptr *);
typedef void *(*fptrGetGrmValue)(Mdhim_Ptr *);
typedef int   (*fptrGetGrmValueLen)(Mdhim_Ptr *);

typedef struct _Mdhim_Ptr {

    void *pDerivedObj;              //  pointer to itself as base object
    struct mdhim_t *mdhim;          //  pointer to the mdhim_t struct
    void *mdhim_rm;                 //  pointer to the mdhim_rm_t struct
    void *mdhim_brm;                //  pointer to the mdhim_brm_t struct
    void *mdhim_getrm;              //  pointer to the mdhim_getrm_t struct
    void *mdhim_bgetrm;             //  pointer to the mdhim_bgetrm_t struct
    mdhim_options_t *mdhim_options; //  pointer to the mdhim_options_t struct
 
    // MDHIM Functions
    fptrMdhimInit mdhimInit;            // Initialize MDHIM
    fptrMdhimClose mdhimClose;          // Close MDHIM
    fptrMdhimCommit mdhimCommit;        // Commit changes in MDHIM to Database
    fptrMdhimStatFlush mdhimStatFlush;  // Flush the stats
    fptrMdhimPut mdhimPut;              // Put single key, value in Database
    fptrMdhimBPut mdhimBPut;            // Put multiple key, value pairs in Database
    fptrMdhimGet mdhimGet;              // Get a single key, value pair from the Database
    fptrMdhimBGet mdhimBGet;            // Get multiple key, value pairs from the Database
    fptrMdhimBGetOp mdhimBGetOp;        // Get multiple key, value pairs from the Database based on the option passed in
    fptrMdhimDelete mdhimDelete;        // Delete key, value from Database
    fptrMdhimBDelete mdhimBDelete;      // Delete multiple key, value pairs from Database
    fptrMdhim_Release_Recv_Msg mdhim_Release_Recv_Msg;

    //  MDHIM_T Accessors
    fptrGetMdhimRank getMdhimRank;                 //  Get the rank from mpi
    fptrGetMdhimCommSize getMdhimCommSize;         //  Get the comm size from mpi
    fptrGetKeyType getKeyType;                     //  get the key type
    fptrGetNumRangeServers getNumRangeServers;     //  get the number of range servers
    fptrGetRangeServerMaster getRangeServerMaster; //  get the master range server

    //  MDHIM_RM_T Accessors
    fptrGetRmMType getRmMType;           //  get the return messages mtype
    fptrGetRmServerRank getRmServerRank; //  get the range server rank returning this message
    fptrGetRmSize getRmSize;             //  get the size of the message
    fptrGetRmError getRmError;           //  get the error of the message


}_Mdhim_Ptr;

Mdhim_Ptr * new_MDHIM(); // Constructor w/ out arguments

void delete_MDHIM(Mdhim_Ptr* const pMdhimAbObj); // Destructor

void Mdhim_Initialize(Mdhim_Ptr *pmdhim, void *comm, void *popts);
int Mdhim_Close(Mdhim_Ptr *pmdhim);
int Mdhim_Commit(Mdhim_Ptr *pmdhim);
int Mdhim_StatFlush(Mdhim_Ptr *pmdhim);
void Mdhim_Put(Mdhim_Ptr *pmdhim, void *pkey, int key_len, void *pvalue, int val_len);
void Mdhim_BPut(Mdhim_Ptr *pmdhim, void **pkeys, int *pkey_lens, void **pvalues, int *pvalue_lens, int num_rec);
void Mdhim_Get(Mdhim_Ptr *pmdhim, void *pkey, int key_len, int op);
void Mdhim_BGet(Mdhim_Ptr *pmdhim, void **pkeys, int *pkey_lens, int num_records);
void Mdhim_BGetOp(Mdhim_Ptr *pmdhim, void *pkey, int key_len, int num_records, int op);
void Mdhim_Delete(Mdhim_Ptr *pmdhim, void *pkey, int key_len);
void Mdhim_BDelete(Mdhim_Ptr *pmdhim, void **pkeys, int *pkey_len, int num_keys);
void MdhimReleaseRecvMsg(void *pmsg);

int Get_Mdhim_Rank(Mdhim_Ptr *pmdhim);
int Get_Mdhim_Comm_Size(Mdhim_Ptr *pmdhim);
int Get_Key_Type(Mdhim_Ptr *pmdhim);
uint32_t Get_Num_Range_Servers(Mdhim_Ptr *pmdhim);
int Get_Range_Server_Master(Mdhim_Ptr *pmdhim);

int Get_Rm_Mtype(Mdhim_Ptr *);
int Get_Rm_Server_Rank(Mdhim_Ptr *);
int Get_Rm_Size(Mdhim_Ptr *);
int Get_Rm_Error(Mdhim_Ptr *);

#endif
