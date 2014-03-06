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

typedef struct _Mdhim_Ptr Mdhim_Ptr;

// pointers to functions
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

typedef struct _Mdhim_Ptr {

    void *pDerivedObj;    // pointer to itself as base object
    void *mdhim;          // pointer to the mdhim_t struct
    void *mdhim_rm;       // pointer to the mdhim_rm_t struct
    void *mdhim_brm;      // pointer to the mdhim_brm_t struct
    void *mdhim_getrm;    // pointer to the mdhim_getrm_t struct
    void *mdhim_bgetrm;   // pointer to the mdhim_bgetrm_t struct
    void *mdhim_options;  // pointer to the mdhim_options_t struct
    
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

} Mdhim_Ptr;

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
