/*
 mdhimiftst.c - file based test frame

 * based on the pbliftst.c
 Copyright (C) 2002 - 2007   Peter Graf

   pbliftst.c file is part of PBL - The Program Base Library.
   PBL is free software.

    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program; if not, write to the Free Software
    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

   For more information on the Program Base Library or Peter Graf,
   please see: http://www.mission-base.com/.


------------------------------------------------------------------------------
*/

/*
 * make sure "strings <exe> | grep Id | sort -u" shows the source file versions
 */
char * mdhimTst_c_id = "$Id: mdhimTst.c,v 1.00 2013/07/08 20:56:50 JHR Exp $";

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <time.h>

#include "mpi.h"
#include "mdhim.h"

#define TEST_BUFLEN              2048

static FILE * logfile;
static FILE * infile;
int verbose = 1;   // By default generate lost of feedback status lines

#ifdef _WIN32
#include <direct.h>
#endif

#include <stdarg.h>

static void tst_say(
char * format,
...
)
{
    va_list ap;

    /*
     * use fprintf to give out the text
     */
    va_start( ap, format );
    vfprintf( stdout,
              format,
              ap
            );
    va_end(ap);

    /*
     * use fprintf to give out the text
     */
    va_start( ap, format );
    vfprintf( logfile,
              format,
              ap
            );
    va_end(ap);
}

static void putChar( int c )
{
   static int last = 0;

   if( last == '\n' && c == '\n' )
   {
       return;
   }

   last = c;
   putc( last, logfile );
}

static int getChar( void )
{
    int c;
    c = getc( infile );

    /*
     * a '#' starts a comment for the rest of the line
     */
    if( c == '#')
    {
        /*
         * comments starting with ## are duplicated to the output
         */
        c = getc( infile );
        if( c == '#' )
        {
            putChar( '#' );
            putChar( '#' );

            while( c != '\n' && c != EOF )
            {
                c = getc( infile );
                if( c != EOF )
                {
                    putChar( c );
                }
            }
        }
        else
        {
            while( c != '\n' && c != EOF )
            {
                c = getc( infile );
            }
        }
    }

/*
    if( c != EOF )
    {
        putChar( c );
    }
*/

    return( c );
}

static void getWord( char * buffer )
{
    int c;
    int i;

    /*
     * skip preceeding blanks
     */
    c = ' ';
    while( c == '\t' || c == ' ' || c == '\n' || c == '\r' )
    {
        c = getChar();
    }

    /*
     * read one word
     */
    for( i = 0; i < TEST_BUFLEN - 1; i++, c = getChar() )
    {
        if( c == EOF )
        {
            exit( 0 );
        }


        if( c == '\r' )
        {
            continue;
        }

        if( c == '\t' || c == ' ' || c == '\n' || c == '\r' )
        {
            *buffer = '\0';
            return;
        }

        *buffer++ = c;
        putChar( c );
    }

    *buffer = '\0';
}

static int getWordFromString (char *aLine,  char *buffer, int charIdx )
{
    int c;
    int i;

    /*
     * skip preceeding blanks
     */
    c = aLine[charIdx++];
    while( c == '\t' || c == ' ' || c == '\n' || c == '\r' )
    {
        c = aLine[charIdx++];
    }

    /*
     * read one word
     */
    for( i = 0; i < TEST_BUFLEN - 1; i++, c = aLine[charIdx++] )
    {

        if( c == '\r' )
        {
            continue;
        }

        if( c == '\t' || c == ' ' || c == '\n' || c == '\r' )
        {
            *buffer = '\0';
            return charIdx;
        }

        *buffer++ = c;
    }

    *buffer = '\0';
    return charIdx;
}

static void getLine( char * buffer )
{
    int c;
    int i;

    /*
     * skip preceeding blanks
     */
    c = ' ';
    while( c == '\t' || c == ' ' || c == '\n' || c == '\r' )
    {
        c = getChar();
    }

    /*
     * read one line
     */
    for( i = 0; i < TEST_BUFLEN - 1; i++, c = getChar() )
    {

        if( c == EOF || c == '\n' || c == '\r' )
        {
            *buffer = '\0';
            return;
        }

        *buffer++ = c;
    }

    *buffer = '\0';
}

static int commastrlen( char * s )
{
    char *ptr = s;

    while( ptr && *ptr && *ptr != ',' )
    {
        ptr++;
    }

    return( ptr - s );
}

/**
 * test frame for the MDHIM
 *
 * This test frame calls the MDHIM subroutines, it is an interactive or batch file
 * test frame which could be used for regression tests.
 * 
 * When the program is called in verbose mode (not quiet) it also writes a log file
 * for each rank with the name mdhimTst-#.log (where # is the rank)
 *
 * <B>Interactive mode or commands read from input file.</B>
 * -- Interactive mode simply execute mdhimtst, (by default it is verbose)
 * -- Batch mode mdhimtst file_name <-quiet>
 * <UL>
 * Call the program mdhimtst from a UNIX or DOS shell. (with a filename for batch mode)
 * <BR>
 * Use the following commands to test the MDHIM subroutines supplied:
 * <UL>
 * <PRE>
 q       FOR QUIT
 /////////open filename 
 /////////transaction < START | COMMIT | ROLLBACK >
 /////////close
 /////////flush
 put key data
 bput n key data
 /////////find index key < LT | LE | FI | EQ | LA | GE | GT >
 /////////nfind n index key < LT | LE | FI | EQ | LA | GE | GT >
 get key
 bget n key
 del key
 bdel n key
 /////////datalen
 /////////readdata
 /////////readkey index
 /////////updatedata data
 /////////updatekey index key
 </PRE>
 * </UL>
 * Do the following if you want to run the test cases per hand
 * <PRE>
   1. Build the mdhimtst executable.          make all
   2. Run the test frame on a file.        mdhimtst tst0001.TST
 </PRE>
 *
 * </UL>
 */

int main( int argc, char * argv[] )
{
    char     commands[ 1000 ] [ TEST_BUFLEN ]; // Command to be read
    int      cmdIdx = 0; // Command current index
    int      cmdTot = 0; // Total number of commands read
    int      charIdx; // Index to last processed character of a command line
    char     command  [ TEST_BUFLEN ];
    char     filename [ TEST_BUFLEN ];
    char     buffer1  [ TEST_BUFLEN ];
    char     buffer2  [ TEST_BUFLEN ];
    char     buffer3  [ TEST_BUFLEN ];
    int      dowork = 1;

    clock_t  begin, end;
    double   time_spent;
    
    int ret;
    int provided = 0;
    struct mdhim_t *md;
    int key;
    int value;
    struct mdhim_rm_t *rm;
    struct mdhim_getrm_t *grm;

    /*
     * if an argument is given it is treated as a command file
     */
    infile = stdin;
    if( argc > 1 )
    {
        infile = fopen( argv[ 1 ], "r" );
        if( !infile )
        {
            fprintf( stderr, "Failed to open %s, %s\n", argv[ 1 ], strerror( errno ));
            exit( -1 );
        }
    }
    // Default is verbose mode
    if ( argc > 2)
    {
        if( !strcmp( argv[ 2 ], "-quiet" ))
            verbose = 0;
    }
        
    // calls to init MPI for mdhim
    argc = 1;  // Ignore other parameters passed to program
    ret = MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    if (ret != MPI_SUCCESS)
    {
            printf("Error initializing MPI with threads\n");
            exit(1);
    }

    if (provided != MPI_THREAD_MULTIPLE)
    {
            printf("Not able to enable MPI_THREAD_MULTIPLE mode\n");
            exit(1);
    }

    md = mdhimInit(MPI_COMM_WORLD);
    if (!md)
    {
            printf("Error initializing MDHIM\n");
            exit(1);
    }	

    /*
     * open the log file (one per rank if in verbose mode, otherwise write to stderr)
     */
    if (verbose)
    {
        sprintf(filename, "./mdhimTst-%d.log", md->mdhim_rank);
        logfile = fopen( filename, "wb" );
        if( !logfile )
        {
            fprintf( stderr, "can't open logfile, %s, %s\n", filename,
                     strerror( errno ));
            exit( 1 );
        }
    }
    else
    {
        logfile = stderr;
    }
    
    while( dowork && cmdIdx < 1000)
    {
        /*
         * read the next command
         */
        memset( commands[cmdIdx], 0, sizeof( command ));
        errno = 0;
        getLine( commands[cmdIdx]);
        if (verbose) tst_say( "\n##command %d: %s\n", cmdIdx, commands[cmdIdx]);
        
        /*
         * Is this the last/quit command?
         */
        if( commands[cmdIdx][0] == 'q' || commands[cmdIdx][0] == 'Q' )
        {
            dowork = 0;
        }
        cmdIdx++;
    }
    cmdTot = cmdIdx -1;

    /*
     * main command execute loop
     */
    for(cmdIdx=0; cmdIdx < cmdTot; cmdIdx++)
    {
        memset( command, 0, sizeof( command ));
        errno = 0;
        
        charIdx = getWordFromString( commands[cmdIdx], command, 0);

        if (verbose) tst_say( "\n##exec command: %s\n", command );
        begin = clock();
        
        /*
         * execute the command given
         */
        //======================================PUT============================
        if( !strcmp( command, "put" ))
        {
            if (verbose) tst_say( "# put key data\n" );

            charIdx = getWordFromString( commands[cmdIdx], buffer1, charIdx);
            key = atoi(buffer1) + (md->mdhim_rank + 1);
            
            charIdx = getWordFromString( commands[cmdIdx], buffer2, charIdx);
            value = atoi(buffer2) + (md->mdhim_rank + 1);  

            tst_say( "# mdhimPut( %s, %s)\n", buffer1, buffer2 );
            rm = mdhimPut(md, &key, sizeof(key), MDHIM_INT_KEY, &value, sizeof(value));
            if (!rm || rm->error)
            {
                    tst_say("Error putting key/value into MDHIM\n");
            }
            else
            {
                    tst_say("Successfully put key/value into MDHIM\n");
            }
            
        }

        //======================================GET============================
        else if( !strcmp( command, "get" ))
        {
            if (verbose) tst_say( "# get key\n" );

            charIdx = getWordFromString( commands[cmdIdx], buffer1, charIdx);
            key = atoi( buffer1 ) + (md->mdhim_rank + 1);

            tst_say( "# mdhimGet( %d )\n", key);

            grm = mdhimGet(md, &key, sizeof(key), MDHIM_INT_KEY);
            if (!grm || grm->error)
            {
                    tst_say("Error getting value for key: %d from MDHIM\n", key);
            }
            else 
            {
                    tst_say("Successfully got value: %d from MDHIM\n", 
                            *((int *) grm->value));
            }

        }
        
        //======================================BPUT============================
        else if ( !strcmp( command, "bput" ))
        {
            int nkeys = 100;
            if (verbose) tst_say( "# bput n key data\n" );

            charIdx = getWordFromString( commands[cmdIdx], buffer1, charIdx);
            nkeys = atoi( buffer1 );
            
            charIdx = getWordFromString( commands[cmdIdx], buffer2, charIdx);
            key = atoi( buffer2 );
            
            charIdx = getWordFromString( commands[cmdIdx], buffer3, charIdx);
            value = atoi( buffer3 );
            
            tst_say( "# mdhimBPut(%d, %s, %s )\n", nkeys, buffer2, buffer3 );
            
            int **keys;
            int key_lens[nkeys];
            int key_types[nkeys];
            int **values;
            int value_lens[nkeys];
            struct mdhim_brm_t *brm, *brmp;
            int i;
            
            //Populate the keys and values to insert
            keys = malloc(sizeof(int *) * nkeys);
            values = malloc(sizeof(int *) * nkeys);
            
            for (i = 0; i < nkeys; i++)
            {
                    keys[i] = malloc(sizeof(int));
                    *keys[i] = key + (i + 1) * (md->mdhim_rank + 1);
                    if (verbose) tst_say("Rank: %d - Inserting key: %d\n", 
                            md->mdhim_rank, *keys[i]);
                    key_lens[i] = sizeof(int);
                    key_types[i] = MDHIM_INT_KEY;
                    values[i] = malloc(sizeof(int));
                    *values[i] = value + (i + 1) * (md->mdhim_rank + 1);
                    value_lens[i] = sizeof(int);		
            }

            //Insert the keys into MDHIM
            brm = mdhimBPut(md, (void **) keys, key_lens, key_types, 
                            (void **) values, value_lens, nkeys);
            brmp = brm;
            if (!brm || brm->error)
            {
                    tst_say("Rank - %d: Error inserting keys/values into MDHIM\n", 
                            md->mdhim_rank);
            } 
            while (brmp)
            {
                    if (brmp->error < 0)
                    {
                            tst_say("Rank: %d - Error inserting key/values info MDHIM\n", 
                                    md->mdhim_rank);
                    }

                    brmp = brmp->next;
                    //Free the message
                    mdhim_full_release_msg(brm);
                    brm = brmp;
            }

            //Commit the database
            ret = mdhimCommit(md);
            if (ret != MDHIM_SUCCESS)
            {
                    tst_say("Error committing MDHIM database\n");
            }
            else
            {
                    tst_say("Committed MDHIM database\n");
            }
        }
        
        //======================================BGET============================
        else if ( !strcmp( command, "bget" ))
        {
            int nkeys = 100;
            if (verbose) tst_say( "# bget n key\n" );

            charIdx = getWordFromString( commands[cmdIdx], buffer1, charIdx);
            nkeys = atoi( buffer1 );
            
            charIdx = getWordFromString( commands[cmdIdx], buffer2, charIdx);
            key = atoi( buffer2 );
            
            tst_say( "# mdhimBGet(%d, %s )\n", nkeys, buffer2 );
            
            int **keys;
            int key_lens[nkeys];
            int key_types[nkeys];
            struct mdhim_bgetrm_t *bgrm, *bgrmp;
            int i;
            
            //Populate the keys to get
            keys = malloc(sizeof(int *) * nkeys);
            for (i = 0; i < nkeys; i++)
            {
                    keys[i] = malloc(sizeof(int));
                    *keys[i] = key + (i + 1) * (md->mdhim_rank + 1);
                    if (verbose) tst_say("Rank: %d - Creating key (to get): %d\n", 
                            md->mdhim_rank, *keys[i]);
                    key_lens[i] = sizeof(int);
                    key_types[i] = MDHIM_INT_KEY;		
            }

           //Get the values back for each key inserted
            bgrm = mdhimBGet(md, (void **) keys, key_lens, key_types, nkeys);
            bgrmp = bgrm;
            while (bgrmp) {
                    if (bgrmp->error < 0)
                    {
                            tst_say("Rank: %d - Error retrieving values", 
                                    md->mdhim_rank);
                    }

                    for (i = 0; i < bgrmp->num_records && bgrmp->error >= 0; i++)
                    {
                            if (verbose) tst_say("Rank: %d - Got key: %d value: %d\n", 
                                         md->mdhim_rank, 
                                   *(int *)bgrmp->keys[i], *(int *)bgrmp->values[i]);
                    }

                    bgrmp = bgrmp->next;
                    //Free the message received
                    mdhim_full_release_msg(bgrm);
                    bgrm = bgrmp;
            }
        }
        
        //======================================DEL============================
        else if( !strcmp( command, "del" ))
        {
            int key;
            struct mdhim_rm_t *rm;
            
            if (verbose) tst_say( "# del key\n" );
            
            charIdx = getWordFromString( commands[cmdIdx], buffer1, charIdx);
            key = atoi( buffer1 ) + (md->mdhim_rank + 1);
            
            tst_say( "# mdhimDelete( %s )\n", buffer1 );

            rm = mdhimDelete(md, &key, sizeof(key), MDHIM_INT_KEY);
            if (!rm || rm->error)
            {
                    tst_say("Error deleting key/value from MDHIM\n");
            }
            else
            {
                    tst_say("Successfully deleted key/value into MDHIM\n");
            }

        }
        
        //======================================NDEL============================
        else if( !strcmp( command, "ndel" ))
        {
            int nkeys = 100;
            if (verbose) tst_say( "# ndel n key\n" );

            charIdx = getWordFromString( commands[cmdIdx], buffer1, charIdx);
            nkeys = atoi( buffer1 );
            
            charIdx = getWordFromString( commands[cmdIdx], buffer2, charIdx);
            key = atoi( buffer2 );
            
            tst_say( "# mdhimBDelete(%d, %s )\n", nkeys, buffer2 );
            
            struct mdhim_t *md;
            int **keys;
            int key_lens[nkeys];
            int key_types[nkeys];
            struct mdhim_brm_t *brm, *brmp;
            int i;
        
            keys = malloc(sizeof(int *) * nkeys);
            for (i = 0; i < nkeys; i++)
            {
                    keys[i] = malloc(sizeof(int));
                    *keys[i] = key + (i + 1) * (md->mdhim_rank + 1);
                    key_lens[i] = sizeof(int);
                    key_types[i] = MDHIM_INT_KEY;
            }

            //Delete the records
            brm = mdhimBDelete(md, (void **) keys, key_lens, key_types, nkeys);
            brmp = brm;
            if (!brm || brm->error) {
                    tst_say("Rank - %d: Error deleting keys/values from MDHIM\n", 
                            md->mdhim_rank);
            } 
            while (brmp)
            {
                    if (brmp->error < 0)
                    {
                            tst_say("Rank: %d - Error deleting keys\n", md->mdhim_rank);
                    }

                    brmp = brmp->next;
                    //Free the message
                    mdhim_full_release_msg(brm);
                    brm = brmp;
            }

        }

        else
        {
            printf( "# q       FOR QUIT\n" );
            //printf( "# open filename keyfile1,dkeyfile2,... update\n" );
            //printf( "# transaction < START | COMMIT | ROLLBACK >\n" );
            //printf( "# close\n" );
            //printf( "# flush\n" );
            printf( "# put key data\n" );
            printf( "# nput n key data\n" );
            //printf( "# find index key < LT | LE | FI | EQ | LA | GE | GT >\n" );
            //printf( "# nfind n index key < LT | LE | FI | EQ | LA | GE | GT >\n" );
            printf( "# get key\n" );
            printf( "# nget n key\n" );
            //printf( "# datalen\n" );
            //printf( "# readdata\n" );
            //printf( "# readkey index\n" );
            //printf( "# updatedata data\n" );
            //printf( "# updatekey index key\n" );
            printf( "# del key\n" );
            printf( "# ndel n key\n" );
        }
        
        end = clock();
        time_spent = (double)(end - begin) / CLOCKS_PER_SEC;
        printf("Seconds to %s : %f\n\n", commands[cmdIdx], time_spent);
    }
    
    fclose(logfile);
    
    // Calls to finalize mdhim session and close MPI-communication
    ret = mdhimClose(md);
    if (ret != MDHIM_SUCCESS)
    {
            tst_say("Error closing MDHIM\n");
    }

    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Finalize();

    return( 0 );
}
