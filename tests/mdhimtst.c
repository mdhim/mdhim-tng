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
#include "db_options.h"

// From partitioner.h:
/*
#define MDHIM_INT_KEY 1
//64 bit signed integer
#define MDHIM_LONG_INT_KEY 2
#define MDHIM_FLOAT_KEY 3
#define MDHIM_DOUBLE_KEY 4
#define MDHIM_LONG_DOUBLE_KEY 5
#define MDHIM_STRING_KEY 6
//An arbitrary sized key
#define MDHIM_BYTE_KEY 7  */

#define TEST_BUFLEN              2048

static FILE * logfile;
static FILE * infile;
int verbose = 1;   // By default generate lost of feedback status lines
// MDHIM_INT_KEY=1, MDHIM_LONG_INT_KEY=2, MDHIM_FLOAT_KEY=3, MDHIM_DOUBLE_KEY=4
// MDHIM_LONG_DOUBLE_KEY=5, MDHIM_STRING_KEY=6, MDHIM_BYTE_KEY=7 
int key_type = 1;  // Default "int"

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

/* Read one line at a time. Skip any leadings blanks, and send an end of file
 as if a "q" command had been encountered. Return a string with the line read. */
static void getLine( char * buffer )
{
    int c;
    int i;

    // skip preceeding blanks
    c = ' ';
    while( c == '\t' || c == ' ' || c == '\n' || c == '\r' )
    {
        c = getChar();
    }

    // End of input file (even if we did not find a q!
    if( c == EOF )
    {
        *buffer++ = 'q';
        *buffer++ = '\0';
        return;
    }
    
    // Read one line
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

void usage(void)
{
	printf("Usage:\n");
	printf(" -f <BatchInputFileName> (file with batch commands)\n");
	printf(" -d <DataBaseType> (Type of DB to use: unqLite=1, levelDB=2)\n");
        printf(" -t <IndexKeyType> (Type of keys: int=1, longInt=2, float=3, "
               "double=4, longDouble=5, string=6, byte=7)\n");
        printf(" -q (Quiet mode, default is verbose)\n");
	exit (8);
}

//======================================PUT============================
static void execPut(char *command, struct mdhim_t *md, int charIdx)
{
    int key;
    long l_key;
    struct mdhim_rm_t *rm;
    char buffer1 [ TEST_BUFLEN ];
    char buffer2 [ TEST_BUFLEN ];
    char key_string [ TEST_BUFLEN ];
    char value [ TEST_BUFLEN ];
    
    if (verbose) tst_say( "# put key data\n" );
    charIdx = getWordFromString( command, buffer1, charIdx);
    charIdx = getWordFromString( command, buffer2, charIdx);
    sprintf(value, "%s_%d", buffer2, (md->mdhim_rank + 1));
        
    switch (key_type)
    {
        case MDHIM_INT_KEY:
             key = atoi(buffer1) + (md->mdhim_rank + 1);
             tst_say( "# mdhimPut( %d, %s)\n", key, value );
             rm = mdhimPut(md, &key, sizeof(key), value, sizeof(value));
             break;
             
        case MDHIM_LONG_INT_KEY:
             l_key = atol(buffer1) + (md->mdhim_rank + 1);
             tst_say( "# mdhimPut( %ld, %s)\n", l_key, value );
             rm = mdhimPut(md, &l_key, sizeof(l_key), value, sizeof(value));
             break;
             
        case MDHIM_STRING_KEY:
             sprintf(key_string, "%s%d", buffer1, ( md->mdhim_rank + 1) );
             tst_say( "# mdhimPut( %s, %s)\n", key_string, value );
             rm = mdhimPut(md, (void *)key_string, strlen(key_string), value, sizeof(value));
             break;
            
        case MDHIM_BYTE_KEY:
             sprintf(key_string, "%s%d", buffer1, ( md->mdhim_rank + 1) );
             tst_say( "# mdhimPut( %s, %s)\n", key_string, value );
             rm = mdhimPut(md, (void *)key_string, strlen(key_string), value, sizeof(value));
             break;
             
        default:
            tst_say("Error, unrecognized Key_type in execPut\n");
    }

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
static void execGet(char *command, struct mdhim_t *md, int charIdx)
{
    int key;
    long l_key;
    struct mdhim_getrm_t *grm;
    char buffer1 [ TEST_BUFLEN ];
    char key_string [ TEST_BUFLEN ];
    
    if (verbose) tst_say( "# get key\n" );

    charIdx = getWordFromString( command, buffer1, charIdx);
    
    switch (key_type)
    {
       case MDHIM_INT_KEY:
            key = atoi( buffer1 ) + (md->mdhim_rank + 1);
            sprintf(key_string, "%d", key);
            tst_say( "# mdhimGet( %d )\n", key);
            grm = mdhimGet(md, &key, sizeof(key), MDHIM_GET_EQ);
            break;
            
       case MDHIM_LONG_INT_KEY:
            l_key = atol( buffer1 ) + (md->mdhim_rank + 1);
            sprintf(key_string, "%ld", l_key);
            tst_say( "# mdhimGet( %ld )\n", l_key);
            grm = mdhimGet(md, &l_key, sizeof(l_key), MDHIM_GET_EQ);
            break;
            
       case MDHIM_STRING_KEY:
            sprintf(key_string, "%s%d", buffer1, (md->mdhim_rank + 1));
            tst_say( "# mdhimGet( %s )\n", key_string);
            grm = mdhimGet(md, (void *)key_string, strlen(key_string), MDHIM_GET_EQ);
            break;
                        
       case MDHIM_BYTE_KEY:
            sprintf(key_string, "%s%d", buffer1, (md->mdhim_rank + 1));
            tst_say( "# mdhimGet( %s )\n", key_string);
            grm = mdhimGet(md, (void *)key_string, strlen(key_string), MDHIM_GET_EQ);
            break;
 
       default:
            tst_say("Error, unrecognized Key_type in execGet\n");
    }
    
    if (!grm || grm->error)
    {
        tst_say("Error getting value for key: %s from MDHIM\n", key_string);
    }
    else 
    {
        tst_say("Successfully got value: %s from MDHIM\n", (char *) grm->value);
    }

}

//======================================BPUT============================
static void execBput(char *command, struct mdhim_t *md, int charIdx)
{
    int nkeys = 100;
    int key, value, ret;
    char buffer1 [ TEST_BUFLEN ];
    char buffer2 [ TEST_BUFLEN ];
    char buffer3 [ TEST_BUFLEN ];
    if (verbose) tst_say( "# bput n key data\n" );

    charIdx = getWordFromString( command, buffer1, charIdx);
    nkeys = atoi( buffer1 );

    charIdx = getWordFromString( command, buffer2, charIdx);
    key = atoi( buffer2 );

    charIdx = getWordFromString( command, buffer3, charIdx);
    value = atoi( buffer3 );

    tst_say( "# mdhimBPut(%d, %s, %s )\n", nkeys, buffer2, buffer3 );

    int **keys;
    int key_lens[nkeys];
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
            values[i] = malloc(sizeof(int));
            *values[i] = value + (i + 1) * (md->mdhim_rank + 1);
            value_lens[i] = sizeof(int);		
    }

    //Insert the keys into MDHIM
    brm = mdhimBPut(md, (void **) keys, key_lens, 
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
static void execBget(char *command, struct mdhim_t *md, int charIdx)
{
    int nkeys = 100;
    int key;
    char buffer1 [ TEST_BUFLEN ];
    char buffer2 [ TEST_BUFLEN ];
    if (verbose) tst_say( "# bget n key\n" );

    charIdx = getWordFromString( command, buffer1, charIdx);
    nkeys = atoi( buffer1 );

    charIdx = getWordFromString( command, buffer2, charIdx);
    key = atoi( buffer2 );

    tst_say( "# mdhimBGet(%d, %s )\n", nkeys, buffer2 );

    int **keys;
    int key_lens[nkeys];
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
    }

   //Get the values back for each key inserted
    bgrm = mdhimBGet(md, (void **) keys, key_lens, nkeys);
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
static void execDel(char *command, struct mdhim_t *md, int charIdx)
{
    int key;
    char buffer1 [ TEST_BUFLEN ];
    struct mdhim_rm_t *rm;

    if (verbose) tst_say( "# del key\n" );

    charIdx = getWordFromString( command, buffer1, charIdx);
    key = atoi( buffer1 ) + (md->mdhim_rank + 1);

    tst_say( "# mdhimDelete( %s )\n", buffer1 );

    rm = mdhimDelete(md, &key, sizeof(key));
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
static void execBdel(char *command, struct mdhim_t *md, int charIdx)
{
    int nkeys = 100;
    int key;
    char buffer1 [ TEST_BUFLEN ];
    char buffer2 [ TEST_BUFLEN ];
    if (verbose) tst_say( "# bdel n key\n" );

    charIdx = getWordFromString( command, buffer1, charIdx);
    nkeys = atoi( buffer1 );

    charIdx = getWordFromString( command, buffer2, charIdx);
    key = atoi( buffer2 );

    tst_say( "# mdhimBDelete(%d, %s )\n", nkeys, buffer2 );

    int **keys;
    int key_lens[nkeys];
    struct mdhim_brm_t *brm, *brmp;
    int i;

    keys = malloc(sizeof(int *) * nkeys);
    for (i = 0; i < nkeys; i++)
    {
            keys[i] = malloc(sizeof(int));
            *keys[i] = key + (i + 1) * (md->mdhim_rank + 1);
            key_lens[i] = sizeof(int);
            //key_types[i] = MDHIM_INT_KEY;
    }

    //Delete the records
    brm = mdhimBDelete(md, (void **) keys, key_lens, nkeys);
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
 * -- Batch mode mdhimtst -f <file_name> -d <databse_type> -t <key_type> <-quiet>
 * <UL>
 * Call the program mdhimtst from a UNIX or DOS shell. (with a -f<filename> for batch mode)
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
    int      dowork = 1;

    clock_t  begin, end;
    double   time_spent;
    
    db_options_t *db_opts; // Local variable for db create options to be passed
    
    int ret;
    int provided = 0;
    struct mdhim_t *md;
    
    int db_type = 2; //UNQLITE=1, LEVELDB=2 (data_store.h) 

    // Process arguments
    infile = stdin;
    while ((argc > 1) && (argv[1][0] == '-'))
    {
        switch (argv[1][1])
        {
            case 'f':
                printf("Input file: %s\n", &argv[1][2]);
                infile = fopen( &argv[1][2], "r" );
                if( !infile )
                {
                    fprintf( stderr, "Failed to open %s, %s\n", 
                             &argv[1][2], strerror( errno ));
                    exit( -1 );
                }
                break;

            case 'd': // DataBase type (1, unQlite, levelDB)
                printf("Data Base type: %s\n", &argv[1][2]);
                db_type = atoi( &argv[1][2] );
                // Should be passed to range_server_init in range_server.c
                break;

            case 't':
                printf("Key type: %s\n", &argv[1][2]);
                key_type = atoi( &argv[1][2] );
                break;


            case 'b':
                printf("Debug mode: %s\n", &argv[1][2]);
                // needs to be passed to mdhimInit to set mlog parameters
                break;

            default:
                printf("Wrong Argument (it will be ignored): %s\n", argv[1]);
                usage();
        }

        ++argv;
        --argc;
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

    // Create options for DB initialization
    db_opts = malloc(sizeof(struct db_options_t));
    db_options_set_path(db_opts, "./");
    db_options_set_name(db_opts, "mdhim_tstDB");
    db_options_set_type(db_opts, db_type);
    db_options_set_key_type(db_opts, key_type);
    
    md = mdhimInit(MPI_COMM_WORLD, db_opts);
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
        // read the next command
        memset( commands[cmdIdx], 0, sizeof( command ));
        errno = 0;
        getLine( commands[cmdIdx]);
        
        if (verbose) tst_say( "\n##command %d: %s\n", cmdIdx, commands[cmdIdx]);
        
        // Is this the last/quit command?
        if( commands[cmdIdx][0] == 'q' || commands[cmdIdx][0] == 'Q' )
        {
            dowork = 0;
        }
        cmdIdx++;
    }
    cmdTot = cmdIdx -1;

    // main command execute loop
    for(cmdIdx=0; cmdIdx < cmdTot; cmdIdx++)
    {
        memset( command, 0, sizeof( command ));
        errno = 0;
        
        charIdx = getWordFromString( commands[cmdIdx], command, 0);

        if (verbose) tst_say( "\n##exec command: %s\n", command );
        begin = clock();
        
        // execute the command given
        if( !strcmp( command, "put" ))
        {
            execPut(commands[cmdIdx], md, charIdx);
        }
        else if( !strcmp( command, "get" ))
        {
            execGet(commands[cmdIdx], md, charIdx);
        }
        else if ( !strcmp( command, "bput" ))
        {
            execBput(commands[cmdIdx], md, charIdx);
        }
        else if ( !strcmp( command, "bget" ))
        {
            execBget(commands[cmdIdx], md, charIdx);
        }
        else if( !strcmp( command, "del" ))
        {
            execDel(commands[cmdIdx], md, charIdx);
        }
        else if( !strcmp( command, "bdel" ))
        {
            execBdel(commands[cmdIdx], md, charIdx);
        }
        else
        {
            printf( "# q       FOR QUIT\n" );
            //printf( "# open filename keyfile1,dkeyfile2,... update\n" );
            //printf( "# transaction < START | COMMIT | ROLLBACK >\n" );
            //printf( "# close\n" );
            //printf( "# flush\n" );
            printf( "# put key data\n" );
            printf( "# bput n key data\n" );
            //printf( "# find index key < LT | LE | FI | EQ | LA | GE | GT >\n" );
            //printf( "# nfind n index key < LT | LE | FI | EQ | LA | GE | GT >\n" );
            printf( "# get key\n" );
            printf( "# bget n key\n" );
            //printf( "# datalen\n" );
            //printf( "# readdata\n" );
            //printf( "# readkey index\n" );
            //printf( "# updatedata data\n" );
            //printf( "# updatekey index key\n" );
            printf( "# del key\n" );
            printf( "# bdel n key\n" );
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
