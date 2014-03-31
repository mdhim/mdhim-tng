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
#include <sys/time.h>

#include "mpi.h"
#include "mdhim.h"
#include "mdhim_options.h"

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

#define TEST_BUFLEN              4096

static FILE * logfile;
static FILE * infile;
int verbose = 1;   // By default generate lots of feedback status lines
int dbOptionAppend = MDHIM_DB_OVERWRITE;
int to_log = 0;
// MDHIM_INT_KEY=1, MDHIM_LONG_INT_KEY=2, MDHIM_FLOAT_KEY=3, MDHIM_DOUBLE_KEY=4
// MDHIM_LONG_DOUBLE_KEY=5, MDHIM_STRING_KEY=6, MDHIM_BYTE_KEY=7 
int key_type = 1;  // Default "int"

#define MAX_ERR_REPORT 50
static char *errMsgs[MAX_ERR_REPORT];
static int errMsgIdx = 0;

static int sc_len;  // Source Character string length for Random String generation
static char *sourceChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQ124567890"; 

// Labels for basic get operations
static char *getOpLabel[] = { "MDHIM_GET_EQ", "MDHIM_GET_NEXT", "MDHIM_GET_PREV",
				      "MDHIM_GET_FIRST", "MDHIM_GET_LAST"};

#ifdef _WIN32
#include <direct.h>
#endif

#include <stdarg.h>

// Add to error message list
static void addErrorMessage(char *msg)
{
// Max number of error messages reached ignore the rest, but still increment count
if (errMsgIdx > MAX_ERR_REPORT)
	errMsgIdx++;
else
	errMsgs[errMsgIdx++] = msg;
}

static void tst_say(
int err, char * format,
	...
	)
{
va_list ap;
    
if (err)
{
char *errMsg = (char *)malloc(sizeof(char)* TEST_BUFLEN);
va_start( ap, format );
vsnprintf(errMsg, TEST_BUFLEN, format, ap);
addErrorMessage(errMsg);
        
// Make sure error messages print to stderr when loging output
if (to_log) fprintf(stderr, "%s", errMsg); 
va_end(ap);
}
    
/*
 * use fprintf to give out the text
 */
if (to_log)
{
va_start( ap, format );
vfprintf( logfile, format, ap);
va_end(ap);
}
else
{
va_start( ap, format );
vfprintf( stdout, format, ap);
va_end(ap);
}
    
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

static int getWordFromString (char *aLine,  char *buffer, int charIdx )
{
int c;
int i;

// Check to see if past the end
if (charIdx >= strlen(aLine))
{
*buffer = '\0';
return charIdx;
}
    
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

/* Expands escapes from a sequence of characters to null terminated string
 *
 * src must be a sequence of characters.
 * len is the size of the sequence of characters to convert.
 * 
 * returns a string of size 2 * len + 1 will always be sufficient
 *
 */

char *expand_escapes(const char* src, int len) 
{
	char c;
	int i;
	char* dest;
	char* res;
  
	if ((res = malloc(2 * len + 1)) == NULL)
	{
		printf("Error allocating memory in expand_escapes.\n");
		return NULL;
	}
	dest = res;

	for (i=0 ; i<len-1; i++ ) {
		c = *(src++);
		switch(c) {
		case '\0': 
			*(dest++) = '\\';
			*(dest++) = '0';
			break;
		case '\a': 
			*(dest++) = '\\';
			*(dest++) = 'a';
			break;
		case '\b': 
			*(dest++) = '\\';
			*(dest++) = 'b';
			break;
		case '\t': 
			*(dest++) = '\\';
			*(dest++) = 't';
			break;
		case '\n': 
			*(dest++) = '\\';
			*(dest++) = 'n';
			break;
		case '\v': 
			*(dest++) = '\\';
			*(dest++) = 'v';
			break;
		case '\f': 
			*(dest++) = '\\';
			*(dest++) = 'f';
			break;
		case '\r': 
			*(dest++) = '\\';
			*(dest++) = 'r';
			break;
		case '\\': 
			*(dest++) = '\\';
			*(dest++) = '\\';
			break;
		case '\"': 
			*(dest++) = '\\';
			*(dest++) = '\"';
			break;
		default:
			*(dest++) = c;
		}
	}

	*dest = '\0'; /* Ensure null terminator */
	return res;
}

// Handle erroneous requests
char *getValLabel(int idx)
{
	if (idx < 0 || idx > (sizeof(getOpLabel) / sizeof(*getOpLabel)))
		return "Invalid_get_OPERATOR";
	else
		return getOpLabel[idx];
}

void usage(void)
{
	printf("Usage:\n");
	printf(" -f<BatchInputFileName> (file with batch commands)\n");
	printf(" -d<DataBaseType> (Type of DB to use: levelDB=1)\n");
        printf(" -t<IndexKeyType> (Type of keys: int=1, longInt=2, float=3, "
               "double=4, longDouble=5, string=6, byte=7)\n");
        printf(" -p<pathForDataBase> (path where DB will be created)\n");
        printf(" -n<DataBaseName> (Name of DataBase file or directory)\n");
        printf(" -b<DebugLevel> (MLOG_CRIT=1, MLOG_DBG=2)\n");
        printf(" -a (DB store append mode. By default records with same key are "
               "overwritten. This flag turns on the option to append to existing values.");
        printf(" -q<0|1> (Quiet mode, default is verbose) 1=write out to log file\n");
	exit (8);
}

// Release the memory used in a Bulk request. If values/value_lens not present just use NULL
void freeKeyValueMem(int nkeys, void **keys, int *key_lens, char **values, int *value_lens)
{
	int i;
	for (i = 0; i < nkeys; i++)
	{
		if (keys[i]) free(keys[i]);
		if (values && value_lens && value_lens[i] && values[i]) free(values[i]);
	}
	if (key_lens) free(key_lens);	
	if (keys) free(keys);
	if (value_lens) free(value_lens);
	if (values) free(values);
}

//======================================FLUSH============================
static void execFlush(char *command, struct mdhim_t *md, int charIdx)
{
	//Get the stats
	int ret = mdhimStatFlush(md, md->primary_index);

	if (ret != MDHIM_SUCCESS) {
		tst_say(1, "ERROR: rank %d executing flush.\n", md->mdhim_rank);
	} else {
		tst_say(0, "Flush executed successfully.\n");
	}
        
}

//======================================PUT============================
static void execPut(char *command, struct mdhim_t *md, int charIdx)
{
	int i_key;
	long l_key;
	float f_key;
	double d_key;
	struct mdhim_brm_t *brm;
	char str_key [ TEST_BUFLEN ];
	char buffer2 [ TEST_BUFLEN ];
	char key_string [ TEST_BUFLEN ];
	char value [ TEST_BUFLEN ];
	int ret;
    
	if (verbose) tst_say(0, "# put key data\n" );
	charIdx = getWordFromString( command, str_key, charIdx);
	// Get value to store
	charIdx = getWordFromString( command, buffer2, charIdx);
	sprintf(value, "%s_%d", buffer2, (md->mdhim_rank + 1));
    
	// Based on key type generate a key using rank 
	switch (key_type)
	{
        case MDHIM_INT_KEY:
		i_key = atoi(str_key) * (md->mdhim_rank + 1);
		sprintf(key_string, "%d", i_key);
		if (verbose) tst_say(0, "# mdhimPut( %s, %s) [int]\n", key_string, value );
		brm = mdhimPut(md, &i_key, 
			       sizeof(i_key), value, 
			       strlen(value)+1, NULL, NULL);
		break;
             
        case MDHIM_LONG_INT_KEY:
		l_key = atol(str_key) * (md->mdhim_rank + 1);
		sprintf(key_string, "%ld", l_key);
		if (verbose) tst_say(0, "# mdhimPut( %s, %s) [long]\n", key_string, value );
		brm = mdhimPut(md, &l_key, sizeof(l_key), 
			       value, strlen(value)+1, 
			       NULL, NULL);
		break;

        case MDHIM_FLOAT_KEY:
		f_key = atof( str_key ) * (md->mdhim_rank + 1);
		sprintf(key_string, "%f", f_key);
		if (verbose) tst_say(0, "# mdhimPut( %s, %s ) [float]\n", key_string, value );
		brm = mdhimPut(md, &f_key, 
			       sizeof(f_key), value, 
			       strlen(value)+1, NULL, NULL);
		break;
            
	case MDHIM_DOUBLE_KEY:
		d_key = atof( str_key ) * (md->mdhim_rank + 1);
		sprintf(key_string, "%e", d_key);
		if (verbose) tst_say(0, "# mdhimPut( %s, %s ) [double]\n", key_string, value );
		brm = mdhimPut(md, &d_key, 
			       sizeof(d_key), value, 
			       strlen(value)+1, NULL, NULL);
		break;
                                     
        case MDHIM_STRING_KEY:
        case MDHIM_BYTE_KEY:
		sprintf(key_string, "%s0%d", str_key, (md->mdhim_rank + 1));
		if (verbose) tst_say(0, "# mdhimPut( %s, %s) [string|byte]\n", key_string, value );
		brm = mdhimPut(md, (void *)key_string, 
			       strlen(key_string), value, 
			       strlen(value)+1, NULL, NULL);
		break;
             
        default:
		tst_say(1, "ERROR: unrecognized Key_type in execPut\n");
	}

	// Report any error(s)
	if (!brm || brm->error)
	{
		tst_say(1, "ERROR: rank %d putting key: %s with value: %s into MDHIM\n", 
			md->mdhim_rank, key_string, value);
	}
	else
	{
		tst_say(0, "Successfully put key/value into MDHIM\n");
	}
    
	//Commit the database
	ret = mdhimCommit(md, md->primary_index);
	if (ret != MDHIM_SUCCESS)
	{
		tst_say(1, "ERROR: rank %d committing put key: %s to MDHIM database\n", 
			md->mdhim_rank, key_string);
	}
	else
	{
		if (verbose) tst_say(0, "Committed put to MDHIM database\n");
	}

}

//======================================GET============================
// Operations for getting a key/value from messages.h
// MDHIM_GET_EQ=0, MDHIM_GET_NEXT=1, MDHIM_GET_PREV=2
// MDHIM_GET_FIRST=3, MDHIM_GET_LAST=4
static void execGet(char *command, struct mdhim_t *md, int charIdx)
{
	int i_key;
	long l_key;
	float f_key;
	double d_key;
	struct mdhim_bgetrm_t *bgrm;
	char str_key [ TEST_BUFLEN ];
	char buffer2 [ TEST_BUFLEN ];
	char *v_value = NULL;
	char key_string [ TEST_BUFLEN ];
	char returned_key [ TEST_BUFLEN ];
	int getOp, newIdx;
    
	if (verbose) tst_say(0, "# get key <getOperator verfication_value>\n" );

	charIdx = getWordFromString( command, str_key, charIdx);
	newIdx = getWordFromString( command, buffer2, charIdx);
    
	if (newIdx != charIdx)
	{
		getOp = atoi(buffer2); // Get operation type
		charIdx = newIdx;
        
		// Is there a verification value?
		newIdx = getWordFromString( command, buffer2, charIdx);
		if (newIdx != charIdx)
		{
			v_value = malloc(sizeof(char) * TEST_BUFLEN);
			sprintf(v_value, "%s_%d", buffer2, (md->mdhim_rank + 1));//Value to verify
		}
	}
	else
	{
		getOp = MDHIM_GET_EQ;  //Default a get with an equal operator
	}
    
	// Based on key type generate a key using rank 
	switch (key_type)
	{
	case MDHIM_INT_KEY:
		i_key = atoi( str_key ) * (md->mdhim_rank + 1);
		sprintf(key_string, "%d", i_key);
		if (verbose) tst_say(0, "# mdhimGet( %s, %s ) [int]\n", key_string, getValLabel(getOp));
		bgrm = mdhimGet(md, md->primary_index, 
				&i_key, sizeof(i_key), getOp);
		break;
            
	case MDHIM_LONG_INT_KEY:
		l_key = atol( str_key ) * (md->mdhim_rank + 1);
		sprintf(key_string, "%ld", l_key);
		if (verbose) tst_say(0, "# mdhimGet( %s, %s ) [long]\n", key_string, getValLabel(getOp));
		bgrm = mdhimGet(md, md->primary_index, 
				&l_key, sizeof(l_key), getOp);
		break;
            
	case MDHIM_FLOAT_KEY:
		f_key = atof( str_key ) * (md->mdhim_rank + 1);
		sprintf(key_string, "%f", f_key);
		if (verbose) tst_say(0, "# mdhimGet( %s, %s ) [float]\n", key_string, getValLabel(getOp));
		bgrm = mdhimGet(md, md->primary_index, 
				&f_key, sizeof(f_key), getOp);
		break;
            
	case MDHIM_DOUBLE_KEY:
		d_key = atof( str_key ) * (md->mdhim_rank + 1);
		sprintf(key_string, "%e", d_key);
		if (verbose) tst_say(0, "# mdhimGet( %s, %s ) [double]\n", key_string, getValLabel(getOp));
		bgrm = mdhimGet(md, md->primary_index, 
				&d_key, sizeof(d_key), getOp);
		break;
                        
	case MDHIM_STRING_KEY:
	case MDHIM_BYTE_KEY:
		sprintf(key_string, "%s0%d", str_key, (md->mdhim_rank + 1));
		if (verbose) tst_say(0, "# mdhimGet( %s, %s ) [string|byte]\n", key_string, getValLabel(getOp));
		bgrm = mdhimGet(md, md->primary_index, 
				(void *)key_string, strlen(key_string), 
				getOp);
		break;
 
	default:
		tst_say(1, "Error, unrecognized Key_type in execGet\n");
		return;
	}
    
	if (!bgrm || bgrm->error)
	{
		tst_say(1, "ERROR: rank %d getting value for key (%s): %s from MDHIM\n", 
			md->mdhim_rank, getValLabel(getOp), key_string);
	}
	else if (bgrm->keys[0] && bgrm->values[0])
	{
		// Generate correct string from returned key
		switch (key_type)
		{
		case MDHIM_INT_KEY:
			sprintf(returned_key, "[int]: %d", *((int *) bgrm->keys[0]));
			break;
               
		case MDHIM_LONG_INT_KEY:
			sprintf(returned_key, "[long]: %ld", *((long *) bgrm->keys[0]));
			break;
            
		case MDHIM_FLOAT_KEY:
			sprintf(returned_key, "[float]: %f", *((float *) bgrm->keys[0]));
			break;
            
		case MDHIM_DOUBLE_KEY:
			sprintf(returned_key, "[double]: %e", *((double *) bgrm->keys[0]));
			break;
                        
		case MDHIM_STRING_KEY:
		case MDHIM_BYTE_KEY:
			sprintf(returned_key, "[string|byte]: %s", (char *)bgrm->keys[0]);
   
		}
        
		if ( v_value == NULL )  // No verification value, anything is OK.
		{
			tst_say(0, "Successfully get(%s) value: %s for key %s from MDHIM\n", 
				getValLabel(getOp), expand_escapes(bgrm->values[0], bgrm->value_lens[0]), returned_key);
		}
		else if (memcmp(expand_escapes(bgrm->values[0], bgrm->value_lens[0]), v_value, bgrm->value_lens[0]))
		{
			tst_say(1, "ERROR: rank %d incorrect get(%s) value: %s for key %s from MDHIM expecting %s\n", 
				md->mdhim_rank, getValLabel(getOp), expand_escapes(bgrm->values[0], bgrm->value_lens[0]), returned_key, v_value);
		}
		else
		{
			tst_say(0, "Successfully get(%s) correct value: %s for key %s from MDHIM\n", 
				getValLabel(getOp), expand_escapes(bgrm->values[0], bgrm->value_lens[0]), returned_key);
		}

	}
	else
	{
		tst_say(1, "ERROR: rank %d got(%s) null value or return key for key: %s from MDHIM\n", 
			md->mdhim_rank, getValLabel(getOp), key_string);
	}

	if (v_value) free(v_value);
}

//======================================BPUT============================
static void execBput(char *command, struct mdhim_t *md, int charIdx)
{
	int nkeys = 100;
	int ret;
	char buffer1 [ TEST_BUFLEN ];
	char str_key [ TEST_BUFLEN ];
	char value [ TEST_BUFLEN ];
	struct mdhim_brm_t *brm, *brmp;
	int i, size_of;
	void **keys;
	int *key_lens;
	char **values;
	int *value_lens;    
	if (verbose) tst_say(0, "# bput n key data\n" );

	//Initialize variables
	size_of = 0;
	keys = NULL;

	// Number of keys to generate
	charIdx = getWordFromString( command, buffer1, charIdx);
	nkeys = atoi( buffer1 );
    
	key_lens = malloc(sizeof(int) * nkeys);
	value_lens = malloc(sizeof(int) * nkeys);
    
	// starting key value
	charIdx = getWordFromString( command, str_key, charIdx);

	// starting data value
	charIdx = getWordFromString( command, value, charIdx);

	if (verbose) tst_say(0, "# mdhimBPut(%d, %s, %s )\n", nkeys, str_key, value );

	// Allocate memory and size of key (size of string|byte key will be modified
	// when the key is constructed.)
	values = malloc(sizeof(char *) * nkeys);
	switch (key_type)
	{
	case MDHIM_INT_KEY:
		keys = malloc(sizeof(int *) * nkeys);
		size_of = sizeof(int);
		break;
           
	case MDHIM_LONG_INT_KEY:
		keys = malloc(sizeof(long *) * nkeys);
		size_of = sizeof(long);
		break;
           
	case MDHIM_FLOAT_KEY:
		keys = malloc(sizeof(float *) * nkeys);
		size_of = sizeof(float);
		break;
           
	case MDHIM_DOUBLE_KEY:
		keys = malloc(sizeof(double *) * nkeys);
		size_of = sizeof(double);
		break;
           
	case MDHIM_STRING_KEY:
	case MDHIM_BYTE_KEY:
		keys = malloc(sizeof(char *) * nkeys);
		size_of = sizeof(char);
		break;
	}    

	// Create the keys and values to store
	for (i = 0; i < nkeys; i++)
	{
		keys[i] = malloc(size_of);
		key_lens[i] = size_of;
		values[i] = malloc(sizeof(char) * TEST_BUFLEN);
		sprintf(values[i], "%s_%d_%d", value, (md->mdhim_rank + 1), (i + 1));
		value_lens[i] = strlen(values[i]) + 1;

		// Based on key type, rank and index number generate a key
		switch (key_type)
		{
		case MDHIM_INT_KEY:
                {
			int **i_keys = (int **)keys;
			*i_keys[i] = (atoi( str_key ) * (md->mdhim_rank + 1)) + (i + 1);
			if (verbose) tst_say(0, "Rank: %d - Creating int key (to insert): "
					     "%d with value: %s\n", 
					     md->mdhim_rank, *i_keys[i], values[i]);
                }
                break;

		case MDHIM_LONG_INT_KEY:
                {
			long **l_keys = (long **)keys;
			*l_keys[i] = (atol( str_key ) * (md->mdhim_rank + 1)) + (i + 1);
			if (verbose) tst_say(0, "Rank: %d - Creating long key (to insert): "
					     "%ld with value: %s\n", 
					     md->mdhim_rank, *l_keys[i], values[i]);
                }
                break;
                
		case MDHIM_FLOAT_KEY:
                {
			float **f_keys = (float **)keys;
			*f_keys[i] = (atof( str_key ) * (md->mdhim_rank + 1)) + (i + 1);
			if (verbose) tst_say(0, "Rank: %d - Creating float key (to insert): "
					     "%f with value: %s\n", 
					     md->mdhim_rank, *f_keys[i], values[i]);
		}
                break;

		case MDHIM_DOUBLE_KEY:
                {
			double **d_keys = (double **)keys;
			*d_keys[i] = (atof( str_key ) * (md->mdhim_rank + 1)) + (i + 1);
			if (verbose) tst_say(0, "Rank: %d - Creating double key (to insert): "
					     "%e with value: %s\n", 
					     md->mdhim_rank, *d_keys[i], values[i]);
                }
                break;
                
		case MDHIM_STRING_KEY:
		case MDHIM_BYTE_KEY:
                {
			char **s_keys = (char **)keys;
			s_keys[i] = malloc(size_of * TEST_BUFLEN);
			sprintf(s_keys[i], "%s0%d0%d", str_key, (md->mdhim_rank + 1), (i + 1));
			key_lens[i] = strlen(s_keys[i]);
			if (verbose) tst_say(0, "Rank: %d - Creating string|byte key "
					     "(to insert): %s with value: %s\n", 
					     md->mdhim_rank, (char *)s_keys[i], values[i]);
                }
                break;
		}		
	}

	//Insert the keys into MDHIM
	brm = mdhimBPut(md, keys, key_lens, (void **) values, 
			value_lens, nkeys, NULL, NULL);
	brmp = brm;
	ret = 0;
	if (!brm || brm->error)
	{
		tst_say(1, "ERROR: rank - %d bulk inserting keys/values into MDHIM\n", 
			md->mdhim_rank);
		ret = 1;
	}
    
	while (brmp)
	{
		if (brmp->error < 0)
		{
			tst_say(1, "ERROR: rank %d - Error bulk inserting key/values info MDHIM\n", 
				md->mdhim_rank);
			ret = 1;
		}

		brmp = brmp->next;
		//Free the message
		mdhim_full_release_msg(brm);
		brm = brmp;
	}
    
	// if NO errors report success
	if (!ret)
	{
		tst_say(0, "Rank: %d - Successfully bulk inserted key/values into MDHIM\n", 
                        md->mdhim_rank);
	}

	//Commit the database
	ret = mdhimCommit(md, md->primary_index);
	if (ret != MDHIM_SUCCESS)
	{
		tst_say(1, "ERROR: rank %d committing bput to MDHIM database\n", md->mdhim_rank);
	}
	else
	{
		if (verbose) tst_say(0, "Committed bput to MDHIM database\n");
	}
    
	// Release memory
	freeKeyValueMem(nkeys, keys, key_lens, values, value_lens);
}
        
//======================================BGET============================
static void execBget(char *command, struct mdhim_t *md, int charIdx)
{
	int nkeys = 100;
	char buffer [ TEST_BUFLEN ];
	char str_key [ TEST_BUFLEN ];
	char *v_value = NULL;
	if (verbose) tst_say(0, "# bget n key <verfication_value>\n" );
	struct mdhim_bgetrm_t *bgrm, *bgrmp;
	int i, size_of, ret, newIdx;
	void **keys;
	int *key_lens;
	int totRecds;

	size_of = 0;
	keys = NULL;

	// Get the number of records to create for bget
	charIdx = getWordFromString( command, buffer, charIdx);
	nkeys = atoi( buffer );
    
	key_lens = malloc(sizeof(int) * nkeys);
    
	// Get the key to use as starting point
	charIdx = getWordFromString( command, str_key, charIdx);
    
	// Is there a verification value?
	newIdx = getWordFromString( command, buffer, charIdx);
	if (newIdx != charIdx)
	{
		v_value = malloc(sizeof(char) * TEST_BUFLEN);
		sprintf(v_value, "%s_%d", buffer, (md->mdhim_rank + 1)); //Value to verify
	}

	if (verbose) tst_say(0, "# mdhimBGet(%d, %s)\n", nkeys, str_key );

	// Allocate memory and size of key (size of string|byte key will be modified
	// when the key is constructed.)
	switch (key_type)
	{
	case MDHIM_INT_KEY:
		keys = malloc(sizeof(int *) * nkeys);
		size_of = sizeof(int);
		break;
           
	case MDHIM_LONG_INT_KEY:
		keys = malloc(sizeof(long *) * nkeys);
		size_of = sizeof(long);
		break;
           
	case MDHIM_FLOAT_KEY:
		keys = malloc(sizeof(float *) * nkeys);
		size_of = sizeof(float);
		break;
           
	case MDHIM_DOUBLE_KEY:
		keys = malloc(sizeof(double *) * nkeys);
		size_of = sizeof(double);
		break;
                 
	case MDHIM_STRING_KEY:
	case MDHIM_BYTE_KEY:
		keys = malloc(sizeof(char *) * nkeys);
		size_of = sizeof(char);
		break;
	}
    
	// Generate the keys as set above
	for (i = 0; i < nkeys; i++)
	{   
		keys[i] = malloc(size_of);
		key_lens[i] = size_of;
        
		// Based on key type, rank and index number generate a key
		switch (key_type)
		{
		case MDHIM_INT_KEY:
                {
			int **i_keys = (int **)keys;
			*i_keys[i] = (atoi( str_key ) * (md->mdhim_rank + 1)) + (i + 1);
			if (verbose) tst_say(0, "Rank: %d - Creating int key (to get): %d\n", 
					     md->mdhim_rank, *i_keys[i]);
                }
                break;

		case MDHIM_LONG_INT_KEY:
                {
			long **l_keys = (long **)keys;
			*l_keys[i] = (atol( str_key ) * (md->mdhim_rank + 1)) + (i + 1);
			if (verbose) tst_say(0, "Rank: %d - Creating long key (to get): %ld\n", 
					     md->mdhim_rank, *l_keys[i]);
                }
                break;

		case MDHIM_FLOAT_KEY:
                {
			float **f_keys = (float **)keys;
			*f_keys[i] = (atof( str_key ) * (md->mdhim_rank + 1)) + (i + 1);
			if (verbose) tst_say(0, "Rank: %d - Creating float key (to get): %f\n", 
					     md->mdhim_rank, *f_keys[i]);
                }
                break;

		case MDHIM_DOUBLE_KEY:
                {
			double **d_keys = (double **)keys;
			*d_keys[i] = (atof( str_key ) * (md->mdhim_rank + 1)) + (i + 1);
			if (verbose) tst_say(0, "Rank: %d - Creating double key (to get): "
					     " %e\n", md->mdhim_rank, *d_keys[i]);
                }
                break; 

		case MDHIM_STRING_KEY:
		case MDHIM_BYTE_KEY:
                {
			char **s_keys = (char **)keys;
			s_keys[i] = malloc(size_of * TEST_BUFLEN);
			sprintf(s_keys[i], "%s0%d0%d", str_key, (md->mdhim_rank + 1), (i + 1));
			key_lens[i] = strlen(s_keys[i]);
			if (verbose) tst_say(0, "Rank: %d - Creating string|byte key (to get):"
					     " %s\n", md->mdhim_rank, s_keys[i]);
                }
                break;
  
		default:
			tst_say(1, "Error, unrecognized Key_type in execBGet\n");
			return;
		}
            		
	}
    
	//Get the values back for each key retrieved
	bgrm = mdhimBGet(md, md->primary_index, keys, key_lens, nkeys, MDHIM_GET_EQ);
	ret = 0; // Used to determine if any errors are encountered
    
	totRecds = 0;
	bgrmp = bgrm;
	while (bgrmp) {
		if (bgrmp->error < 0)
		{
			tst_say(1, "ERROR: rank %d retrieving values\n", md->mdhim_rank);
			ret = 1;
		}

		totRecds += bgrmp->num_keys;
		for (i = 0; i < bgrmp->num_keys && bgrmp->error >= 0; i++)
		{
			if ( v_value != NULL ) 
				sprintf(buffer, "%s_%d", v_value, i + 1); //Value to verify
            
			if ( v_value == NULL )  // No verification value, anything is OK.
			{
				if (verbose) tst_say(0, "Rank: %d successfully get[%d] value: %s from MDHIM\n", 
						     md->mdhim_rank, i, expand_escapes(bgrmp->values[i], 
										       bgrmp->value_lens[i]));
			}
			else if (memcmp(expand_escapes(bgrmp->values[i], bgrmp->value_lens[i]), buffer, bgrmp->value_lens[i]))
			{
				tst_say(1, "ERROR: rank %d incorrect get[%d] value: %s from MDHIM expecting %s\n", 
					md->mdhim_rank, i, expand_escapes(bgrmp->values[i], bgrmp->value_lens[i]), buffer);
			}
			else
			{
				if (verbose) tst_say(0, "Rank: %d successfully get[%d] correct value: %s from MDHIM\n", 
						     md->mdhim_rank, i, expand_escapes(bgrmp->values[i], bgrmp->value_lens[i]));
			}
		}

		bgrmp = bgrmp->next;
		//Free the message received
		mdhim_full_release_msg(bgrm);
		bgrm = bgrmp;
	}
    
	// if NO errors report success
	if (!ret)
	{
		if (totRecds)
			tst_say(0, "Rank: %d - Successfully bulk retrieved %d key/values from MDHIM\n", 
				md->mdhim_rank, totRecds);
		else
			tst_say(1, "ERROR: rank %d got no records for bulk retrieved from MDHIM\n", 
				md->mdhim_rank);
	}
    
	// Release memory
	freeKeyValueMem(nkeys, keys, key_lens, NULL, NULL);
	free(v_value);
}

//======================================BGETOP============================
static void execBgetOp(char *command, struct mdhim_t *md, int charIdx)
{
	int nrecs = 100;
	char buffer1 [ TEST_BUFLEN ];
	char key_string [ TEST_BUFLEN ];
	struct mdhim_bgetrm_t *bgrm;
	int i, getOp;
	int i_key;
	long l_key;
	float f_key;
	double d_key;
    
	if (verbose) tst_say(0, "# bgetop n key op\n" );
    
	bgrm = NULL;
	// Get the number of records to retrieve in bgetop
	charIdx = getWordFromString( command, buffer1, charIdx);
	nrecs = atoi( buffer1 );
    
	// Get the key to use as starting point
	charIdx = getWordFromString( command, key_string, charIdx);
    
	// Get the operation type to use
	charIdx = getWordFromString( command, buffer1, charIdx);
	getOp = atoi( buffer1 );

	if (verbose) tst_say(0, "# mdhimBGetOp(%d, %s, %s)\n", 
			     nrecs, key_string, getValLabel(getOp) );

	// Based on key type generate a key using rank 
	switch (key_type)
	{
	case MDHIM_INT_KEY:
		i_key = atoi( key_string ) * (md->mdhim_rank + 1) + 1;
		sprintf(key_string, "%d", i_key);
		if (verbose) tst_say(0, "# mdhimBGetOp( %d, %s, %s ) [int]\n", 
				     nrecs, key_string, getValLabel(getOp));
		bgrm = mdhimBGetOp(md, md->primary_index, &i_key, sizeof(i_key), nrecs, 
				   getOp);
		break;
            
	case MDHIM_LONG_INT_KEY:
		l_key = atol( key_string ) * (md->mdhim_rank + 1) + 1;
		sprintf(key_string, "%ld", l_key);
		if (verbose) tst_say(0, "# mdhimBGetOp( %d, %s, %s ) [long]\n", 
				     nrecs, key_string, getValLabel(getOp));
		bgrm = mdhimBGetOp(md, md->primary_index, &l_key, sizeof(l_key), nrecs, 
				   getOp);
		break;
            
	case MDHIM_FLOAT_KEY:
		f_key = atof( key_string ) * (md->mdhim_rank + 1) + 1;
		sprintf(key_string, "%f", f_key);
		if (verbose) tst_say(0, "# mdhimBGetOp( %d, %s, %s ) [float]\n", 
				     nrecs, key_string, getValLabel(getOp));
		bgrm = mdhimBGetOp(md, md->primary_index, &f_key, sizeof(f_key), nrecs, 
				   getOp);
		break;
            
	case MDHIM_DOUBLE_KEY:
		d_key = atof( key_string ) * (md->mdhim_rank + 1) + 1;
		sprintf(key_string, "%e", d_key);
		if (verbose) tst_say(0, "# mdhimBGetOp( %d, %s, %s ) [double]\n", 
				     nrecs, key_string, getValLabel(getOp));
		bgrm = mdhimBGetOp(md, md->primary_index, &d_key, sizeof(d_key), nrecs, 
				   getOp);
		break;
                        
	case MDHIM_STRING_KEY:
	case MDHIM_BYTE_KEY:
		sprintf(key_string, "%s0%d0%d", key_string, (md->mdhim_rank + 1), 1);
		if (verbose) tst_say(0, "# mdhimBGetOp( %d, %s, %s ) [string|byte]\n", 
				     nrecs, key_string, getValLabel(getOp));
		bgrm = mdhimBGetOp(md, md->primary_index, (void *)key_string, strlen(key_string), 
				   nrecs, getOp);
		break;
 
	default:
		tst_say(1, "Error, unrecognized Key_type in execGet\n");
	}
    
	if (!bgrm || bgrm->error)
	{
		tst_say(1, "ERROR: rank %d getting %d values for start key (%s): %s from MDHIM\n", 
			md->mdhim_rank, nrecs, getValLabel(getOp), key_string);
	}
	else if (verbose)
	{
		for (i = 0; i < bgrm->num_keys && !bgrm->error; i++)
		{			
			tst_say(0, "Rank: %d - Got value[%d]: %s for start key: %s from MDHIM\n", 
				md->mdhim_rank, i, expand_escapes(bgrm->values[i], bgrm->value_lens[i]), 
				key_string);
		}
	}
	else
	{
		tst_say(0, "Rank: %d - Successfully got %d values for start key: %s from MDHIM\n", 
			md->mdhim_rank, bgrm->num_keys, key_string);
	}
    
	//Free the message received
	mdhim_full_release_msg(bgrm);
}
        
//======================================DEL============================
static void execDel(char *command, struct mdhim_t *md, int charIdx)
{
	int i_key;
	long l_key;
	float f_key;
	double d_key;
	char str_key [ TEST_BUFLEN ];
	char key_string [ TEST_BUFLEN ];
	struct mdhim_brm_t *brm;

	if (verbose) tst_say(0, "# del key\n" );

	brm = NULL;
	charIdx = getWordFromString( command, str_key, charIdx);
    
	switch (key_type)
	{
	case MDHIM_INT_KEY:
		i_key = atoi( str_key ) * (md->mdhim_rank + 1);
		sprintf(key_string, "%d", i_key);
		if (verbose) tst_say(0, "# mdhimDelete( %s ) [int]\n", key_string);
		brm = mdhimDelete(md, md->primary_index, &i_key, sizeof(i_key));
		break;
            
	case MDHIM_LONG_INT_KEY:
		l_key = atol( str_key ) * (md->mdhim_rank + 1);
		sprintf(key_string, "%ld", l_key);
		if (verbose) tst_say(0, "# mdhimDelete( %s ) [long]\n", key_string);
		brm = mdhimDelete(md, md->primary_index, &l_key, sizeof(l_key));
		break;
            
	case MDHIM_FLOAT_KEY:
		f_key = atof( str_key ) * (md->mdhim_rank + 1);
		sprintf(key_string, "%f", f_key);
		if (verbose) tst_say(0, "# mdhimDelete( %s ) [float]\n", key_string);
		brm = mdhimDelete(md, md->primary_index, &f_key, sizeof(f_key));
		break;
            
	case MDHIM_DOUBLE_KEY:
		d_key = atof( str_key ) * (md->mdhim_rank + 1);
		sprintf(key_string, "%e", d_key);
		if (verbose) tst_say(0, "# mdhimDelete( %s ) [double]\n", key_string);
		brm = mdhimDelete(md, md->primary_index, &d_key, sizeof(d_key));
		break;
                        
	case MDHIM_STRING_KEY:
	case MDHIM_BYTE_KEY:
		sprintf(key_string, "%s0%d", str_key, (md->mdhim_rank + 1));
		if (verbose) tst_say(0, "# mdhimDelete( %s ) [string|byte]\n", key_string);
		brm = mdhimDelete(md, md->primary_index, (void *)key_string, strlen(key_string));
		break;
 
	default:
		tst_say(1, "Error, unrecognized Key_type in execDelete\n");
	}

	if (!brm || brm->error)
	{
		tst_say(1, "ERROR: rank %d deleting key/value from MDHIM. key: %s\n", 
			md->mdhim_rank, key_string);
	}
	else
	{
		tst_say(0, "Successfully deleted key/value from MDHIM. key: %s\n", key_string);
	}

}

//======================================NDEL============================
static void execBdel(char *command, struct mdhim_t *md, int charIdx)
{
	int nkeys = 100;
	char buffer1 [ TEST_BUFLEN ];
	char str_key [ TEST_BUFLEN ];
	void **keys;
	int *key_lens;
	struct mdhim_brm_t *brm, *brmp;
	int i, size_of, ret;
    
	if (verbose) tst_say(0, "# bdel n key\n" );
    
	keys = NULL;
	size_of = 0;

	// Number of records to delete
	charIdx = getWordFromString( command, buffer1, charIdx);
	nkeys = atoi( buffer1 );
	key_lens = malloc(sizeof(int) * nkeys);

	// Starting key value
	charIdx = getWordFromString( command, str_key, charIdx);

	if (verbose) tst_say(0, "# mdhimBDelete(%d, %s )\n", nkeys, str_key );
    
	// Allocate memory and size of key (size of string|byte key will be modified
	// when the key is constructed.)
	switch (key_type)
	{
	case MDHIM_INT_KEY:
		keys = malloc(sizeof(int *) * nkeys);
		size_of = sizeof(int);
		break;
           
	case MDHIM_LONG_INT_KEY:
		keys = malloc(sizeof(long *) * nkeys);
		size_of = sizeof(long);
		break;
           
	case MDHIM_FLOAT_KEY:
		keys = malloc(sizeof(float *) * nkeys);
		size_of = sizeof(float);
		break;
           
	case MDHIM_DOUBLE_KEY:
		keys = malloc(sizeof(double *) * nkeys);
		size_of = sizeof(double);
		break;
                 
	case MDHIM_STRING_KEY:
	case MDHIM_BYTE_KEY:
		keys = malloc(sizeof(char *) * nkeys);
		size_of = sizeof(char);
		break;
	}

	for (i = 0; i < nkeys; i++)
	{
		keys[i] = malloc(size_of);
		key_lens[i] = size_of;

		// Based on key type, rank and index number generate a key
		switch (key_type)
		{
		case MDHIM_INT_KEY:
                {
			int **i_keys = (int **)keys;
			*i_keys[i] = (atoi( str_key ) * (md->mdhim_rank + 1)) + (i + 1);
			if (verbose) tst_say(0, "Rank: %d - Creating int key (to delete): %d\n", 
					     md->mdhim_rank, *i_keys[i]);
                }
                break;

		case MDHIM_LONG_INT_KEY:
                {
			long **l_keys = (long **)keys;
			*l_keys[i] = (atol( str_key ) * (md->mdhim_rank + 1)) + (i + 1);
			if (verbose) tst_say(0, "Rank: %d - Creating long key (to delete): %ld\n", 
					     md->mdhim_rank, *l_keys[i]);
                }
                break;

		case MDHIM_FLOAT_KEY:
                {
			float **f_keys = (float **)keys;
			*f_keys[i] = (atof( str_key ) * (md->mdhim_rank + 1)) + (i + 1);
			if (verbose) tst_say(0, "Rank: %d - Creating float key (to delete): %f\n", 
					     md->mdhim_rank, *f_keys[i]);
                }
                break;

		case MDHIM_DOUBLE_KEY:
                {
			double **d_keys = (double **)keys;
			*d_keys[i] = (atof( str_key ) * (md->mdhim_rank + 1)) + (i + 1);
			if (verbose) tst_say(0, "Rank: %d - Creating double key (to delete): "
					     " %e\n", md->mdhim_rank, *d_keys[i]);
                }
                break; 

		case MDHIM_STRING_KEY:
		case MDHIM_BYTE_KEY:
                {
			char **s_keys = (char **)keys;
			s_keys[i] = malloc(size_of * TEST_BUFLEN);
			sprintf(s_keys[i], "%s0%d0%d", str_key, (md->mdhim_rank + 1), (i + 1));
			key_lens[i] = strlen(s_keys[i]); 
			if (verbose) tst_say(0, "Rank: %d - Creating string|byte key (to delete):"
					     " %s\n", md->mdhim_rank, s_keys[i]);
                }
                break;
  
		default:
			tst_say(1, "Error, unrecognized Key_type in execBDel\n");
			return;
		}
            
	}

	//Delete the records
	brm = mdhimBDelete(md, md->primary_index, (void **) keys, key_lens, nkeys);
	brmp = brm;
	if (!brm || brm->error) {
		tst_say(1, "ERROR: rank %d deleting keys/values from MDHIM\n", 
			md->mdhim_rank);
	} 
    
	ret = 0;
	while (brmp)
	{
		if (brmp->error < 0)
		{
			tst_say(1, "ERROR: rank %d deleting keys\n", md->mdhim_rank);
			ret = 1;
		}

		brmp = brmp->next;
		//Free the message
		mdhim_full_release_msg(brm);
		brm = brmp;
	}
    
	// if NO errors report success
	if (!ret)
	{
		tst_say(0, "Rank: %d - Successfully bulk deleted key/values from MDHIM\n", 
                        md->mdhim_rank);
	}

	// Release memory
	freeKeyValueMem(nkeys, keys, key_lens, NULL, NULL);
}

// Generate a random string of up to max_len
char *random_string( int max_len, int exact_size)
{
	int len;
	char *retVal;
	int i;

	if (exact_size)
		len = max_len;
	else
		len = rand() % max_len + 1;
	retVal = (char *) malloc( len + 1 );

	for (i=0; i<len; ++i)
		retVal[i] = sourceChars[ rand() % sc_len ];

	retVal[len] = '\0';
	return retVal;
} 

//======================================NPUT============================
static void execNput(char *command, struct mdhim_t *md, int charIdx)
{
	int i_key;
	long l_key;
	float f_key;
	double d_key;
	struct mdhim_brm_t *brm;
	char buffer [ TEST_BUFLEN ];
	char *key_string; 
	char *value;
	int n_iter, key_len, value_len, rand_str_size;
	int ret, i;

	brm = NULL;
	key_string = malloc(sizeof(char) * TEST_BUFLEN);
	ret = 0;
    
	if (verbose) tst_say(0, "# nput n key_length data_length exact_size\n" );
    
	charIdx = getWordFromString( command, buffer, charIdx);
	n_iter = atoi( buffer ); // Get number of iterations
    
	charIdx = getWordFromString( command, buffer, charIdx);
	key_len = atoi( buffer ); // For string/byte key types otherwise ignored (but required)
    
	charIdx = getWordFromString( command, buffer, charIdx);
	value_len = atoi( buffer ); // Get maximum length of value string
    
	charIdx = getWordFromString( command, buffer, charIdx);
	// If zero strings are of the exact length stated above, otherwise they are
	// of variable length up to data_len or key_len.
	rand_str_size = atoi( buffer );  
    
	for (i=0; i<n_iter; i++)
	{
		if (i > 0) free(value);
		value = random_string(value_len, rand_str_size);
    
		// Based on key type generate appropriate random key
		switch (key_type)
		{
		case MDHIM_INT_KEY:
			i_key = rand() / 5;
			sprintf(key_string, "%d", i_key);
			if (verbose) tst_say(0, "# mdhimPut( %s, %s ) [int]\n", 
					     key_string, value );
			brm = mdhimPut(md, &i_key, 
				       sizeof(i_key), value, 
				       strlen(value)+1, NULL, NULL);
			break;

		case MDHIM_LONG_INT_KEY:
			l_key = rand() / 3;
			sprintf(key_string, "%ld", l_key);
			if (verbose) tst_say(0, "# mdhimPut( %s, %s ) [long]\n", 
					     key_string, value );
			brm = mdhimPut(md, &l_key, sizeof(l_key), value, 
				       strlen(value)+1, NULL, NULL);
			break;

		case MDHIM_FLOAT_KEY:
			f_key = rand() / 5.0;
			sprintf(key_string, "%f", f_key);
			if (verbose) tst_say(0, "# mdhimPut( %s, %s ) [float]\n", 
					     key_string, value );
			brm = mdhimPut(md, &f_key, sizeof(f_key), value, 
				       strlen(value)+1, NULL, NULL);
			break;

		case MDHIM_DOUBLE_KEY:
			d_key = rand() / 3.0;
			sprintf(key_string, "%e", d_key);
			if (verbose) tst_say(0, "# mdhimPut( %s, %s ) [double]\n", 
					     key_string, value );
			brm = mdhimPut(md, &d_key, sizeof(d_key), value, 
				       strlen(value)+1, NULL, NULL);
			break;

		case MDHIM_STRING_KEY:
		case MDHIM_BYTE_KEY:
			if (i > 0) free(key_string);
			key_string = random_string(key_len, rand_str_size);
			if (verbose) tst_say(0, "# mdhimPut( %s, %s ) [string|byte]\n", 
					     key_string, value );
			brm = mdhimPut(md, (void *)key_string, 
				       strlen(key_string), 
				       value, strlen(value)+1, 
				       NULL, NULL);
			break;

		default:
			tst_say(1, "Error, unrecognized Key_type in execNput\n");
		}
        
		// Record any error(s)
		if (!brm || brm->error)
		{
			if (verbose) tst_say(1, "ERROR: rank %d N putting key: %s with value: %s "
					     "into MDHIM\n", md->mdhim_rank, key_string, value);
			ret ++;
		}
  
	}

	// Report any error(s)
	if (ret)
	{
		tst_say(1, "ERROR: rank %d - %d error(s) N putting key/value into MDHIM\n", 
			md->mdhim_rank, ret);
	}
	else
	{
		tst_say(0, "Successfully N put %d key/values into MDHIM\n", n_iter);
	}
    
	//Commit the database
	ret = mdhimCommit(md, md->primary_index);
	if (ret != MDHIM_SUCCESS)
	{
		tst_say(1, "ERROR: rank %d committing N put key/value(s) to MDHIM database\n",
			md->mdhim_rank);
	}
	else
	{
		if (verbose) tst_say(0, "Committed N put to MDHIM database\n");
	}

}

//======================================NGETN============================
static void execNgetn(char *command, struct mdhim_t *md, int charIdx)
{
	int i_key;
	long l_key;
	float f_key;
	double d_key;
	struct mdhim_bgetrm_t *bgrm;
	char buffer [ TEST_BUFLEN ];
	char *key_string;
	int n_iter, key_len, rand_str_size;
	int ret, i;
    
	bgrm = NULL;
	key_string = malloc(sizeof(char) * TEST_BUFLEN);
	ret = 0;
    
	if (verbose) tst_say(0, "# ngetn n key_length exact_size\n" );
    
	charIdx = getWordFromString( command, buffer, charIdx);
	n_iter = atoi( buffer ); // Get number of iterations
    
	charIdx = getWordFromString( command, buffer, charIdx);
	key_len = atoi( buffer ); // For string/byte key types otherwise ignored (but required)
    
	charIdx = getWordFromString( command, buffer, charIdx);
	// If zero strings are of the exact length stated above, otherwise they are
	// of variable length up to key_len.
	rand_str_size = atoi( buffer );
    
	// Based on key type generate a random first key or use zero if key_len = 0
	switch (key_type)
	{
        case MDHIM_INT_KEY:
		if ( key_len == 0)
			i_key = 0;
		else
			i_key = rand() / 5;
		sprintf(key_string, "%d", i_key);
		if (verbose) tst_say(0, "# mdhimGet( %s ) [int]\n", key_string );
		bgrm = mdhimGet(md, md->primary_index, &i_key, sizeof(i_key), MDHIM_GET_NEXT);
		break;

        case MDHIM_LONG_INT_KEY:
		if ( key_len == 0)
			l_key = 0;
		else
			l_key = rand() / 3;
		sprintf(key_string, "%ld", l_key);
		if (verbose) tst_say(0, "# mdhimGet( %s ) [long]\n", key_string );
		bgrm = mdhimGet(md, md->primary_index, &l_key, sizeof(l_key), MDHIM_GET_NEXT);
		break;

        case MDHIM_FLOAT_KEY:
		if ( key_len == 0)
			f_key = 0.0;
		else
			f_key = rand() / 5.0;
		sprintf(key_string, "%f", f_key);
		if (verbose) tst_say(0, "# mdhimGet( %s ) [float]\n", key_string );
		bgrm = mdhimGet(md, md->primary_index, &f_key, sizeof(f_key), MDHIM_GET_NEXT);
		break;

	case MDHIM_DOUBLE_KEY:
		if ( key_len == 0)
			d_key = 0.0;
		else
			d_key = rand() / 3.0;
		sprintf(key_string, "%e", d_key);
		if (verbose) tst_say(0, "# mdhimGet( %s ) [double]\n", key_string );
		bgrm = mdhimGet(md, md->primary_index, &d_key, sizeof(d_key), MDHIM_GET_NEXT);
		break;

        case MDHIM_STRING_KEY:
        case MDHIM_BYTE_KEY:
		if ( key_len == 0 )
		{
			key_string[0] = '0';
			key_string[1] = '\0';
		}
		else
			key_string = random_string(key_len, rand_str_size);
		if (verbose) tst_say(0, "# mdhimGet( %s ) [string|byte]\n", key_string );
		bgrm = mdhimGet(md, md->primary_index, (void *)key_string, strlen(key_string), 
				MDHIM_GET_NEXT);
		break;

        default:
		tst_say(1, "Error, unrecognized Key_type in execNgetn\n");
	}

	// Record any error(s)
	if (!bgrm || bgrm->error)
	{
		// For some reason could not get first record abort the request
		tst_say(1, "ERROR: rank %d N getting FIRST key: %s from MDHIM\n", 
			md->mdhim_rank, key_string);
		ret++;
	}
	else if (bgrm->keys[0] && bgrm->values[0])
	{
		if (verbose) tst_say(0, "Successfully got FIRST value: %s for key "
				     "[string|byte](%s) from MDHIM\n", 
				     expand_escapes(bgrm->values[0], bgrm->value_lens[0]), 
				     getValLabel(MDHIM_GET_NEXT));
        
		for (i=1; i<n_iter; i++)
		{
			bgrm = mdhimGet(md, md->primary_index, bgrm->keys[0], bgrm->key_lens[0], 
				       MDHIM_GET_NEXT);
			// Record any error(s)
			if (!bgrm || bgrm->error)
			{
				tst_say(1, "ERROR: rank %d N getting key[%d] from MDHIM. Abort request.\n", 
					md->mdhim_rank, i);
				ret ++;
				break;
			}
			else if (bgrm->keys[0] && bgrm->values[0])
			{
				if (verbose) tst_say(0, "Successfully got %dth value: %s for key "
						     "[string|byte](%s) from MDHIM\n", i,
						     expand_escapes(bgrm->values[0], bgrm->value_lens[0]),
						     getValLabel(MDHIM_GET_NEXT));
			}
			else
			{
				tst_say(1, "ERROR: rank %d got null value or key at N get key[%d] "
					"from MDHIM. Abort request.\n", md->mdhim_rank, i);
				ret ++;
				break;
			}
		}
	}
	else
	{
		tst_say(1, "ERROR: rank %d got null value or return  key for FIRST key (%s): %s from MDHIM\n", 
			md->mdhim_rank, getValLabel(MDHIM_GET_NEXT), key_string);
		ret++;
	}

	// Report any error(s)
	if (ret)
	{
		tst_say(1, "ERROR: rank %d got %d error(s) N getting key/value from MDHIM\n", 
			md->mdhim_rank, ret);
	}
	else
	{
		tst_say(0, "Successfully N got %d out of %d key/values desired from MDHIM\n", 
			i, n_iter);
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
	char     *db_path = "./";
	char     *db_name = "mdhimTst-";
	int      dowork = 1;
	int      dbug = 1; //MLOG_CRIT=1, MLOG_DBG=2
	int      factor = 1; //Range server factor
	int      slice = 100000; //Range server slice size

	struct timeval begin, end;
	long double   time_spent;
    
	mdhim_options_t *db_opts; // Local variable for db create options to be passed
    
	int ret;
	int provided = 0;
	struct mdhim_t *md;
    
	int db_type = LEVELDB; //(data_store.h) 

	// Process arguments
	infile = stdin;
	while ((argc > 1) && (argv[1][0] == '-'))
	{
		switch (argv[1][1])
		{
		case 'f':
			printf("Input file: %s || ", &argv[1][2]);
			infile = fopen( &argv[1][2], "r" );
			if( !infile )
			{
				fprintf( stderr, "Failed to open %s, %s\n", 
					 &argv[1][2], strerror( errno ));
				exit( -1 );
			}
			break;

		case 'd': // DataBase type (1=levelDB)
			printf("Data Base type: %s || ", &argv[1][2]);
			db_type = atoi( &argv[1][2] );
			break;

		case 't':
			printf("Key type: %s || ", &argv[1][2]);
			key_type = atoi( &argv[1][2] );
			break;

		case 'b':
			printf("Debug mode: %s || ", &argv[1][2]);
			dbug = atoi( &argv[1][2] );
			break;
                
		case 'p':
			printf("DB Path: %s || ", &argv[1][2]);
			db_path = &argv[1][2];
			break;
                
		case 'n':
			printf("DB name: %s || ", &argv[1][2]);
			db_name = &argv[1][2];
			break;

		case 'c':
			printf("Range server factor: %s || ", &argv[1][2]);
			factor = atoi( &argv[1][2] );
			break;  

		case 's':
			printf("Range server slice size: %s || ", &argv[1][2]);
			slice = atoi( &argv[1][2] );
			break; 
 
		case 'a':
			printf("DB option append value is on || ");
			dbOptionAppend = MDHIM_DB_APPEND;
			break;

		case 'q':
			to_log = atoi( &argv[1][2] );
			if (!to_log) 
			{
				printf("Quiet mode || ");
				verbose = 0;
			}
			else
			{
				printf("Quiet to_log file mode || ");
			}
			break;
                
		default:
			printf("Wrong Argument (it will be ignored): %s\n", argv[1]);
			usage();
		}

		++argv;
		--argc;
	}
	printf("\n");
    
	// Set the debug flag to the appropriate Mlog mask
	switch (dbug)
	{
        case 2:
		dbug = MLOG_DBG;
		break;
            
        default:
		dbug = MLOG_CRIT;
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
	db_opts = mdhim_options_init();
	mdhim_options_set_db_path(db_opts, db_path);
	mdhim_options_set_db_name(db_opts, db_name);
	mdhim_options_set_db_type(db_opts, db_type);
	mdhim_options_set_key_type(db_opts, key_type);
	mdhim_options_set_debug_level(db_opts, dbug);
	mdhim_options_set_server_factor(db_opts, factor);
	mdhim_options_set_max_recs_per_slice(db_opts, slice);
	mdhim_options_set_value_append(db_opts, dbOptionAppend);  // Default is overwrite
    
	md = mdhimInit(MPI_COMM_WORLD, db_opts);
	if (!md)
	{
		printf("Error initializing MDHIM\n");
		exit(1);
	}
    
	/* initialization for random string generation */
	srand( time( NULL ) + md->mdhim_rank);
	sc_len = strlen( sourceChars );
    
	/*
	 * open the log file (one per rank if in verbose mode, otherwise write to stderr)
	 */
	if (verbose)
	{
		sprintf(filename, "./%s%d.log", db_name, md->mdhim_rank);
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
    
	// Read all command(s) to execute
	while( dowork && cmdIdx < 1000)
	{
		// read the next command
		memset( commands[cmdIdx], 0, sizeof( command ));
		errno = 0;
		getLine( commands[cmdIdx]);
        
		if (verbose) tst_say(0, "\n##command %d: %s\n", cmdIdx, commands[cmdIdx]);
        
		// Is this the last/quit command?
		if( commands[cmdIdx][0] == 'q' || commands[cmdIdx][0] == 'Q' )
		{
			dowork = 0;
		}
		cmdIdx++;
	}
	cmdTot = cmdIdx -1;

	// Main command execute loop
	for(cmdIdx=0; cmdIdx < cmdTot; cmdIdx++)
	{
		memset( command, 0, sizeof( command ));
		errno = 0;
        
		charIdx = getWordFromString( commands[cmdIdx], command, 0);

		if (verbose) tst_say(0, "\n##exec command: %s\n", command );
		gettimeofday(&begin, NULL);
        
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
		else if( !strcmp( command, "flush" ))
		{
			execFlush(commands[cmdIdx], md, charIdx);
		}
		else if( !strcmp( command, "nput" ))
		{
			execNput(commands[cmdIdx], md, charIdx);
		}
		else if( !strcmp( command, "ngetn" ))
		{
			execNgetn(commands[cmdIdx], md, charIdx);
		}
		else if( !strcmp( command, "bgetop" ))
		{
			execBgetOp(commands[cmdIdx], md, charIdx);
		}
		else
		{
			printf( "# q       FOR QUIT\n" );
			//printf( "# open filename keyfile1,dkeyfile2,... update\n" );
			//printf( "# close\n" );
			printf( "# flush\n" );
			printf( "# put key value\n" );
			printf( "# bput n key value\n" );
			printf( "# nput n key_size value_size exact_size #(0=variable | 1=exact)\n");
			printf( "# get key getOp #(EQ=0 | NEXT=1 | PREV=2 | FIRST=3 | LAST=4)\n" );
			printf( "# bget n key\n" );
			printf( "# bgetop n key getOp #(NEXT=1 | FIRST=3)\n" );
			printf( "# ngetn n key_length exact_size #(0=variable | 1=exact)\n" );
			printf( "# del key\n" );
			printf( "# bdel n key\n" );

		}
        
		gettimeofday(&end, NULL);
		time_spent = (long double) (end.tv_sec - begin.tv_sec) + 
			((long double) (end.tv_usec - begin.tv_usec)/1000000.0);
		tst_say(0, "Seconds to %s : %Lf\n\n", commands[cmdIdx], time_spent);
	}
    
	if (errMsgIdx)
	{
		int i, errsInCmds = errMsgIdx; // Only list the errors up to now
		for (i=0; i<MAX_ERR_REPORT && i<errsInCmds; i++)
			tst_say(1, "==%s", errMsgs[i]);
		tst_say(1, "==TOTAL ERRORS for rank: %d => %d (first %d shown)\n", 
			md->mdhim_rank, errsInCmds, i);
	}
	else
	{
		tst_say(0, "\n==No errors for rank: %d\n", md->mdhim_rank);
	}
    
	// Calls to finalize mdhim session and close MPI-communication
	ret = mdhimClose(md);
	if (ret != MDHIM_SUCCESS)
	{
		tst_say(1, "Error closing MDHIM\n");
	}    
	fclose(logfile);
    
	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();

	return( 0 );
}
