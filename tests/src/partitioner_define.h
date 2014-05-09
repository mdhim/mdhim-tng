//#define RANGE_SERVER_FACTOR 4 // NOW a global variable in partitioner.h
#define MDHIM_MAX_SLICES 2147483647
//32 bit unsigned integer
#define MDHIM_INT_KEY 1
#define MDHIM_LONG_INT_KEY 2
#define MDHIM_FLOAT_KEY 3
#define MDHIM_DOUBLE_KEY 4
#define MDHIM_STRING_KEY 5
//An arbitrary sized key
#define MDHIM_BYTE_KEY 6

//Maximum length of a key
#define MAX_KEY_LEN 262144

/* The exponent used for the algorithm that determines the range server

   This exponent, should cover the number of characters in our alphabet 
   if 2 is raised to that power. If the exponent is 6, then, 64 characters are covered 
*/
#define MDHIM_ALPHABET_EXPONENT 6  
