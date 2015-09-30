void mdhimftinit(int *appComm);
void mdhimftopts_init();
void mdhimftclose();
void mdhimftput(void *key, int *key_size, void *val, int *val_size);
void mdhimftoptions_dbpath (char *path);
void mdhimftoptions_dbname (char *name);
void mdhimftoptions_dbtype (int type);
void mdhimftoptions_server_factor (int factor);
void mdhimftoptions_max_recs_per_slice (int recs);
void mdhimftoptions_debug_level (int level);

