program main

  use, intrinsic :: iso_c_binding

  use mpi
  use mdhim

  implicit none

  character ( len=100 ) key, val, path, dbname
  integer ( c_int ) key_size, val_size
  type (c_ptr) opts
  integer ( c_int ) comm
  integer ( kind = 4 ) ierr
  integer ( kind = 4 ) provided
  integer ( kind = 4 ) rank

  call MPI_Init_Thread ( MPI_THREAD_MULTIPLE, provided, ierr )

  path = '/panfs/pas12a/vol1/hng/'
  dbname = 'mdhimf90'
  comm = MPI_COMM_WORLD
  call mdhimftopts_init ( )
  call mdhimftoptions_dbname( dbname );
  call mdhimftoptions_dbpath( path );
  call mdhimftinit ( comm )
  call MPI_Comm_rank( comm, rank, ierr )   
  key = 'blahblah'
  key_size = 8
  val = 'foofoo'
  val_size = 6
  call mdhimftput (key, key_size, val, val_size)
  val = ''
  val_size = 0
  call mdhimftget (key, key_size, val, val_size)
  write ( *, '(a,a)' ) '  Got value: ', val
  call mdhimftclose ( )

  stop
end
