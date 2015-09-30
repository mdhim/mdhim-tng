module mdhim
  use, intrinsic :: iso_c_binding

  use mpi

  implicit none

  interface
    subroutine mdhimftinit ( comm ) bind ( c )
      use iso_c_binding
      integer ( c_int ) :: comm
    end subroutine mdhimftinit
    subroutine mdhimftclose ( ) bind ( c )
      use iso_c_binding
    end subroutine mdhimftclose
    subroutine mdhimftput (key, key_size, val, val_size) bind ( c )
      use iso_c_binding
      character(kind=c_char), intent(in) :: key(*)
      integer (c_int), intent(in) :: key_size
      character(kind=c_char), intent(in) :: val(*)
      integer (c_int), intent(in) :: val_size
    end subroutine mdhimftput
    subroutine mdhimftget (key, key_size, val, val_size) bind ( c )
      use iso_c_binding
      character(kind=c_char), intent(in) :: key(*)
      integer (c_int), intent(in) :: key_size
      character(kind=c_char), intent(out) :: val(*)
      integer (c_int), intent(out) :: val_size
    end subroutine mdhimftget
    subroutine mdhimftopts_init () bind ( c )
      use iso_c_binding
    end subroutine mdhimftopts_init
    subroutine mdhimftoptions_dbpath (path) bind ( c )
      use iso_c_binding
      character(kind=c_char), intent(in) :: path(*)
    end subroutine mdhimftoptions_dbpath
    subroutine mdhimftoptions_dbname (name) bind ( c )
      use iso_c_binding
      character(kind=c_char), intent(in) :: name(*)
    end subroutine mdhimftoptions_dbname
    subroutine mdhimftoptions_dbtype (type) bind ( c )
      use iso_c_binding
      integer (c_int), intent(in) :: type 
    end subroutine mdhimftoptions_dbtype
    subroutine mdhimftoptions_server_factor (factor) bind ( c )
      use iso_c_binding
      integer (c_int), intent(in) :: factor
    end subroutine mdhimftoptions_server_factor
    subroutine mdhimftoptions_max_recs_per_slice (recs) bind ( c )
      use iso_c_binding
      integer (c_int), intent(in) :: recs
    end subroutine mdhimftoptions_max_recs_per_slice
    subroutine mdhimftoptions_debug_level (level) bind ( c )
      use iso_c_binding
      integer (c_int), intent(in) :: level
    end subroutine mdhimftoptions_debug_level
  end interface
end module
