#include <stdio.h>
#include <stddef.h>

typedef struct frame{
  struct frame *next;
  void (*work_fn) (void);
  int sc;

  size_t result;
}struct_frame;
  
void worker ()
{
  struct_frame *fp = dfs_tload ();
  printf ("Get value:%d.\n", fp->result);
  dfs_tend ();
}

int main (int argc, char **argv)
{
  struct_frame *fp = dfs_tcreate (1, sizeof (struct_frame), worker);
  dfs_twrite (fp, offsetof (struct_frame, result), 122);
  dfs_tdecrease (fp);
}

