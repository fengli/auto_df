#include <stdio.h>
#include "main.h"

int A[N];

int read_write_array ()
{
  int i;
  int tmp;

  for (i = 0; i < N; i++)
    {
      tmp = A[generate_read_index (i)];
      A[generate_write_index (i)] = tmp;
    }

  return A[N/2];
}

void init_array ()
{
  int i;

  for (i = 0; i < N; i++)
    A[i] = i;
}

int main (int argc, char **argv)
{
  int result;

  init_array ();
  result = read_write_array ();
  printf ("result:%d\n", result);
}

