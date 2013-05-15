#include <stdio.h>
#include <stddef.h>

#define N 6

void dfs_tdecrease (size_t frame_id);
void dfs_tend ();
size_t dfs_tcreate (size_t sc, size_t size, void *wfn);
void dfs_twrite (size_t id, size_t offset, size_t value);
void *dfs_tload (void);

static inline int generate_write_index (int i)
{
  return i%3;
}

static inline int generate_read_index (int i)
{
  return i%100;
}

