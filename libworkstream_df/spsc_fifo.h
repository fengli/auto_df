#ifndef SPSC_FIFO_H
#define SPSC_FIFO_H

#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>

#include "wstream_df.h"

/* #define _DEBUG_THIS_ */

#ifdef _DEBUG_THIS_
#define XLOG(...) printf(__VA_ARGS__)
#else
#define XLOG(...)  /**/
#endif
#define XERR(...) printf(__VA_ARGS__)
#define XSUM(...) printf(__VA_ARGS__)

typedef struct view
{
  size_t id; // For now use a size_t as the runtime stores a tainted pointer here
  size_t data_offset;
  size_t view_offset;
  size_t burst;
  size_t horizon;

} view_t, *view_p;

typedef struct spsc_buffer{
  size_t log_size;
  size_t size;
  size_t modulo_mask;
  view_t *array;
}spsc_buffer_t, *spsc_buffer_p;

static inline spsc_buffer_p
spsc_buffer_alloc (size_t log_size)
{
  spsc_buffer_p spsc_buffer;
  if (posix_memalign ((void **)&spsc_buffer, 64, sizeof (spsc_buffer_t)))
    wstream_df_fatal ("Out of memory ...");

  spsc_buffer->log_size = log_size;
  spsc_buffer->size = (1 << log_size);
  spsc_buffer->modulo_mask = spsc_buffer->size - 1;
  if (posix_memalign ((void **)&spsc_buffer->array, 64,
		       sizeof (view_t) * spsc_buffer->size))
    wstream_df_fatal ("Out of memory ...");

  return spsc_buffer;
}

static inline void
spsc_buffer_free (spsc_buffer_p spsc_buffer)
{
  if (spsc_buffer && spsc_buffer->array)
    free (spsc_buffer->array);
  if (spsc_buffer)
    free (spsc_buffer);
}

static inline size_t
spsc_buffer_size (spsc_buffer_p spsc_buffer)
{
  return spsc_buffer->size;
}

static inline view_p
spsc_buffer_get (spsc_buffer_p spsc_buffer, size_t pos)
{
  return &spsc_buffer->array[pos & spsc_buffer->modulo_mask];
}

static inline void
spsc_buffer_set (spsc_buffer_p spsc_buffer, size_t pos, view_p elem)
{
  spsc_buffer->array[pos & spsc_buffer->modulo_mask].id = elem->id;
  spsc_buffer->array[pos & spsc_buffer->modulo_mask].data_offset = elem->data_offset;
  spsc_buffer->array[pos & spsc_buffer->modulo_mask].view_offset = elem->view_offset;
  spsc_buffer->array[pos & spsc_buffer->modulo_mask].burst = elem->burst;
  spsc_buffer->array[pos & spsc_buffer->modulo_mask].horizon = elem->horizon;
}

static inline void
spsc_buffer_grow (spsc_buffer_p volatile *spsc_buffer, size_t read_idx, size_t write_idx)
{
  // FIXME: use realloc and only copy the end-block on success
  spsc_buffer_p new_spsc_buffer = spsc_buffer_alloc ((*spsc_buffer)->log_size + 1);
  spsc_buffer_p old_spsc_buffer = *spsc_buffer;

  size_t old_buffer_size = old_spsc_buffer->size;
  size_t old_read_idx_pos = read_idx & old_spsc_buffer->modulo_mask;
  size_t old_write_idx_pos = write_idx & old_spsc_buffer->modulo_mask;

  size_t new_read_idx_pos = read_idx & new_spsc_buffer->modulo_mask;
  size_t new_write_idx_pos = write_idx & new_spsc_buffer->modulo_mask;

  /* If no wrap-around for old buffer, new one can't have one if size
     is doubled.  */
  if (old_read_idx_pos < old_write_idx_pos)
    memcpy (&new_spsc_buffer->array[new_read_idx_pos],
	    &(*spsc_buffer)->array[old_read_idx_pos],
	    (old_write_idx_pos - old_read_idx_pos) * sizeof (view_t));

  /* If old buffer wraps around, then either new one wraps around at
     same place or it just doesn't.  */
  else
    {
      /* No wrap around in new buffer?  */
      int no_wrap_around = (new_read_idx_pos < new_write_idx_pos) ? 1 : 0;

      memcpy (&new_spsc_buffer->array[new_read_idx_pos],
	      &(*spsc_buffer)->array[old_read_idx_pos],
	      (old_buffer_size - old_read_idx_pos) * sizeof (view_t));
      memcpy (&new_spsc_buffer->array[no_wrap_around * old_buffer_size],
	      (*spsc_buffer)->array,
	      (old_write_idx_pos) * sizeof (view_t));
    }

  store_store_fence ();

  *spsc_buffer = new_spsc_buffer;

  store_store_fence ();

  spsc_buffer_free (old_spsc_buffer);
}

/*************************************************************************/
/*************************************************************************/



typedef struct spsc_fifo {
  size_t volatile write_idx __attribute__ ((aligned (64)));
  size_t volatile read_idx __attribute__ ((aligned (64)));
  spsc_buffer_p volatile spsc_buffer __attribute__ ((aligned (64)));
} spsc_fifo_t, *spsc_fifo_p;


static inline void
spsc_fifo_alloc (spsc_fifo_p fifo, size_t log_size)
{
  fifo->read_idx = 0;
  fifo->write_idx = 0;
  fifo->spsc_buffer = spsc_buffer_alloc (log_size);
}


/* Dealloc the FIFO.  */
static inline void
spsc_fifo_free (spsc_fifo_p fifo)
{
  spsc_buffer_free (fifo->spsc_buffer);
}

/* Push element ELEM to the FIFO. Increase the size if necessary.  */
static inline void
spsc_fifo_push (spsc_fifo_p fifo, view_p elem)
{
  if (fifo->write_idx >= fifo->read_idx + spsc_buffer_size (fifo->spsc_buffer))
    spsc_buffer_grow (&fifo->spsc_buffer, fifo->read_idx, fifo->write_idx);

  spsc_buffer_set (fifo->spsc_buffer, fifo->write_idx, elem);

  fifo->write_idx++;
}

/* Get one task from FIFO for execution.  */
static inline view_p
spsc_fifo_peek (spsc_fifo_p fifo)
{
  if (fifo->read_idx < fifo->write_idx)
    return spsc_buffer_get (fifo->spsc_buffer, fifo->read_idx);
  return NULL;
}

/* Steal one elem from deque FIFO. return NULL if confict happens.  */
static inline void
spsc_fifo_pop (spsc_fifo_p fifo)
{
  fifo->read_idx++;
}

typedef struct peek_chain
{
  size_t num_peekers;

  view_t peekers[];
} peek_chain_t, *peek_chain_p;

/* Traverse from the current view until the first non-peeking view and
   copy in a newly allocated buffer, which is returned.  */
static inline peek_chain_p
spsc_fifo_clone_and_pop_peek_chain (spsc_fifo_p fifo)
{
  size_t read_idx = fifo->read_idx;
  size_t write_idx = fifo->write_idx;
  size_t modulo_mask = fifo->spsc_buffer->modulo_mask;

  size_t low_pos = read_idx & modulo_mask;
  size_t high_pos;
  peek_chain_p result;
  size_t size;

  view_p v_first = spsc_fifo_peek (fifo);
  view_p v = v_first;

  if (!v || v->burst != 0)
    wstream_df_fatal ("Cloning a peek chain without peek operations.");

  while (read_idx + 1 < write_idx && v->burst == 0)
    {
      read_idx++;
      v = spsc_buffer_get (fifo->spsc_buffer, read_idx);
    }
  if (!v || v->burst == 0)
    wstream_df_fatal ("Cloning an incomplete peek chain.");

  /* Found the end of this chain.  */
  size = read_idx - fifo->read_idx;
  high_pos = (read_idx - 1) & modulo_mask;

  /* Allocate the return peek chain.  */
  if (posix_memalign ((void **)&result, 64,
		      sizeof (size_t) + (sizeof (view_t) * size)))
    wstream_df_fatal ("Out of memory ...");

  /* If there's no wrap-around in the buffer for the chain */
  if (low_pos <= high_pos)
    memcpy (result->peekers, v_first, size * sizeof (view_t));
  else
    {
      memcpy (result->peekers, v_first, (fifo->spsc_buffer->size - low_pos) * sizeof (view_t));
      memcpy (result->peekers + (fifo->spsc_buffer->size - low_pos), fifo->spsc_buffer->array, (high_pos + 1) * sizeof (view_t));
    }
  result->num_peekers = size;

  /* Pop those peeking views.  */
  fifo->read_idx = read_idx;

  return result;
}


/* Close a peeking chain, which is done by setting the burst of the
   last written view.  We require that the original burst be null and
   the new one be identical to its horizon.  */
static inline int
spsc_fifo_close_peek_chain (spsc_fifo_p fifo, size_t burst)
{
  view_p v;
  int res = 1;

  /* Basically, as long as a peek chain contains no 0-burst elements,
     they're invisible (if we close and there was a single element,
     they become simple "input" equivalents.  */
  if (fifo->write_idx == 0)
    wstream_df_fatal ("Attempting to close a peek chain on an empty FIFO.");
  else if (fifo->write_idx == 1)
    res = 0;
  else if ((spsc_buffer_get (fifo->spsc_buffer, fifo->write_idx - 2))->burst != 0)
    res = 0;

  if (burst == 0)
    {
      if (fifo->read_idx != fifo->write_idx)
	{
	  v = spsc_buffer_get (fifo->spsc_buffer, fifo->write_idx - 1);
	  v->burst = v->horizon;
	}
      return res;
    }

  if (fifo->read_idx == fifo->write_idx)
    wstream_df_fatal ("Attempting to close a peek chain on an empty buffer.");

  v = spsc_buffer_get (fifo->spsc_buffer, fifo->write_idx - 1);

  if (v->burst != 0 || v->horizon != burst)
    wstream_df_fatal ("Attempting to close a peek chain on a non-peeking view or with a burst/horizon mismatch.");

  v->burst = v->horizon;

  return res;
}
#endif
