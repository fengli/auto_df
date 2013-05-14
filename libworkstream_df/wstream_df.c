#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>

#define _WSTREAM_DF_DEBUG 1

#if 0
# define __TAINT_(P) (((size_t)(P)) ^ (((size_t) 1 << 63) - 1))
#else
# define __TAINT_(P) (P)
#endif

#define _DEBUG_THIS1_

#ifdef _DEBUG_THIS1_
#define __XLOG(...) printf(__VA_ARGS__)
#else
#define __XLOG(...)  /**/
#endif
#define __XERR(...) printf(__VA_ARGS__)

//#define _PAPI_PROFILE
#ifdef _PAPI_PROFILE
#include <papi.h>
#endif

#define _PAPI_COUNTER_SETS 3

# define _PAPI_P0B
# define _PAPI_P0E
# define _PAPI_P1B
# define _PAPI_P1E
# define _PAPI_P2B
# define _PAPI_P2E
# define _PAPI_P3B
# define _PAPI_P3E

# define _PAPI_INIT_CTRS(I)
# define _PAPI_DUMP_CTRS(I)

#ifdef _PAPI_PROFILE
# define _PAPI_P0B start_papi_counters (0)
# define _PAPI_P0E stop_papi_counters (0)
# define _PAPI_P1B start_papi_counters (1)
# define _PAPI_P1E stop_papi_counters (1)
# define _PAPI_P2B start_papi_counters (2)
# define _PAPI_P2E stop_papi_counters (2)
//# define _PAPI_P3B start_papi_counters (3)
//# define _PAPI_P3E stop_papi_counters (3)

# define _PAPI_INIT_CTRS(I)			\
  {						\
    int _ii;					\
    for (_ii = 0; _ii < I; ++ _ii)		\
      init_papi_counters (_ii);			\
  }

# define _PAPI_DUMP_CTRS(I)			\
  {						\
    int _ii;					\
    for (_ii = 0; _ii < I; ++ _ii)		\
      dump_papi_counters (_ii);			\
  }
#endif

#include "wstream_df.h"
#include "error.h"
#include "cdeque.h"
#include "spsc_fifo.h"

//#define _PRINT_STATS

#ifdef _PAPI_PROFILE
#define _papi_num_events 4
int _papi_tracked_events[_papi_num_events] =
  {PAPI_TOT_CYC, PAPI_L1_DCM, PAPI_L2_DCM, PAPI_L3_TCM};
//  {PAPI_TOT_CYC, PAPI_L1_DCM, PAPI_L2_DCM, PAPI_TLB_DM, PAPI_PRF_DM, PAPI_MEM_SCY};
#endif

/***************************************************************************/
/* Data structures for T*.  */
/***************************************************************************/

typedef struct wstream_df_frame
{
  void (*work_fn) (void);
  int synchronization_counter;

  /* Variable size struct */
  char buf [];
} wstream_df_frame_t, *wstream_df_frame_p;


typedef struct wstream_df_view
{
  /* MUST always be 1st field.  */
  size_t data_offset;
  /* MUST always be 2nd field.  */
  void *peek_chain;
  size_t burst;
  size_t owner;
} wstream_df_view_t, *wstream_df_view_p;

/* The stream data structure.  It only relies on two linked lists of
   views in the case of a sequential control program (or at least
   sequential with respect to production and consumption of data in
   any given stream), or two single-producer single-consumer queues if
   the production and consumption of data is scheduled by independent
   control program threads.  */
typedef struct wstream_df_stream
{
  spsc_fifo_t producer_queue __attribute__((aligned (64)));
  spsc_fifo_t consumer_queue __attribute__((aligned (64)));

  bool has_active_peek_chain;
  size_t num_completed_peek_chains;
  peek_chain_p carry_over_peek_chain;
  size_t reached_position;

  int refcount;
  size_t element_size;
} wstream_df_stream_t, *wstream_df_stream_p;

/* T* worker threads have each their own private work queue, which
   contains the frames that are ready to be executed.  For now the
   control program will be distributing work round-robin, later we
   will add work-sharing to make this more load-balanced.  A
   single-producer single-consumer concurrent dequeue could be used in
   this preliminary stage, which would require no atomic operations,
   but we'll directly use a multi-producer single-consumer queue
   instead.  */
typedef struct __attribute__ ((aligned (64))) wstream_df_thread
{
  pthread_t posix_thread_id;

  cdeque_t work_deque __attribute__((aligned (64)));
  size_t volatile created_frames __attribute__((aligned (64)));
  size_t volatile executed_frames __attribute__((aligned (64)));
  bool volatile terminate_p __attribute__((aligned (64)));

  int worker_id;
  wstream_df_frame_p own_next_cached_thread;

#ifdef _PAPI_PROFILE
  int _papi_eset[16];
  long long counters[16][_papi_num_events];
#endif
} wstream_df_thread_t, *wstream_df_thread_p;


/* The current frame pointer is stored here in TLS */
static __thread wstream_df_frame_p current_fp;
static int num_workers;
static wstream_df_thread_p wstream_df_worker_threads;
static __thread wstream_df_thread_p current_thread = NULL;

/***************************************************************************/
/***************************************************************************/

#ifdef _PAPI_PROFILE
void
dump_papi_counters (int eset_id)
{
  long long counters[_papi_num_events];
  char event_name[PAPI_MAX_STR_LEN];
  int i, pos = 0;
  int max_length = _papi_num_events * (PAPI_MAX_STR_LEN + 30);
  char out_buf[max_length];

  //PAPI_read (current_thread->_papi_eset[eset_id], counters);
  pos += sprintf (out_buf, "Thread %d (eset %d):", current_thread->worker_id, eset_id);
  for (i = 0; i < _papi_num_events; ++i)
    {
      PAPI_event_code_to_name (_papi_tracked_events[i], event_name);
      pos += sprintf (out_buf + pos, "\t %s %15lld", event_name, current_thread->counters[eset_id][i]);
    }
  printf ("%s\n", out_buf); fflush (stdout);
}

void
init_papi_counters (int eset_id)
{
  int retval, j;

  current_thread->_papi_eset[eset_id] = PAPI_NULL;
  if ((retval = PAPI_create_eventset (&current_thread->_papi_eset[eset_id])) != PAPI_OK)
    wstream_df_fatal ("Cannot create PAPI event set (%s)", PAPI_strerror (retval));

  if ((retval = PAPI_add_events (current_thread->_papi_eset[eset_id], _papi_tracked_events, _papi_num_events)) != PAPI_OK)
    wstream_df_fatal ("Cannot add events to set (%s)", PAPI_strerror (retval));

  for (j = 0; j < _papi_num_events; ++j)
    current_thread->counters[eset_id][j] = 0;
}


void
start_papi_counters (int eset_id)
{
  int retval;
  if ((retval = PAPI_start (current_thread->_papi_eset[eset_id])) != PAPI_OK)
    wstream_df_fatal ("Cannot sart PAPI counters (%s)", PAPI_strerror (retval));
}

void
stop_papi_counters (int eset_id)
{
  int retval, i;
  long long counters[_papi_num_events];

  if ((retval = PAPI_stop (current_thread->_papi_eset[eset_id], counters)) != PAPI_OK)
    wstream_df_fatal ("Cannot stop PAPI counters (%s)", PAPI_strerror (retval));

  for (i = 0; i < _papi_num_events; ++i)
    current_thread->counters[eset_id][i] += counters[i];
}

void
accum_papi_counters (int eset_id)
{
  int retval;

  if ((retval = PAPI_accum (current_thread->_papi_eset[eset_id], current_thread->counters[eset_id])) != PAPI_OK)
    wstream_df_fatal ("Cannot start PAPI counters (%s)", PAPI_strerror (retval));
}
#endif

/***************************************************************************/
/***************************************************************************/
/***             TSTAR instructions (runtime implementation)             ***/
/***************************************************************************/
/***************************************************************************/

/* Load the current frame to a local buffer and return a pointer to
   the buffer.  Stack allocation is assumed so deallocation is not
   required.  */
void *
dfs_tload (void)
{
  __XLOG ("+++dfs_load: %p(ret)\n", current_fp);
  /* Runtime compatibility implementation.  */
  return (void *) current_fp;
}


/* Store the contents of BUFFER, up to SIZE bytes, to the frame FP, at
   OFFSET.  */
void
dfs_tstore (size_t id, size_t offset, void *buffer, size_t size)
{
  __XLOG ("+++dfs_tstore: id:%lx, offset:%lx, buffer:%p, size:%lx",id,offset,buffer,size);
  /* Runtime compatibility implementation.  The frame pointer should
     normally be an ID, so the offset is "really" necessary, while
     here it is obtained by subtracting the frame pointer from the
     original pointer, then adding them back up together.  */
  memcpy ((void *) __TAINT_ (id) + offset, buffer, size);
}

void
dfs_twrite (size_t id, size_t offset, size_t value)
{
  __XLOG("+++dfs_twrite: id:%lx, offset:%lx, value:%lx\n",id,offset,value);
  *((size_t *) (__TAINT_ (id) + offset)) = value;
}

void
dfs_twrite_32 (size_t id, size_t offset, size_t value)
{
  __XLOG("+++dfs_twrite_32: id:%lx, offset:%lx, value:%lx\n",id,offset,value);  
  *((int *) (__TAINT_ (id) + offset)) = (int) value;
}

void
dfs_twrite_16 (size_t id, size_t offset, size_t value)
{
  __XLOG("+++dfs_twrite_16: id:%lx, offset:%lx, value:%lx\n",id,offset,value);  
  *((short *) (__TAINT_ (id) + offset)) = (short) value;
}

void
dfs_twrite_8 (size_t id, size_t offset, size_t value)
{
  __XLOG("+++dfs_twrite_8: id:%lx, offset:%lx, value:%lx\n",id,offset,value);  
  *((char *) (__TAINT_ (id) + offset)) = (char) value;
}


/* Create a new thread, with frame pointer size, and sync counter */
size_t
dfs_tcreate (size_t sc, size_t size, void *wfn)
{
  wstream_df_frame_p frame_pointer;

  current_thread->created_frames++;
  __compiler_fence;

  if (posix_memalign ((void **)&frame_pointer, 64, size))
    wstream_df_fatal ("Out of memory ...");
  frame_pointer->synchronization_counter = sc;
  frame_pointer->work_fn = (void (*) (void)) wfn;

  __XLOG("+++dfs_tcreate: ret:%lx, sc:%lx, size:%lx, wfn:%p\n",__TAINT_(frame_pointer),sc,size,wfn);  
  return (size_t) __TAINT_ (frame_pointer);
}


/* Decrease the synchronization counter by N.  */
static inline void
tdecrease_n (void *data, size_t n)
{
  wstream_df_frame_p fp = (wstream_df_frame_p) data;
  int sc = 0;

  if (fp->synchronization_counter != (int) n)
    sc = __sync_sub_and_fetch (&(fp->synchronization_counter), (int) n);
  /* else the atomic sub would return 0.  This relies on the fact that
     the synchronization_counter is strictly decreasing.  */

  /* Schedule the thread if its synchronization counter reaches 0.  */
  if (sc == 0)
    {
      if (current_thread->own_next_cached_thread != NULL)
	cdeque_push_bottom (&current_thread->work_deque,
			    current_thread->own_next_cached_thread);
      current_thread->own_next_cached_thread = fp;
    }
  if (sc < 0)
    wstream_df_fatal ("Sub-zero synchronization counter");
}

/* Decrease the synchronization counter by one.  This is not used in
   the current code generation.  Kept for compatibility with the T*
   ISA.  */
void dfs_tdecrease (size_t frame_id)
{
  __XLOG ("+++dfs_tdecrease: frame_id:%lx\n", frame_id);
  tdecrease_n ((void *) __TAINT_ (frame_id), 1);
}

/* Decrease the synchronization counter by N.  */
void dfs_tdecrease_n (size_t frame_id, size_t n)
{
  __XLOG ("+++dfs_tdecrease_n: frame_id:%lx, n:%lx\n", frame_id, n);  
  tdecrease_n ((void *) __TAINT_ (frame_id), n);
}

/* Destroy the current thread */
extern void
dfs_tend ()
{
  __XLOG ("+++dfs_tend\n");
  /* The task is ended, therefore we can free it.*/
  free (current_fp);
  current_fp = NULL;
}


/***************************************************************************/
/***************************************************************************/
/***                    WorkStream runtime support                       ***/
/***************************************************************************/
/***************************************************************************/

static inline void
save_peek_chain (wstream_df_stream_p stream, peek_chain_p chain, size_t burst)
{
  size_t i;

  if (posix_memalign ((void **)&stream->carry_over_peek_chain, 64,
		      sizeof (size_t) + (sizeof (view_t) * chain->num_peekers)))
    wstream_df_fatal ("Out of memory ...");

  memcpy (stream->carry_over_peek_chain, chain,
	  sizeof (size_t) + (sizeof (view_t) * chain->num_peekers));

  for (i = 0; i < chain->num_peekers; ++i)
    stream->carry_over_peek_chain->peekers[i].data_offset += burst;
}


static inline void
match_stream_queues (wstream_df_stream_p stream)
{
  spsc_fifo_p prod_queue = &stream->producer_queue;
  spsc_fifo_p cons_queue = &stream->consumer_queue;
  view_p p_elem, c_elem;

  while ((p_elem = spsc_fifo_peek (prod_queue)) != NULL
	 && (c_elem = spsc_fifo_peek (cons_queue)) != NULL)
    {
      peek_chain_p chain = NULL;

      /* First handle peek chains.  */
      if (c_elem->burst == 0)
	{
	  /* If the peek chain is not complete, we can't process it.  */
	  if (stream->num_completed_peek_chains == 0)
	    return;

	  /* Remove one peek chain from the stream.  */
	  stream->num_completed_peek_chains--;
	  chain = spsc_fifo_clone_and_pop_peek_chain (cons_queue);
	  if ((c_elem = spsc_fifo_peek (cons_queue)) == NULL)
	    wstream_df_fatal ("Peek chain completion error.");

	  /* If the matching producer cannot satisfy the consumer(s),
	     then we need to keep a  */
	  if (c_elem->horizon > p_elem->burst)
	    save_peek_chain (stream, chain, p_elem->burst);
	}
      else
	{
	  /* If there is a partially satisfied peek chain.  */
	  if (stream->carry_over_peek_chain != NULL)
	    {
	      chain = stream->carry_over_peek_chain;

	      /* If the peek chain is still un-satisfied.  */
	      if (c_elem->horizon > stream->reached_position + p_elem->burst)
		save_peek_chain (stream, chain, p_elem->burst);
	    }
	}

      /* Write the pointer to the peek chain in the producer's view.  */
      dfs_twrite (p_elem->id,
		 p_elem->view_offset + offsetof (wstream_df_view_t, peek_chain),
		 (size_t) chain);


      /* Write the consumer's ID in the producer's view.  */
      dfs_twrite (p_elem->id,
		 p_elem->view_offset + offsetof (wstream_df_view_t, owner),
		 c_elem->id);

      /* Write the consumer's data offset (with the reached position
	 shift) in the producer's view.  */
      dfs_twrite (p_elem->id,
		 p_elem->view_offset + offsetof (wstream_df_view_t, data_offset),
		 c_elem->data_offset + stream->reached_position);

      /* TDEC the producer by 1 to notify it that its
	 consumers for that view have arrived.  */
      dfs_tdecrease (p_elem->id);

      /* The producer is necessarily consumed in the operation (when
	 it completes).  */
      spsc_fifo_pop (prod_queue);
      stream->reached_position += p_elem->burst;
      if (c_elem->horizon <= stream->reached_position)
	{
	  spsc_fifo_pop (cons_queue);
	  stream->reached_position = 0;
	  stream->carry_over_peek_chain = NULL;
	}
    }
}


/*
   This solver assumes that for any given stream:

   (1) all producers have the same burst == horizon
   (2) all consumers have the same horizon and
       (burst == horizon) || (burst == 0)
   (3) the consumers' horizon is an integer multiple
       of the producers' burst
*/

void
wstream_df_resolve_dependences (size_t frame_id, size_t data_offset, size_t view_offset,
			   size_t burst, size_t horizon,
			   void *stream_ptr, bool is_read_view_p)
{
  wstream_df_stream_p stream = (wstream_df_stream_p) stream_ptr;
  spsc_fifo_p prod_queue = &stream->producer_queue;
  spsc_fifo_p cons_queue = &stream->consumer_queue;
  view_t s_elem;

  s_elem.id = frame_id;
  s_elem.data_offset = data_offset;
  s_elem.view_offset = view_offset;
  s_elem.burst = burst;
  s_elem.horizon = horizon;

  if (is_read_view_p == true)
    {
      spsc_fifo_push (cons_queue, &s_elem);

      if (burst == 0)
	{
	  /* Peek operation */
	  stream->has_active_peek_chain = true;
	}
      else
	{
	  /* If there was an on-going peek chain on this stream, the
	     current non-0-burst read view closes it.  */
	  if (stream->has_active_peek_chain == true)
	    {
	      stream->num_completed_peek_chains++;
	      stream->has_active_peek_chain = false;
	    }

	  /* FIXME: for now, as the fifo is only SC, we can't have
	     both producer and consumer generators do the matching, so
	     we restrict it to consumers (with a potential for a lack
	     of reactivity.  The stream destructor (which is
	     guaranteed to be exclusive) does the last pass to clear
	     up any stragglers.  */
	}
      match_stream_queues (stream);
    }
  else
    {
      if (burst == 0)
	wstream_df_fatal ("0-burst producers are not allowed.");
      else
	spsc_fifo_push (prod_queue, &s_elem);
    }
}

/***************************************************************************/
/* Threads and scheduling.  */
/***************************************************************************/

/* Count the number of cores this process has.  */
static int
wstream_df_num_cores ()
{
  int i, cnt=0;
  cpu_set_t cs;
  CPU_ZERO (&cs);
  sched_getaffinity (getpid (), sizeof (cs), &cs);

  for (i = 0; i < MAX_NUM_CORES; i++)
    if (CPU_ISSET (i, &cs))
      cnt++;

  return cnt;
}

void *
wstream_df_worker_thread_fn (void *data)
{
  cdeque_p sched_deque;
  wstream_df_thread_p cthread = (wstream_df_thread_p) data;
  const unsigned int wid = cthread->worker_id;
  unsigned int rands = 77777 + wid * 19;
  unsigned int steal_from = 0;

  current_thread = cthread;

  sched_deque = &cthread->work_deque;

  if (wid != 0)
    {
      _PAPI_INIT_CTRS (_PAPI_COUNTER_SETS);
    }

  while (true)
    {
      bool termination_p = cthread->terminate_p;
      wstream_df_frame_p fp = cthread->own_next_cached_thread;
      __compiler_fence;

      if (fp == NULL)
	fp = (wstream_df_frame_p)  (cdeque_take (sched_deque));
      else
	cthread->own_next_cached_thread = NULL;

      if (fp == NULL)
	{
	  // Cheap alternative to nrand48
	  rands = rands * 1103515245 + 12345;
	  steal_from = rands % num_workers;
	  if (__builtin_expect (steal_from != wid, 1))
	    fp = cdeque_steal (&wstream_df_worker_threads[steal_from].work_deque);
	}

      if (fp != NULL)
	{
	  current_fp = fp;

	  _PAPI_P3B;
	  fp->work_fn ();
	  _PAPI_P3E;

	  __compiler_fence;
	  cthread->executed_frames++;
	}
      else if (!termination_p)
	{
	  sched_yield ();
	}
      else
	{
	  /* Try to detect whether all created frames have been
	     executed.  As terminate_p is set on this thread, it means
	     that the control program has finished creating frames and
	     this read is up to date.  */
	  long long missing = 0;
	  int i;

	  //missing = created_frames - executed_frames;
	  for (i = 0; i < num_workers; ++i)
	    {
	      missing -= wstream_df_worker_threads[i].executed_frames;
	    }
	  __compiler_fence;
	  for (i = 0; i < num_workers; ++i)
	    {
	      missing += wstream_df_worker_threads[i].created_frames;
	    }

	  if (missing == 0)
	    {
	      if (wid != 0)
		{
		  _PAPI_DUMP_CTRS (_PAPI_COUNTER_SETS);
		}

	      return NULL;
	    }
	}
    }
}

static void
start_worker (wstream_df_thread_p wstream_df_worker, int ncores)
{
  pthread_attr_t thread_attr;
  int i;

  int id = wstream_df_worker->worker_id;
  int core = id % ncores;

#ifdef _PRINT_STATS
  printf ("worker %d mapped to core %d\n", id, core);
#endif

  pthread_attr_init (&thread_attr);

  cpu_set_t cs;
  CPU_ZERO (&cs);
  CPU_SET (core, &cs);

  int errno = pthread_attr_setaffinity_np (&thread_attr, sizeof (cs), &cs);
  if (errno < 0)
    wstream_df_fatal ("pthread_attr_setaffinity_np error: %s\n", strerror (errno));

  pthread_create (&wstream_df_worker->posix_thread_id, &thread_attr,
		  wstream_df_worker_thread_fn, wstream_df_worker);

  CPU_ZERO (&cs);
  errno = pthread_getaffinity_np (wstream_df_worker->posix_thread_id, sizeof (cs), &cs);
  if (errno != 0)
    wstream_df_fatal ("pthread_getaffinity_np error: %s\n", strerror (errno));

  for (i = 0; i < CPU_SETSIZE; i++)
    if (CPU_ISSET (i, &cs) && i != core)
      wstream_df_error ("got affinity to core %d, expecting %d\n", i, core);

  if (!CPU_ISSET (core, &cs))
    wstream_df_error ("no affinity to core %d\n", core);

  pthread_attr_destroy (&thread_attr);
}

__attribute__((constructor))
void pre_main() {

  int i;

#ifdef _PAPI_PROFILE
  int retval = PAPI_library_init(PAPI_VER_CURRENT);
  if (retval != PAPI_VER_CURRENT)
    wstream_df_fatal ("Cannot initialize PAPI library");
#endif

  /* Set workers number as the number of cores.  */
#ifndef _WSTREAM_DF_NUM_THREADS
  num_workers = wstream_df_num_cores ();
#else
  num_workers = _WSTREAM_DF_NUM_THREADS;
#endif

  if (posix_memalign ((void **)&wstream_df_worker_threads, 64,
		       num_workers * sizeof (wstream_df_thread_t)))
    wstream_df_fatal ("Out of memory ...");

  int ncores = wstream_df_num_cores ();

#ifdef _PRINT_STATS
  printf ("Creating %d workers for %d cores\n", num_workers, ncores);
#endif

  for (i = 0; i < num_workers; ++i)
    {
      cdeque_init (&wstream_df_worker_threads[i].work_deque, WSTREAM_DF_DEQUE_LOG_SIZE);
      wstream_df_worker_threads[i].created_frames = 0;
      wstream_df_worker_threads[i].executed_frames = 0;
      wstream_df_worker_threads[i].terminate_p = false;
      wstream_df_worker_threads[i].worker_id = i;
      wstream_df_worker_threads[i].own_next_cached_thread = NULL;
    }

  /* Add a guard frame for the control program (in case threads catch
     up with the control program).  */
  current_thread = &wstream_df_worker_threads[0];

  _PAPI_INIT_CTRS (_PAPI_COUNTER_SETS);

  __compiler_fence;

  for (i = 1; i < num_workers; ++i)
    start_worker (&wstream_df_worker_threads[i], ncores);
}

__attribute__((destructor))
void post_main() {
  int i;
  void *ret;

  for (i = 0; i < num_workers; ++i)
    {
      wstream_df_worker_threads[i].terminate_p = true;
    }

  /* Also have this thread execute tasks once it's done.  This also
     serves in the case of 0 worker threads, to execute all on a
     single thread.  */
  wstream_df_worker_thread_fn (&wstream_df_worker_threads[0]);

  for (i = 1; i < num_workers; ++i)
    {
      pthread_join (wstream_df_worker_threads[i].posix_thread_id, &ret);
    }

  _PAPI_DUMP_CTRS (_PAPI_COUNTER_SETS);

#ifdef _PRINT_STATS
  for (i = 0; i < num_workers; ++i)
    {
      int executed_tasks = wstream_df_worker_threads[i].executed_frames;
      int worker_id = wstream_df_worker_threads[i].worker_id;
      printf ("worker %d executed %d tasks\n", worker_id, executed_tasks);
    }
#endif
}


/***************************************************************************/
/* Stream creation and destruction.  */
/***************************************************************************/

static inline void
init_stream (void *s, size_t element_size)
{
  spsc_fifo_alloc (&((wstream_df_stream_p) s)->producer_queue, 2);
  spsc_fifo_alloc (&((wstream_df_stream_p) s)->consumer_queue, 2);

  ((wstream_df_stream_p) s)->num_completed_peek_chains = 0;
  ((wstream_df_stream_p) s)->has_active_peek_chain = false;
  ((wstream_df_stream_p) s)->carry_over_peek_chain = NULL;
  ((wstream_df_stream_p) s)->reached_position = 0;
  ((wstream_df_stream_p) s)->refcount = 1;
  ((wstream_df_stream_p) s)->element_size = element_size;
}
/* Allocate and return an array of ARRAY_BYTE_SIZE/ELEMENT_SIZE
   streams.  */
void
wstream_df_stream_ctor (void **s, size_t element_size)
{
  if (posix_memalign (s, 64, sizeof (wstream_df_stream_t)))
    wstream_df_fatal ("Out of memory ...");
  init_stream (*s, element_size);
}

void
wstream_df_stream_array_ctor (void **s, size_t num_streams, size_t element_size)
{
  unsigned int i;

  for (i = 0; i < num_streams; ++i)
    {
      if (posix_memalign (&s[i], 64, sizeof (wstream_df_stream_t)))
	wstream_df_fatal ("Out of memory ...");
      init_stream (s[i], element_size);
    }
}

void wstream_df_tick (void *, size_t);
static inline void
force_empty_queues (void *s)
{
  wstream_df_stream_p stream = (wstream_df_stream_p) s;
  spsc_fifo_p prod_queue = &stream->producer_queue;
  spsc_fifo_p cons_queue = &stream->consumer_queue;
  view_p s_elem;

  /* Clean up any leftover matchings.  */
  if (stream->has_active_peek_chain == true)
    {
      stream->num_completed_peek_chains +=
	spsc_fifo_close_peek_chain (&stream->consumer_queue, 0);
      stream->has_active_peek_chain = false;
    }
  match_stream_queues (stream);

  if (spsc_fifo_peek (cons_queue) != NULL)
    wstream_df_fatal ("Leftover consumers at stream destruction -- bailing out.");

  while ((s_elem = spsc_fifo_peek (prod_queue)) != NULL)
    wstream_df_tick (stream, s_elem->burst);
}

static inline void
dec_stream_ref (void *s)
{
  wstream_df_stream_p stream = (wstream_df_stream_p) s;

#if 0
  int refcount = __sync_sub_and_fetch (&stream->refcount, 1);

  if (refcount == 0)
    {
      force_empty_queues (s);
      spsc_buffer_free (stream->consumer_queue.spsc_buffer);
      spsc_buffer_free (stream->producer_queue.spsc_buffer);

      free (s);
    }
#else
  int refcount = stream->refcount;

  // debug --
#ifdef _WSTREAM_DF_DEBUG
  if (refcount < 1)
    wstream_df_fatal ("Stream destructor called for a refcount < 1 (refcount == %d)", refcount);
#endif

  if (refcount > 1)
    refcount = __sync_sub_and_fetch (&stream->refcount, 1);
  else
    {
#ifdef _WSTREAM_DF_DEBUG
      refcount = __sync_sub_and_fetch (&stream->refcount, 1);

      if (refcount != 0)
	wstream_df_fatal ("Stream destructor called for a refcount < 1 (refcount == %d)", refcount);
#else
      refcount = 0;
#endif
    }

  if (refcount == 0)
    {
      force_empty_queues (s);
      spsc_buffer_free (stream->consumer_queue.spsc_buffer);
      spsc_buffer_free (stream->producer_queue.spsc_buffer);

      free (s);
    }
#endif
}

/* Deallocate the array of streams S.  */
void
wstream_df_stream_dtor (void **s, size_t num_streams)
{
  unsigned int i;

  if (num_streams == 1)
    {
      dec_stream_ref ((void *)s);
    }
  else
    {
      for (i = 0; i < num_streams; ++i)
	dec_stream_ref (s[i]);
    }
}

/* Take an additional reference on stream(s) S.  */
void
wstream_df_stream_reference (void *s, size_t num_streams)
{
  if (num_streams == 1)
    {
      wstream_df_stream_p stream = (wstream_df_stream_p) s;
      __sync_add_and_fetch (&stream->refcount, 1);
    }
  else
    {
      unsigned int i;

      for (i = 0; i < num_streams; ++i)
	{
	  wstream_df_stream_p stream = (wstream_df_stream_p) ((void **) s)[i];
	  __sync_add_and_fetch (&stream->refcount, 1);
	}
    }
}

/***************************************************************************/
/* Runtime extension for arrays of views and DF broadcast.  */
/***************************************************************************/

void
wstream_df_resolve_n_dependences (size_t n, size_t frame_id, size_t data_array_offset,
				  size_t view_array_offset, size_t burst,
				  size_t horizon, void *stream_array_ptr, bool is_read_view_p)
{
  unsigned int i;

  for (i = 0; i < n; ++i)
    {
      wstream_df_stream_p stream = ((wstream_df_stream_p *) stream_array_ptr)[i];
      size_t view_offset = view_array_offset + i * sizeof (wstream_df_view_t);
      size_t data_offset = data_array_offset + i * horizon;

      /* Write the ID in the current view.  */
      dfs_twrite (frame_id, view_offset + offsetof (wstream_df_view_t, owner), frame_id);

      /* Write the RELATIVE (wrt. the view_array_offset) data offset
	 in the current view.  */
      if (is_read_view_p)
	dfs_twrite (frame_id, view_offset + offsetof (wstream_df_view_t, data_offset), data_offset - view_array_offset);

      /* Write the burst in the current view.  */
      dfs_twrite (frame_id, view_offset + offsetof (wstream_df_view_t, burst), burst);

      wstream_df_resolve_dependences (frame_id, data_offset, view_offset,
				 burst, horizon, stream, is_read_view_p);
    }
}

static inline void
broadcast (void *data_buffer, void *peek_chain, size_t burst)
{
  peek_chain_p chain = (peek_chain_p) peek_chain;
  size_t i;

  if (chain == NULL)
    return;

  for (i = 0; i < chain->num_peekers; ++i)
    {
      dfs_tstore (chain->peekers[i].id, chain->peekers[i].data_offset, data_buffer, burst);
      dfs_tdecrease_n (chain->peekers[i].id, burst);
    }
}

void
wstream_df_broadcast (void *data_buffer, void *peek_chain, size_t burst)
{
  broadcast (data_buffer, peek_chain, burst);
}

/* Decrease the synchronization counter of each of the NUM views in
   the array by N.  */
void
wstream_df_tdecrease_n_vec (size_t num, void *frame_ptr,
			    size_t dummy_view_offset, size_t n)
{
  size_t view_array_offset =
    ((wstream_df_view_p) (frame_ptr + dummy_view_offset))->data_offset;
  wstream_df_view_p view_array = (wstream_df_view_p) (frame_ptr + view_array_offset);
  unsigned int i;

  for (i = 0; i < num; ++i)
    {
      dfs_tdecrease_n (view_array[i].owner, n);
    }
}

/* TSTORE all output buffers of an array of views.  */
void
wstream_df_tstore_vec (size_t num, void *frame_ptr,
		       size_t dummy_view_offset, size_t burst, void *buffer)
{
  size_t view_array_offset =
    ((wstream_df_view_p) (frame_ptr + dummy_view_offset))->data_offset;
  wstream_df_view_p view_array = (wstream_df_view_p) (frame_ptr + view_array_offset);
  unsigned int i;

  for (i = 0; i < num; ++i)
    {
#ifdef _WSTREAM_DF_DEBUG
      if (view_array[i].burst != burst)
	wstream_df_fatal ("Using broadcast with mismatched bursts.");
#endif
      dfs_tstore (view_array[i].owner, view_array[i].data_offset,
		  ((char *) buffer) + burst * i, burst);
      if (view_array[i].peek_chain != NULL)
	broadcast (((char *) buffer) + burst * i, view_array[i].peek_chain, burst);
    }
}

void
wstream_df_tick (void *s, size_t burst)
{
  wstream_df_stream_p stream = (wstream_df_stream_p) s;
  size_t fake_frame;

  /* As we don't adjust the burst to be in bytes in the code
     generator, do it here. */
  burst *= stream->element_size;

  /* ASSERT that cons_view->active_peek_chain != NULL !! Otherwise
     this requires passing the matching producers a fake consumer view
     where to write at least during the producers' execution ... */
  if (stream->has_active_peek_chain == false)
    {
      /* Normally an error, but for OMPSs expansion, we need to allow
	 it.  */
      /*  wstream_df_fatal ("TICK (%d) matches no PEEK view, which is unsupported at this time.",
	  burst * stream->elem_size);
      */
      /* Allocate a fake view, with a fake data block, that allows to
	 keep the same dependence resolution algorithm.  */
      size_t frame_size = burst + sizeof (wstream_df_view_t) + sizeof (wstream_df_frame_t);

      fake_frame = dfs_tcreate (frame_size + 1, frame_size, NULL);
      current_thread->executed_frames++; /* Ensure that this fake frame is not awaited at termination.  */
      wstream_df_resolve_dependences (fake_frame,
				 sizeof (wstream_df_frame_t) + sizeof (wstream_df_view_t),
				 sizeof (wstream_df_frame_t),
				 burst, burst, s, true);
    }
  else
    {
      stream->num_completed_peek_chains +=
	spsc_fifo_close_peek_chain (&stream->consumer_queue, burst);
      stream->has_active_peek_chain = false;
      match_stream_queues (stream);
    }

}
