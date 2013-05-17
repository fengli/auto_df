#include "main.h"
#include <stdio.h>
#include <stddef.h>
#include <stdbool.h>

typedef void (*workfn)(void);

typedef struct fp_consumer_producer{
  struct fp_consumer *next;
  void (*work_fn)(void);
  /* synchronization counter.  */
  int sc;
  void *fp_producer_cont;
  size_t result;
}struct_fp_consumer_producer,struct_fp_producer,struct_fp_consumer;

typedef struct fp_producer_cont{
  struct fp_producer *next;
  void (*work_fn)(void);
  /* synchronization counter.  */
  int sc;
  size_t result;
  void *fp_consumer;
  void *fp_producer_cont;
}struct_fp_producer_cont, *struct_fp_producer_cont_p;

int A[N];
size_t A_shadow[N];

int read_write_array ()
{
  int i;

  for (i = 0; i < N; i++)
    {
      A[generate_write_index (i)]  = A[generate_read_index (i)];
    }

  return A[N/2];
}

void init_array ()
{
  int i;

  for (i = 0; i < N; i++)
    A[i] = i;
}


void consumer_producer_work_fn ()
{
  XLOG ("* %s\n", __FUNCTION__);
  
  struct_fp_consumer_producer *fpcp = (struct_fp_consumer_producer *) dfs_tload ();
  size_t result = fpcp->result;
  struct_fp_producer_cont *fppc = (struct_fp_producer_cont *) fpcp->fp_producer_cont;

  dfs_twrite ((size_t)fppc, offsetof (struct_fp_producer_cont, result), result);
  dfs_tdecrease ((size_t)fppc);

  dfs_tend ();
}

void producer_work_fn ()
{
  XLOG ("* %s\n", __FUNCTION__);
  
  struct_fp_producer *fpp = (struct_fp_producer *) dfs_tload ();
  size_t result = fpp->result;
  struct_fp_producer_cont *fppc = (struct_fp_producer_cont *) fpp->fp_producer_cont;

  dfs_twrite ((size_t) fppc, offsetof (struct_fp_producer_cont, result), result);
  dfs_tdecrease ((size_t) fppc);

  dfs_tend ();
}

void producer_cont_work_fn ()
{
  XLOG ("* %s\n", __FUNCTION__);
  
  struct_fp_producer_cont *fppc = (struct_fp_producer_cont *) dfs_tload();
  size_t result = fppc->result;
  struct_fp_consumer *fpc = fppc->fp_consumer;
  dfs_twrite ((size_t)fpc, offsetof (struct_fp_consumer, result), result);
  dfs_tdecrease ((size_t)fpc);
  struct_fp_producer_cont *fppc2 = fppc->fp_producer_cont;
  dfs_twrite ((size_t) fppc2, offsetof (struct_fp_producer_cont, result), result);
  dfs_tdecrease ((size_t) fppc2);

  dfs_tend ();
}

void consumer_print_result_work_fn ()
{
  XLOG ("* %s \n", __FUNCTION__);

  struct_fp_consumer *fpc = (struct_fp_consumer *) dfs_tload ();
  size_t result = fpc->result;
  fprintf (stderr, "[stream] result calculated:%zd\n", result);
}

size_t get_fp_producer_cont (int i)
{
  XLOG ("* %s return=%zx\n", __FUNCTION__,A_shadow[i]);
  return A_shadow[i];
}

void set_fp_producer_cont (int i, size_t val)
{
  XLOG ("* %s: A_shadow[%d]=%zx\n", __FUNCTION__,i, val);
  A_shadow[i] = val;
}

/* At producer node, a producer and a continuation for the producer
   will be created.  */
void
_register_producer_node (struct_fp_producer *fpp, workfn producer_work_fn,
			 workfn producer_cont_work_fn, int windex,
			 size_t value_to_produce)
{
  /* self_produce_mode equals to true means the node will generate the value by
     itself through parameter instead of accepting from another source.  */
  bool self_produce_mode = true;
  if (fpp) self_produce_mode = false;

  if (self_produce_mode)
    fpp = (struct_fp_producer *) dfs_tcreate (1, sizeof (struct_fp_producer), producer_work_fn);

  struct_fp_producer_cont *fppc =
    (struct_fp_producer_cont *) dfs_tcreate (3, sizeof (struct_fp_producer_cont), producer_cont_work_fn);
  dfs_twrite ((size_t) fpp, offsetof (struct_fp_producer, fp_producer_cont), (size_t) fppc);
  set_fp_producer_cont (windex, (size_t) fppc);

  if (self_produce_mode){
    dfs_twrite ((size_t) fpp, offsetof (struct_fp_producer, result), value_to_produce);
  }

  dfs_tdecrease ((size_t) fpp);
}

struct_fp_consumer *
_register_consumer_node (workfn consumer_work_fn, workfn producer_cont_work_fn,
			 int rindex, int sc)
{
  /* Create the consumer worker thread.  */
  struct_fp_consumer *fpc =
    (struct_fp_consumer *)dfs_tcreate (sc,sizeof (struct_fp_consumer),
				       consumer_work_fn);

  /* Get the df-frame of the producer's continuation.  */
  struct_fp_producer_cont *fppc = (struct_fp_producer_cont *) get_fp_producer_cont (rindex);

  /* Write the df-frame of the consumer to it's producer's continuation.  */
  dfs_twrite ((size_t) fppc, offsetof (struct_fp_producer_cont, fp_consumer), (size_t) fpc);
  dfs_tdecrease ((size_t)fppc);

  /* Create a continuation for fp_producer_cont. Which will take care of passing
     the result to possible following consumers (broadcast).  */
  struct_fp_producer_cont *fppc_next =
    (struct_fp_producer_cont *) dfs_tcreate (3, sizeof (struct_fp_producer_cont), producer_cont_work_fn);

  /* Write the continuation of fp_producer_cont to fp_producer_cont. So that when the
     result is fired, it'll write the result to this continuation.  */
  dfs_twrite ((size_t)fppc, offsetof (struct_fp_producer_cont, fp_producer_cont), (size_t)fppc_next);
  dfs_tdecrease ((size_t) fppc);

  /* Update the producer continuation for stream at windex, which now will
     be fpct.  */
  set_fp_producer_cont (rindex, (size_t) fppc_next);

  return fpc;
}

void
register_producer_node (workfn producer_work_fn, workfn producer_cont_work_fn,
			int windex, size_t value_to_produce)
{
  _register_producer_node (NULL, producer_work_fn, producer_cont_work_fn,
			   windex, value_to_produce);
}

void register_consumer_node (workfn consumer_work_fn, workfn producer_cont_work_fn, int rindex)
{
  _register_consumer_node (consumer_work_fn, producer_cont_work_fn, rindex, 1);
}

void register_consumer_producer_node (workfn consumer_producer_work_fn, workfn producer_cont_work_fn,
				      int rindex, int windex)
{
  struct_fp_consumer_producer *fpcp = _register_consumer_node (consumer_producer_work_fn,
							       producer_cont_work_fn,
							       rindex, 2);
  _register_producer_node (fpcp, NULL, producer_cont_work_fn, windex, 0);
}

void
init_array_stream ()
{
  int i;

  XLOG ("* %s\n", __FUNCTION__);


  for (i = 0; i < N; i++){
    int windex = i;
    size_t value_to_produce = (size_t) i + 101;

    register_producer_node (producer_work_fn, producer_cont_work_fn, windex, value_to_produce);
  }
}

void
read_write_array_stream ()
{
  int i;

  XLOG ("* %s\n", __FUNCTION__);

  for (i = 0; i < N; i++){
    XLOG ("%dth iteration:\n",i);

    int rindex = generate_read_index (i);
    int windex = generate_write_index (i);

    register_consumer_producer_node (consumer_producer_work_fn, producer_cont_work_fn,
				     rindex, windex);
  }
}

void print_result_stream ()
{
  XLOG ("===function %s ===\n", __FUNCTION__);
  int rindex = N/2;

  register_consumer_node (consumer_print_result_work_fn, producer_cont_work_fn, rindex);
}

int main (int argc, char **argv)
{
  int result;

  init_array_stream ();
  read_write_array_stream ();
  print_result_stream ();
}
