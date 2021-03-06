#include "main.h"
#include <stdio.h>
#include <stddef.h>

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
  XLOG ("* %s: get value:%zd, write to fppc:%zx\n", __FUNCTION__, result, (size_t)fppc);
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

void init_array_stream ()
{
  int i;

  XLOG ("* %s\n", __FUNCTION__);
  
  for (i = 0; i < N; i++){
    struct_fp_producer *fpp =
      (struct_fp_producer *) dfs_tcreate (1, sizeof (struct_fp_producer), producer_work_fn);

    struct_fp_producer_cont *fppc =
      (struct_fp_producer_cont *) dfs_tcreate (3, sizeof (struct_fp_producer_cont), producer_cont_work_fn);
    dfs_twrite ((size_t) fpp, offsetof (struct_fp_producer, fp_producer_cont), (size_t) fppc);
    dfs_twrite ((size_t) fpp, offsetof (struct_fp_producer, result), (size_t) (i+100));
    set_fp_producer_cont (i, (size_t) fppc);
    dfs_tdecrease ((size_t) fpp);
  }
}

void read_write_array_stream ()
{
  int i;

  XLOG ("* %s\n", __FUNCTION__);

  for (i = 0; i < N; i++){
    XLOG ("%dth iteration:\n",i);
    /**************** Handling consumer **********************/
    int rindex = generate_read_index (i);

    /* Create the consumer and producer worker thread.  */
    struct_fp_consumer_producer *fpcp =
      (struct_fp_consumer_producer *)dfs_tcreate (2,sizeof (struct_fp_consumer_producer),
						  consumer_producer_work_fn);

    /* Get the df-frame of the producer's continuation.  */
    struct_fp_producer_cont *fppc = (struct_fp_producer_cont *) get_fp_producer_cont (rindex);

    /* Write the df-frame of the consumer to it's producer's continuation.  */
    dfs_twrite ((size_t) fppc, offsetof (struct_fp_producer_cont, fp_consumer), (size_t) fpcp);
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

    /*************** Handling Producer ***********************/
    int windex = generate_write_index (i);;

    /* Create the continuation for the producer thread.  */
    struct_fp_producer_cont *fppc2 =
      (struct_fp_producer_cont *)dfs_tcreate (3, sizeof (struct_fp_producer_cont), producer_cont_work_fn);

    /* Write the continuation df-frame to consumer-producer worker thread.  */
    dfs_twrite ((size_t) fpcp, offsetof (struct_fp_consumer_producer, fp_producer_cont), (size_t) fppc2);
    dfs_tdecrease ((size_t) fpcp);

    /* Update the producer continuation for stream at index windex.  */
    set_fp_producer_cont (windex, (size_t) fppc2);
  }
}

void print_result_stream ()
{
  XLOG ("===function %s ===\n", __FUNCTION__);
  int rindex = N/2;

  /* Create the consumer worker thread.  */
  struct_fp_consumer *fpcp =
    (struct_fp_consumer *)dfs_tcreate (1,sizeof (struct_fp_consumer),
				       consumer_print_result_work_fn);

  /* Get the df-frame of the producer's continuation.  */
  struct_fp_producer_cont *fppc = (struct_fp_producer_cont *) get_fp_producer_cont (rindex);

  /* Write the df-frame of the consumer to it's producer's continuation.  */
  dfs_twrite ((size_t) fppc, offsetof (struct_fp_producer_cont, fp_consumer), (size_t) fpcp);
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
}

int main (int argc, char **argv)
{
  int result;

  init_array_stream ();
  read_write_array_stream ();
  print_result_stream ();
}
