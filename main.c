#define N 6

extern void *__builtin_ia32_get_cfp ();

extern void *__builtin_ia32_tcreate (size_t, size_t);

extern void __builtin_ia32_tdecrease (void *);

extern void __builtin_ia32_tdecrease_n (void *, size_t);

extern void __builtin_ia32_tend ();

typedef struct fp_consumer{
  struct fp_consumer *next;
  void (*work_fn)(void);
  /* synchronization counter.  */
  int sc;
}struct_fp_consumer, *struct_fp_consumer_p;

typedef struct fp_producer{
  struct fp_consumer *next;
  void (*work_fn)(void);
  /* synchronization counter.  */
  int sc;
  void *fp_producer_cont;
}struct_fp_producer, *struct_fp_producer_p;

typedef struct fp_producer{
  struct fp_consumer *next;
  void (*work_fn)(void);
  /* synchronization counter.  */
  int sc;
  int result;
  void *fp_consumer_p;
  void *fp_consumer_cont_p;
  void *fp_producer_cont;
}struct_fp_producer, *struct_fp_producer_p;

int A[N];

int generate_write_index (int i)
{
  return i%3;
}

int generate_read_index (int i)
{
  return i%100;
}

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

int read_write_array_stream ()
{
  int i, tmp;

  for (i = 0; i < N; i++){
    /**************** Handling consumer **********************/
    int rindex = generate_read_index (i);

    /* Create the consumer worker thread.  */
    fp_consumer *fpc = dfs_tcreate (1, sizeof (struct_fp_consumer), consumer_work_fn);

    /* Get the df-frame of the producer's continuation.  */
    fp_producer_cont = get_fp_producer_cont (rindex);

    /* Write the df-frame of the consumer to it's producer's continuation.  */
    dfs_twrite (fp_producer_cont, offsetof (struct_fp_producer_cont, fp_consumer), fpc);

    /* Create a continuation for fp_producer_cont. Which will take care of passing
       the result to possible following consumers (broadcast).  */
    fp_consumer_cont *fpct = dfs_tcreate (1, sizeof (struct_fp_consumer_cont), producer_cont_work_fn);

    /* Write the continuation of fp_producer_cont to fp_producer_cont. So that when the
       result is fired, it'll write the result to this continuation.  */
    dfs_twrite (fp_producer_cont, offsetof (struct_fp_producer_cont, fp_consumer_cont_p), fpct);

    /* Update the producer continuation for stream at windex, which now will
       be fpct.  */
    set_fp_producer_cont (windex, fpct);

    /*************** Handling Producer ***********************/
    int windex = generate_write_index (i);

    /* Create the producer worker thread.  */
    fp_producer *fpp = dfs_tcreate (2, sizeof (struct_fp_producer), producer_work_fn);

    /* Create the continuation for the producer thread.  */
    fp_producer_cont *fppc = dfs_tcreate (3, sizeof (struct_fp_producer_cont), producer_cont_work_fn);

    /* Write the continuation df-frame to producer worker thread.  */
    dfs_twrite (fpp, offsetof (struct_fp_producer, fp_producer_cont), fppc);

    /* Update the producer continuation for stream at index windex.  */
    set_fp_producer_cont (windex, fppc);
  }
}
