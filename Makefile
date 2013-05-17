LIB_WSTREAM = libworkstream_df
LIB_DIR = $(LIB_WSTREAM)
CFLAGS = -Wall -std=c99 -O3 -ffast-math -g -D_DEBUG
LDFLAGS = -L$(LIB_DIR) -lm -rdynamic -Wl,-rpath,$(LIB_DIR) -lwstream_df

TESTS = main df_main lib

.PHONY: $(LIB_WSTREAM)

$(LIB_WSTREAM):
	make -C $@

all: $(TESTS)

df_main: df_main.c
	gcc $(CFLAGS) $(LDFLAGS) $^ -o $@ 

lib: lib.c
	gcc $(CFLAGS) $(LDFLAGS) $^ -o $@ 

df_test: df_test.c main.h
	gcc $(CFLAGS) $(LDFLAGS) $^ -o $@

main: main.c main.h
	gcc $(CFLAGS) $^ -o $@

test: test.c
	gcc $(CFLAGS) $(LDFLAGS) $^ -o $@


clean:
	rm $(TESTS)