TARGETS = test_assign3_1.exe test_expr.exe
CC = gcc
CCFLAGS = -g
LIBFLAGS = -lpthread

all: 	$(TARGETS)

test_assign3_1.exe:	test_assign3_1.o record_mgr.o expr.o buffer_mgr_stat.o dberror.o buffer_mgr.o storage_mgr.o
	$(CC) $(CCFLAGS) -o $@ $^

test_expr.exe: test_expr.o record_mgr.o expr.o buffer_mgr_stat.o dberror.o buffer_mgr.o storage_mgr.o
	$(CC) $(CCFLAGS) -o $@ $^

test_assign3_1.o:	test_assign3_1.c
	$(CC) $(CCFLAGS) -c test_assign3_1.c

record_mgr.o:   record_mgr.c record_mgr.h
	$(CC) $(CCFLAGS) -c record_mgr.c
	
buffer_mgr.o:	buffer_mgr.c buffer_mgr.h
	$(CC) $(CCFLAGS) -c buffer_mgr.c
	
expr.o:	expr.c expr.h
	$(CC) $(CCFLAGS) -c expr.c

tables.o:	tables.c tables.h
	$(CC) $(CCFLAGS) -c tables.c	
	
buffer_mgr_stat.o:	buffer_mgr_stat.c buffer_mgr_stat.h
	$(CC) $(CCFLAGS) -c buffer_mgr_stat.c

buffer_mgr_stat_clk.o:	buffer_mgr_stat_clk.c buffer_mgr_stat.h
	$(CC) $(CCFLAGS) -c buffer_mgr_stat_clk.c

storage_mgr.o:	storage_mgr.c storage_mgr.h
	$(CC) $(CCFLAGS) -c storage_mgr.c
	
dberror.o: dberror.c dberror.h
	$(CC) $(CCFLAGS) -c dberror.c

.PHONY:	clean

clean:
	rm *.o
	rm *.exe
	rm *.bin*
