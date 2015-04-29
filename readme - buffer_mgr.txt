/*
 * buffer_mgr.c
 *
 *  Created on: Nov 04, 2014
 *      Author: Tejas Dhawale, Deepika Chaudhari, Vaishali Pandurangan
 */

############################################################################
Directory Files:

buffer_mgr.h			Buffer Manager Interfaces
buffer_mgr.c			Implementation of Buffer Manager
buffer_mgr_stat.h		Functions to output buffer or page content to stdout or into a string
buffer_mgr_stat.c		Implementation of buffer_mgr_stat.h
buffer_mgr_stat_clk.c		Implementation of buffer_mgr_stat.h designed for Clock Page Replacement Algorithm
storage_mgr.h  			Storage Manager Interfaces
storage_mgr.c  			Implementation of Storage Manager Interfaces
dberror.h			Error Return Codes Declarations
dberror.c			Error Return Codes Implementations
dt.h				
test_helper.h
test_assign1_1.c 		Test cases for the buffer_mgr interface using the FIFO and LRU strategies
test_assign1_2.c		Additional Test Cases for the buffer_mgr interface using the CLOCK strategy
Makefile      			gcc Makefile
readme.txt			Current File

############################################################################
Solution Design:

BUFFER MANAGER Structure:

1) BUFFER POOL: Buffer Pool is initialized with numPages number of FRAMES that can store pages from the designated file from disk.

	Data Structure:
	a) Doubly Linked List - For FIFO and LRU Implementations.
	b) Circular Linked List - For CLOCK Implementation.

	Linked List Structuring:
	The linked list is structured to portray HEAD->TAIL : NEWEST->OLDEST fashion in case of FIFO and LRU implementations.

	FIFO Structuring:
	In case of FIFO, new page is always appended to the beginning (head) of the linked list, and the oldest (FIRST IN) page is evicted from the tail of the linked list. While pinning an already existing page in the buffer pool, the frame (node) is not re-structured within the linked list.

	LRU Structuring:
	In case of LRU, the frame (node) in which page replacement occurs is shifted as the new head of the linked list. This maintains the most recently used frames towards the beginning (head-end) of the linked list, while pushing the least recently used frames(nodes) towards the tail-end of the linked list. While pinning an already existing page in the buffer pool, the frame (node) is re-structured within the linked list to make it the new head of the linked list.

	CLOCK Structuring:
	In case of CLOCK, no re-structuring of the linked list occurs since Page Replacements are carried out as per the CLOCK POINTER (clkPtr variable) - as specified by the norms of the algorithm.
	  
2) ERROR HANDLING is performed using additional RETURN CODES:
	RC_BM_NULL_FRAME 401
	RC_BM_NULL_BUFFER 402
	RC_BM_NULL_PGFILE 403
	RC_BM_NULL_PAGE 404

############################################################################
EXTRA CREDIT EXTENSIONS:

1. Created new Test Cases in test_assign2_2.c to test the Clock Page Replacement Strategy which follows as per the clkPtr and refBit variables (Algorithm policy).
2. Added additional RC (Error Codes) to implement efficient error handling within the buffer manager.

######################################
STEPS to run Test Cases, 

1. make clean
2. make
3. ./test_assign2_1.exe
4. ./test_assign2_2.exe
