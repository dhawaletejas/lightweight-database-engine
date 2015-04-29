/*
 * storage_mgr.c
 *
 *  Created on: Oct 06, 2014
 *      Author: Tejas Dhawale, Deepika Chaudhari, Vaishali Pandurangan
 */

############################################################################
Directory Files:

storage_mgr.h  			Storage Manager Interfaces
storage_mgr.c  			Implementation of Storage Manager Interfaces
dberror.h			Error Return Codes Declarations
dberror.c			Error Return Codes Implementations
test_assign1_1.c 		Test Cases to check accuracy of Implementation
test_assign1_2.c		Additional Test Cases to check entire implementation throughout
Makefile      			gcc Makefile
readme.txt			Current File

############################################################################
Solution Design:

PAGE FILE Structure:

1) FILE HEADER: Comprises of totalNumPages and curPagePos.
2) FILE BODY: File Records / User Data
	  
Preliminary Checks incorporated to Read/Write a File:

1) Check that the File Handle and Page Handle are valid.
2) Prior to a File Read, check that pageNum is within the range of 0 and totalNumPages.
3) Prior to a File Write, if pageNum exceeds totalNumPages, then add those many number of Empty Blocks to the File.
4) Prior to a File Read/Write, seek to the beginning of the page within the file, considering required offsets (page header offsets).

Code Reusability:

1. Reused readBlock() function within readFirstBlock(), readLastBlock(), readNextBlock(), readPreviousBlock() & readCurrentBlock().
2. Reused writeBlock() function within writeCurrentBlock().
3. Reused (re-iteratively) appendEmptyBlock() function within ensureCapacity().

############################################################################
EXTRA CREDIT EXTENSIONS:

1. Created new Test Cases in test_assign1_2.c to test the entire implementation thoroughly.
   It writes 3 blocks to the newly created and opened page file and reads those blocks utilizing all functions in the implementation.
   It also checks for accurate storage of File Header information - Total Number of Pages & Current Page Position.

######################################
STEPS to run Test Cases, 

1. make
2. ./test_assign1_1.exe
3. ./test_assign1_2.exe
