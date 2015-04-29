/*
 * record_mgr.c
 *
 *  Created on: Dec 04, 2014
 *      Author: Tejas Dhawale, Deepika Chaudhari, Vaishali Pandurangan
 */

############################################################################
Directory Files:

record_mgr.h			Record Manager Interfaces
record_mgr.c			Implementation of Record Manager
expr.h					Defines data structures and functions to deal with expressions for scans.
expr.c					Implementation of expr.h
buffer_mgr.h			Buffer Manager Interfaces
buffer_mgr.c			Implementation of Buffer Manager
buffer_mgr_stat.h		Functions to output buffer or page content to stdout or into a string
buffer_mgr_stat.c		Implementation of buffer_mgr_stat.h
buffer_mgr_stat_clk.c	Implementation of buffer_mgr_stat.h designed for Clock Page Replacement Algorithm
storage_mgr.h  			Storage Manager Interfaces
storage_mgr.c  			Implementation of Storage Manager Interfaces
dberror.h				Error Return Codes Declarations
dberror.c				Error Return Codes Implementations
dt.h					Datatypes Header File	
rm_serializer.c			Serialization Functions for serializing data structures as strings.	
tables.h
test_helper.h			Defines several helper methods for implementing test cases such as ASSERT_TRUE.
test_assign3_1.c 		Test cases for the record_mgr interface
test_expr.c				Test cases using the expr.h interface.
Makefile      			gcc Makefile
readme.txt				Current File

############################################################################
Solution Design:

RECORD MANAGER:

1) The record manager handles tables with a fixed schema.
Clients can insert records, delete records, update records, and scan through the records in a table.
A scan is associated with a search condition and only returns records that match the search condition.
Each table is stored in a separate page file and the record manager accesses the pages of the file through the buffer manager.

Table and Record Manager Functions:

These include functions to initialize and shutdown a record manager.
Furthermore, there are functions to create, open, and close a table.
Creating a table creates the underlying page file and stores information about the schema, free-space etc in the Table Information pages.
All operations on a table such as scanning or inserting records require the table to be opened first.
Afterwards, clients can use the RM_TableData struct to interact with the table.
Closing a table causes all outstanding changes to the table to be written to the page file.
The getNumTuples function returns the number of tuples in the table.

Record Functions

These functions are used to retrieve a record with a certain RID, to delete a record with a certain RID, to insert a new record, and to update an existing record with new values.
When a new record is inserted the record manager assigns an RID to this record and update the record parameter passed to insertRecord.

Scan Functions

A client can initiate a scan to retrieve all tuples from a table that fulfill a certain condition (represented as an Expr).
Starting a scan initializes the RM_ScanHandle data structure passed as an argument to startScan.
Afterwards, calls to the next method should return the next tuple that fulfills the scan condition.
If NULL is passed as a scan condition, then all tuples of the table should be returned.
next() function returns RC_RM_NO_MORE_TUPLES once the scan is completed and RC_OK otherwise.

Schema Functions

These helper functions are used to return the size in bytes of records for a given schema and create a new schema.

Attribute Functions

These functions are used to get or set the attribute values of a record and create a new record for a given schema.
Creating a new record should allocate enough memory to the data field to hold the binary representations for all attributes of this record as determined by the schema.

2) ERROR HANDLING is performed using additional RETURN CODES:
	RC_RM_LARGE_SCHEMA 501
	RC_RM_LARGE_RECORD 502
	RC_RM_INSERT_FAILED 503
	RC_RM_DELETE_FAILED 504
	RC_RM_UPDATE_FAILED 505

############################################################################
EXTRA CREDIT EXTENSIONS:

1. Implemented the TID and Tombstone concepts.
2. Added additional RC (Error Codes) to implement efficient error handling within the record manager.

######################################
STEPS to run Test Cases, 

1. make clean
2. make
3. ./test_assign3_1.exe
4. ./test_expr.exe
