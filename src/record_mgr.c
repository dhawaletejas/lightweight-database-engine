/*
 * record_mgr.c
 *
 *  Created on: 06-Nov-2014
 *      Author: Tejas Dhawale, Deepika Chaudhari, Vaishali Pandurangan
 *
 */

	#include "record_mgr.h"
	#include "rm_serializer.c"
	#include "buffer_mgr.h"
	#include "storage_mgr.h"
	#include "string.h"
	#include "assert.h"

	#define REC_SZ (PAGE_SIZE - (sizeof(char) + (2*sizeof(int)) + '\0'))

	typedef struct Frame
	{
		bool dirtyBit;
		int fixBit;
		struct Frame* next;
		struct Frame* prev;
		BM_PageHandle page;
		int seq;
		int refBit;
	} Frame;

	typedef struct BM_MgmtData
	{
		PageNumber *frameContents;
		bool *dirtyFlags;
		int *fixCounts;
		int *refBits;
		int numReadIO;
		int numWriteIO;
		Frame* clkPtr;
		Frame* head;
		Frame* tail;
		SM_FileHandle fHandle;
	}BM_MgmtData;

	typedef struct RM_ScanTuple
	{
		int next; // Next tuple
		int prev; // Previous tuple
		char data; // Data within this tuple
	}RM_ScanTuple;

	// Stores Management Information of a Table
	typedef struct RM_MgmtData_Table
	{
		int recCnt; //Total count of records in the Table.
		int initFreePg; //Initial (First) Free Page.
		BM_BufferPool bm;
		BM_PageHandle h;
	} RM_MgmtData_Table;

	typedef struct RM_MgmtData_Scan
	{
		RID rid; //Record being Scanned.
		int recScanCnt; //Count of Records scanned by far.
		Expr *cond; //Conditional Expression to be evaluated.
		RM_ScanTuple *dataPtr; //Pointer to scan individual tuples.
		BM_PageHandle h;
	} RM_MgmtData_Scan;

	static int findFreeSlot(RM_ScanTuple *dataPtr, Schema *schema);

	//########## TABLE AND MANAGER ##########

	RC initRecordManager (void *mgmtData)
	{
		initStorageManager();
		return RC_OK;
	}

	RC shutdownRecordManager ()
	{
		return RC_OK;
	}

	//########## DEALING WITH SCHEMAS ##########

	/*
	 * function getRecordSize:
	 * Calculates size of each Record in the given Schema
	 */
	int getRecordSize (Schema *schema)
	{
		int recSz = 0;
		int i=0;
		while(i<schema->numAttr)
		{
			recSz = recSz+schema->typeLength[i];
			i++;
		}
		return (recSz);
	}


	Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes,
			int *typeLength, int keySize, int *keys)
	{
		Schema *sch;
		int i;

		// Memory Allocation to Schema
		sch= (Schema*) malloc(sizeof(Schema));
		sch->numAttr= numAttr;
		sch->keySize= keySize;
		sch->attrNames= (char**) malloc( sizeof(char*)*numAttr);
		sch->dataTypes= (DataType*) malloc( sizeof(DataType)*numAttr);
		sch->typeLength= (int*) malloc( sizeof(int)*numAttr);
		sch->keyAttrs= (int*) malloc( sizeof(int)*keySize);
		i=0;
		while(i<numAttr)
		{
			sch->attrNames[i]= (char*) malloc(64);
			i++;
		}

		// Read entire data of the schema
		i=0;
		while(i<sch->numAttr)
		{
			strncpy(sch->attrNames[i], attrNames[i], 64);
			sch->dataTypes[i]= dataTypes[i];
			if (i<keySize)
				sch->keyAttrs[i]= keys[i];
			switch(sch->dataTypes[i])
			{
			case DT_STRING:
				sch->typeLength[i]= typeLength[i];
				break;
			case DT_INT:
				sch->typeLength[i]= sizeof(int);
				break;
			case DT_FLOAT:
				sch->typeLength[i]= sizeof(float);
				break;
			case DT_BOOL:
				sch->typeLength[i]= sizeof(bool);
				break;
			default:
				assert(!"INVALID DATATYPE");
			}
			i++;
		}
		return sch;
	}

	RC freeSchema(Schema *schema)
	{
		int i=0;
		while(i<schema->numAttr)
		{
			free(schema->attrNames[i]);
			i++;
		}
		free(schema->attrNames);
		free(schema->dataTypes);
		free(schema->typeLength);
		free(schema->keyAttrs);
		free(schema);
		return RC_OK;
	}

	//########## TABLE MANAGEMENT ##########

	/*
	 * Function createTable:
	 *
	 * Creates the underlying Page File for the Table and stores Schema Information in it.
	 */

	RC createTable (char *name, Schema *schema)
	{
		SM_FileHandle fh;
		char data[PAGE_SIZE];
		char *ofst= data;
		int recLen,i;

		// Schema Size cannot exceed 1 Page
		recLen= (4 * sizeof(int)); // recCnt, initFreePg, numAttrs, keySize
		recLen = recLen + (76 * schema->numAttr); // Name(64) + type(4) + len(4) + keyAttr(4)
		if (recLen > PAGE_SIZE)
			return RC_RM_LARGE_SCHEMA;

		// Record Size Limit Check
		recLen= (getRecordSize(schema)+1); // +1 byte for storing TOMBSTONE
		if (recLen > REC_SZ)
			return RC_RM_LARGE_RECORD;

		// recCnt, initFreePg, numAttr, keySize
		memset(ofst, 0, PAGE_SIZE);
		*(int*)ofst = 0; // For number of tuples
		ofst = ofst + sizeof(int);

		*(int*)ofst = 0; // For initFreePg
		ofst = ofst + sizeof(int);

		*(int*)ofst = schema->numAttr;
		ofst = ofst + sizeof(int);

		*(int*)ofst = schema->keySize;
		ofst = ofst + sizeof(int);

		i=0;
		while(i<schema->numAttr)
		{
			// attrNames, dataTypes, typeLength, keyAttrs \0 delimited
			strncpy(ofst, schema->attrNames[i], 64);
			ofst = ofst + 64;

			*(int*)ofst= (int) schema->dataTypes[i];
			ofst = ofst + 4;

			*(int*)ofst= (int) schema->typeLength[i];
			ofst = ofst + 4;

			if (i<schema->keySize)
				*(int*)ofst= (int) schema->keyAttrs[i];
			ofst = ofst + 4;
			i++;
		}

		// Create a Page File with single page Table data.
		createPageFile(name);
		openPageFile(name, &fh);
		writeBlock(0, &fh, data);
		closePageFile(&fh);
		return RC_OK;
	}

	/*
	 * Function openTable:
	 *
	 * Opens the Table.
	 */

	RC openTable (RM_TableData *rel, char *name)
	{
		char *ofst;
		RM_MgmtData_Table *td;
		int numAttrs, keySize, i;

		// Allocate RM_TableData
		td= (RM_MgmtData_Table*) malloc( sizeof(RM_MgmtData_Table) );
		rel->mgmtData= td;
		rel->name= strdup(name);

		// Initialize BufferPool
		initBufferPool(&td->bm, rel->name, 1000, RS_FIFO, NULL);
		// Read page and prepare schema
		pinPage(&td->bm, &td->h, (PageNumber)0);

		ofst= (char*) td->h.data;
		td->recCnt= *(int*)ofst;
		ofst = ofst + sizeof(int);
		td->initFreePg= *(int*)ofst;
		ofst = ofst + sizeof(int);
		numAttrs= *(int*)ofst;
		ofst = ofst + sizeof(int);
		keySize= *(int*)ofst;
		ofst = ofst + sizeof(int);

		// Memory Allocation to Schema
		Schema *schema;
		schema= (Schema*) malloc(sizeof(Schema));
		schema->numAttr= numAttrs;
		schema->keySize= keySize;
		schema->attrNames= (char**) malloc( sizeof(char*)*numAttrs);
		schema->dataTypes= (DataType*) malloc( sizeof(DataType)*numAttrs);
		schema->typeLength= (int*) malloc( sizeof(int)*numAttrs);
		schema->keyAttrs= (int*) malloc( sizeof(int)*keySize);
		i=0;
		while(i<numAttrs)
		{
			schema->attrNames[i]= (char*) malloc(64);
			i++;
		}

		rel->schema= schema; // Assign the above Schema Instance

		// Read entire data of schema
		i=0;
		while(i<rel->schema->numAttr)
		{
			strncpy(rel->schema->attrNames[i], ofst, 64);
			ofst = ofst + 64;

			rel->schema->dataTypes[i]= *(int*)ofst;
			ofst = ofst + 4;

			rel->schema->typeLength[i]= *(int*)ofst;
			ofst = ofst + 4;

			if (i<keySize)
				rel->schema->keyAttrs[i]= *(int*)ofst;
			ofst = ofst + 4;
			i++;
		}

		unpinPage(&td->bm, &td->h); // UnPin Page
		return RC_OK;
	}

	/*
	 * Function closeTable():
	 *
	 * Causes outstanding changes to the table
	 * to be written to the page file and then closes the table.
	 */

	RC closeTable (RM_TableData *rel)
	{
		RM_MgmtData_Table *td;
		char *ofst;

		td= rel->mgmtData;

		// Read page and prepare schema
		pinPage(&td->bm, &td->h, (PageNumber)0);

		ofst = (char*) td->h.data;

		markDirty(&td->bm, &td->h);
		*(int*)ofst= td->recCnt;
		unpinPage(&td->bm, &td->h);

		// Shutdown Buffer Pool
		shutdownBufferPool(&td->bm);

		// Free Schema Memory
		free(rel->name);
		free(rel->schema->attrNames);
		free(rel->schema->dataTypes);
		free(rel->schema->typeLength);
		free(rel->schema->keyAttrs);
		free(rel->schema);
		rel->schema= NULL;
		free(rel->mgmtData);
		rel->mgmtData= NULL;
		return RC_OK;
	}

	/*
	 * function deleteTable():
	 */
	RC deleteTable (char *name)
	{
		destroyPageFile(name);
		return RC_OK;
	}

	/*
	 * function getNumTuples():
	 *
	 * Returns the number of tuples in the table
	 */

	int getNumTuples (RM_TableData *rel)
	{
		int recCnt = ((RM_MgmtData_Table*)rel->mgmtData)->recCnt;
		return recCnt;
	}


	//########## HANDLING RECORDS IN TABLE ##########

	/*
	 * function insertRecord():
	 *
	 * Inserts a new record. When a new record is inserted
	 * the record manager assigns RID to this record and
	 * update the record parameter passed to insertRecord.
	 */

	RC insertRecord (RM_TableData *rel, Record *record)
	{
		RM_MgmtData_Table *td= rel->mgmtData;
		BM_MgmtData *bm= td->bm.mgmtData;
		RM_ScanTuple *dataPtr;
		RM_ScanTuple *remFreePgLL;
		RM_ScanTuple *remFreePgLL2;
		RM_ScanTuple *addFreePgLL;
		BM_PageHandle h;
		BM_PageHandle h2;
		RID *rid= &record->id;
		char *slotNo;
		int recordSize;

		if (td->initFreePg == 0)
		{
			// add new page
			if (appendEmptyBlock(&bm->fHandle) != RC_OK)
				return RC_RM_INSERT_FAILED;

			rid->page= bm->fHandle.totalNumPages-1; //Page Number Allocation

			pinPage(&td->bm, &td->h, (PageNumber)rid->page);
			dataPtr= (RM_ScanTuple*) td->h.data;

			rid->slot= 0; //Slot assignment
		}
		else // Space available for Record in a pre-existing page
		{
			rid->page= td->initFreePg;
			pinPage(&td->bm, &td->h, (PageNumber)rid->page);
			dataPtr= (RM_ScanTuple*) td->h.data;
			rid->slot= findFreeSlot(dataPtr, rel->schema);
			if (rid->slot==-1)
			{
				unpinPage(&td->bm, &td->h);

				// add new page
				if (appendEmptyBlock(&bm->fHandle) != RC_OK)
					return RC_RM_INSERT_FAILED;
				rid->page= bm->fHandle.totalNumPages-1;
				pinPage(&td->bm, &td->h, (PageNumber)rid->page);
				dataPtr= (RM_ScanTuple*) td->h.data;
				rid->slot= 0;
			}
		}

		// Write record now
		markDirty(&td->bm, &td->h);
		slotNo = ((char*) &dataPtr->data) + (rid->slot*(getRecordSize(rel->schema)+1));
		// Write Record to Slot
		recordSize= getRecordSize(rel->schema);
		memcpy(slotNo+1, record->data, recordSize); // +1 for TOMBSTONE
		*(char*)slotNo=1; //Set TOMBSTONE Address

		//Updating Free Page Linked List---------------------
		// Search if there are any TOMBSTONES which are free.
		if (findFreeSlot(dataPtr, rel->schema) != -1)
		{
			// First page to have free space
			if (td->initFreePg == 0)
			{
				dataPtr->next=dataPtr->prev= rid->page;
				td->initFreePg= rid->page;
			}
			else // Add this page to head of list
			{
				// Read head block and link this page
				pinPage(&td->bm, &h, (PageNumber)td->initFreePg);
				addFreePgLL= (RM_ScanTuple*) h.data;

				markDirty(&td->bm, &h);
				addFreePgLL->prev= rid->page;
				unpinPage(&td->bm, &h);

				dataPtr->next= td->initFreePg;
				td->initFreePg= rid->page;
				dataPtr->prev= 0;
			}
		}
		else // No free space
		{
			// Remove from head
			if (rid->page == td->initFreePg)
			{
				// Single node in list
				if (dataPtr->next == 0)
				{
					dataPtr->prev= 0;
					td->initFreePg= 0;
				}
				else
				{
					// Read head block and link this page
					pinPage(&td->bm, &h, (PageNumber)dataPtr->next);
					remFreePgLL= (RM_ScanTuple*) h.data;
					markDirty(&td->bm, &h);
					remFreePgLL->prev= 0;
					unpinPage(&td->bm, &h);

					td->initFreePg= dataPtr->next;
					dataPtr->next=dataPtr->prev= 0;
				}
			}

			// Remove from tail
			else if (dataPtr->next == 0)
			{
				// Read previous block and remove
				pinPage(&td->bm, &h, (PageNumber)dataPtr->prev);
				remFreePgLL= (RM_ScanTuple*) h.data;
				markDirty(&td->bm, &h);
				remFreePgLL->next= 0;
				unpinPage(&td->bm, &h);

				dataPtr->next=dataPtr->prev= 0;
			}
			else if (dataPtr->next != 0 && dataPtr->prev != 0)// Read from middle
			{
				// Read next block and previous block and reorganize linked list
				pinPage(&td->bm, &h, (PageNumber)dataPtr->prev);
				pinPage(&td->bm, &h2, (PageNumber)dataPtr->next);
				remFreePgLL= (RM_ScanTuple*) h.data;
				remFreePgLL2= (RM_ScanTuple*) h2.data;

				markDirty(&td->bm, &h);
				markDirty(&td->bm, &h2);

				remFreePgLL->next= dataPtr->next;
				remFreePgLL2->prev= dataPtr->prev;

				unpinPage(&td->bm, &h);
				unpinPage(&td->bm, &h2);

				dataPtr->next=dataPtr->prev= 0;
			}
		}
		//---------------------------------------------------
		unpinPage(&td->bm, &td->h);
		td->recCnt++;

		return RC_OK;
	}

	/*
	 * function deleteRecord():
	 *
	 * Deletes a record whose RID is specified.
	 */

	RC deleteRecord (RM_TableData *rel, RID id)
	{
		char *slotNo;
		RM_MgmtData_Table *td= rel->mgmtData;
		RM_ScanTuple *dataPtr;
		RM_ScanTuple *addFreePgLL;
		BM_PageHandle h;

		if (id.page == -1 || id.slot == -1)
			return RC_RM_DELETE_FAILED;

		pinPage(&td->bm, &td->h, (PageNumber)id.page);
		dataPtr= (RM_ScanTuple*) td->h.data;
		slotNo = ((char*) &dataPtr->data) + (id.slot*(getRecordSize(rel->schema)+1));
		markDirty(&td->bm, &td->h);
		*(char*)slotNo = -1; // Remove TOMBSTONE

		// Mark free page links
		// First page to have free space
		if (td->initFreePg == 0)
		{
			dataPtr->next=dataPtr->prev= id.page;
			td->initFreePg= id.page;
		}
		else // Add this page to head of list
		{
			// Read head block and link this page
			pinPage(&td->bm, &h, td->initFreePg);
			addFreePgLL= (RM_ScanTuple*) h.data;

			markDirty(&td->bm, &h);
			addFreePgLL->prev= id.page;
			unpinPage(&td->bm, &h);

			dataPtr->next= td->initFreePg;
			td->initFreePg= id.page;
			dataPtr->prev= 0;
		}

		unpinPage(&td->bm, &td->h);

		td->recCnt--;

		return RC_OK;
	}

	/*
	 * function updateRecord():
	 *
	 * Updates an existing record with new values.
	 */

	RC updateRecord (RM_TableData *rel, Record *record)
	{
		RID *rid= &record->id;
		RM_MgmtData_Table *td= rel->mgmtData;
		RM_ScanTuple *dataPtr;
		char *slotNo;
		int recordSize;

		if (rid->page == -1 || rid->slot == -1)
			return RC_RM_UPDATE_FAILED;

		pinPage(&td->bm, &td->h, (PageNumber)rid->page);
		dataPtr= (RM_ScanTuple*) td->h.data;
		slotNo = ((char*) &dataPtr->data) + (rid->slot*(getRecordSize(rel->schema)+1));
		markDirty(&td->bm, &td->h);
		// Write Record to Slot
		recordSize= getRecordSize(rel->schema);
		memcpy(slotNo+1, record->data, recordSize); // +1 for TOMBSTONE
		unpinPage(&td->bm, &td->h);

		return RC_OK;
	}

	RC getRecord (RM_TableData *rel, RID id, Record *record)
	{
		RM_MgmtData_Table *td= rel->mgmtData;
		RM_ScanTuple *dataPtr;
		char *slotNo;
		int recordSize;

		if (id.page == -1 || id.slot == -1)
			return RC_RM_UPDATE_FAILED;

		pinPage(&td->bm, &td->h, (PageNumber)id.page);
		dataPtr= (RM_ScanTuple*) td->h.data;
		slotNo = ((char*) &dataPtr->data) + (id.slot*(getRecordSize(rel->schema)+1));
		//Read Record from Slot
		recordSize= getRecordSize(rel->schema);
		memcpy(record->data, slotNo+1, recordSize); // +1 for TOMBSTONE
		unpinPage(&td->bm, &td->h);

		record->id= id;

		return RC_OK;
	}


	//########## SCANS ##########

	/*
	 * function startScan
	 */
	RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
	{
		RM_MgmtData_Scan *sd;
		sd = (RM_MgmtData_Scan*) malloc(sizeof(RM_MgmtData_Scan));
		scan->mgmtData = sd;
		sd->rid.page= -1;
		sd->rid.slot= -1;
		sd->recScanCnt= 0;
		sd->cond= cond;
		scan->rel= rel;
		return RC_OK;
	}

	/*
	 * function next():
	 */
	RC next (RM_ScanHandle *scan, Record *record)
	{
		RM_MgmtData_Scan *sd= (RM_MgmtData_Scan*) scan->mgmtData;
		RM_MgmtData_Table *td= (RM_MgmtData_Table*) scan->rel->mgmtData;

		char *slotNo;
		int recordSize;
		Value *result = (Value *) malloc(sizeof(Value));
		result->v.boolV = TRUE;

		if (td->recCnt == 0) //Check if tuples exist
			return RC_RM_NO_MORE_TUPLES;

		do
		{
			if (sd->recScanCnt == 0)
			{
				sd->rid.page= 1;
				sd->rid.slot= 0;
				pinPage(&td->bm, &sd->h, (PageNumber)sd->rid.page);
				sd->dataPtr= (RM_ScanTuple*) sd->h.data;
			}
			else if (sd->recScanCnt == td->recCnt ) // Stop scan
			{
				unpinPage(&td->bm, &sd->h);
				sd->rid.page= -1;
				sd->rid.slot= -1;
				sd->recScanCnt = 0;
				sd->dataPtr= NULL;
				return RC_RM_NO_MORE_TUPLES;
			}
			else
			{
				int totSlots = REC_SZ/(getRecordSize(scan->rel->schema)+1);
				sd->rid.slot++;
				if (sd->rid.slot== totSlots)
				{
					sd->rid.page++;
					sd->rid.slot= 0;
					pinPage(&td->bm, &sd->h, (PageNumber)sd->rid.page);
					sd->dataPtr= (RM_ScanTuple*) sd->h.data;
				}
			}
			slotNo = ((char*) &sd->dataPtr->data) + (sd->rid.slot*(getRecordSize(scan->rel->schema)+1));
			// Read Record from Slot
			recordSize= getRecordSize(scan->rel->schema);
			memcpy(record->data, slotNo+1, recordSize); // +1 for TOMBSTONE

			record->id.page=sd->rid.page;
			record->id.slot=sd->rid.slot;
			sd->recScanCnt++;

			if (sd->cond != NULL)
				evalExpr(record, (scan->rel)->schema, sd->cond, &result);

		}while (!result->v.boolV && sd->cond != NULL);

		return RC_OK;
	}

	/*
	 * function closeScan:
	 */
	RC closeScan (RM_ScanHandle *scan)
	{
		RM_MgmtData_Scan *sd= (RM_MgmtData_Scan*) scan->mgmtData;
		RM_MgmtData_Table *td= (RM_MgmtData_Table*) scan->rel->mgmtData;

		if (sd->recScanCnt > 0) // Is Scan Pending?
			unpinPage(&td->bm, &sd->h); // UnPin Page

		// Free mgmtData memory
		free(scan->mgmtData);
		scan->mgmtData= NULL;

		return RC_OK;
	}


	//########## DEALING WITH RECORDS AND ATTRIBUTE VALUES ##########

	/*
	 * function createRecord:
	 */
	RC createRecord (Record **record, Schema *schema)
	{
		*record = (Record*)malloc(sizeof(Record));
		(*record)->data = (char*)malloc(getRecordSize(schema));
		return RC_OK;
	}

	/*
	 * function freeRecord:
	 */
	RC freeRecord (Record *record)
	{
		free(record->data);
		free(record);
		return RC_OK;
	}

	/*
	 * function getAttr:
	 */
	RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
	{
		void *recOfst = record->data;
		Value *value1= (Value*) malloc(sizeof(Value));
		int i=0;
		int dataType = schema->dataTypes[attrNum];

		while(i<schema->numAttr)
		{
			if (i==attrNum)
			{
				value1->dt= dataType;
				switch(dataType)
				{
				case DT_STRING:
					value1->v.stringV= (char*) malloc(schema->typeLength[i]+1);
					memcpy(value1->v.stringV, recOfst, schema->typeLength[i]);
					value1->v.stringV[schema->typeLength[i]]='\0';
					break;
				case DT_INT:
				case DT_FLOAT:
				case DT_BOOL:
					memcpy(&value1->v, recOfst, schema->typeLength[i]);
					break;
				default:
					assert(!"No such type");
				}
				break;
			}
			recOfst = recOfst + schema->typeLength[i];
			i++;
		}
		*value = value1;
		return RC_OK;
	}

	/*
	 * function setAttr:
	 */
	RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
	{
		void *recOfst = record->data;
		int i=0;
		int dataType = schema->dataTypes[attrNum];
		while(i<schema->numAttr)
		{
			if (i==attrNum)
			{
				switch(dataType)
				{
				case DT_STRING:
					memcpy(recOfst, value->v.stringV, schema->typeLength[i]);
					break;
				case DT_INT:
				case DT_BOOL:
				case DT_FLOAT:
					memcpy(recOfst, &value->v, schema->typeLength[i]);
					break;
				default:
					assert(!"No such type");
				}
			}
			recOfst = recOfst + schema->typeLength[i];
			i++;
		}
		return RC_OK;
	}

	/*
	 * function findFreeSlot:
	 *
	 * Finds an available free slot.
	 * Returns -1 if no free slot available.
	 */

	int findFreeSlot(RM_ScanTuple *dataPtr, Schema *schema)
	{
		int i=0;
		char *slotNo = ((char*) &dataPtr->data);
		int recSz= getRecordSize(schema)+1; //Get Record Size
		int n = REC_SZ/recSz; //Total Number of Slots

		while(i<n)
		{
			if (!(*(char*)slotNo>0)) //Get TOMBSTONE Address
				return i;
			slotNo = slotNo + recSz;
			i++;
		}
		return -1;
	}
