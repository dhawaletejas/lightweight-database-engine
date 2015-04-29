/*
 * storage_mgr.c
 *
 *  Created on: 19-Sep-2014
 *      Authors: Tejas Dhawale, Deepika Chaudhari, Vaishali Pandurangan
 */

#include <stdio.h>
#include <string.h>
#include "storage_mgr.h"
#include "dberror.h"


/* GLOBAL DECLARATION - METADATA VARIABLES */

int totNoPg;  // Total Number of Pages
int curPgPos; // Current Page Position
#define SIZE_byte (sizeof(int)) // Size of Integer - 1 Byte.
#define SIZE_FileHeader (2 * SIZE_byte) //File Header Size (totNoPg & curPgPos).
#define OFFSET_totNoPg 0 // Offset for storing/retrieving metadata totNoPg is 0.
#define OFFSET_curPgPos SIZE_byte // Offset for storing/retrieving metadata curPgPos is 1 byte (integer size).
#define OFFSET_pgFile SIZE_FileHeader // Offset for storing/retrieving file records.
// Offset to seek the START POSITION to read/write/append a page within the file.
#define OFFSET_page(pageNum) ((PAGE_SIZE * pageNum) + SIZE_FileHeader)


/* MANIPULATE PAGE FILES */

void initStorageManager(void)
{
	// Initialize File Metadata Variables
	totNoPg = 1;
	curPgPos = 0;
}

/* createPageFile() METHOD:
 *
 * Create a new Page File with the given file name 'fileName'.
 * Initial File size should be equal to 1 empty page.
 *
 * fileName: Page File's name.
 */

RC createPageFile(char *fileName)
{
	/* Initialize File Metadata Variables */
	initStorageManager();

	/* Error Handling: File Not Found */
	if (fileName == NULL)
		return RC_FILE_NOT_FOUND;

	FILE *file = NULL;
	/* Open file in write byte mode */
	file = fopen(fileName, "wb");

	/* Error Handling: File Open fails */
	if(file==NULL)
		return RC_WRITE_FAILED;

	/* File Initialization with an empty page */
	else
	{
		/* Seek Position & Store Page Count in File Header */
		int pgCnt = 1;
		fseek(file,OFFSET_totNoPg,SEEK_SET);
		fwrite(&pgCnt,SIZE_byte,1,file);

		/* Seek Position & Store Current Page Position in File Header */
		fseek(file,OFFSET_curPgPos,SEEK_SET);
		fwrite(&curPgPos,SIZE_byte,1,file);

		/* Allocate Memory worth PAGE_SIZE bytes to the empty page */
		char* emptyPage = (char*)calloc(PAGE_SIZE,sizeof(char));

		/* Seek position to write empty page (skipping file header - 2 bytes) */
		fseek(file,OFFSET_pgFile,SEEK_SET);

		/* Write the empty page to the file */
		fwrite(emptyPage,sizeof(char),PAGE_SIZE,file);

		/* Reset File Metadata Variables
		 * Close File Pointers
		 * Free Allocated Memory*/

		totNoPg = -1;
		curPgPos = -1;
		fclose(file);
		file=NULL;
		free(emptyPage);
		return RC_OK;
	}
}

/* openPageFile() METHOD:
 *
 * Open existing Page File of file 'fileName'.
 * Upon successfully opening the file, update File Handler properties
 * with the respective File attributes.
 *
 * fileName: Page File's name.
 * fHandle: File Handler of file 'fileName'.
 */

RC openPageFile (char *fileName, SM_FileHandle *fHandle)
{
	/* Error Handling: File Not Found */
	if (fileName == NULL)
		return RC_FILE_NOT_FOUND;

	/* Error Handling: File Handle Not Initialized */
	if (fHandle == NULL)
		return RC_FILE_HANDLE_NOT_INIT;

	/*Create file pointer */
	FILE *file;
	/* Opens File in Read Byte mode */
	if((file = fopen(fileName, "r+b"))==NULL)
		return RC_FILE_NOT_FOUND;
	else
	{
		/* Seek to beginning of File */
		fseek(file,OFFSET_totNoPg,SEEK_SET);
		int pgCnt=0;
		/* Read the file properties from file and write to fHandle fields.
		 * Count of total Pages is stored in the first byte (sizeof(int)) */
		fread(&pgCnt,SIZE_byte,1,file);
		fHandle->fileName=fileName;
		fHandle->totalNumPages=pgCnt;
		fHandle->curPagePos=0;
		fHandle->mgmtInfo=file;
		return RC_OK;
	}
}

/* closePageFile() METHOD:
 *
 * Close an opened Page File specified by the File Handler 'fileHandle'.
 *
 * fHandle: File Handler of file 'fileName'.
 */

RC closePageFile(SM_FileHandle *fHandle)
{
	/* Error Handling: File Not Initialized */
	if (fHandle == NULL)
		return RC_FILE_HANDLE_NOT_INIT;

	/* Close the file stream */
	if(fclose(fHandle->mgmtInfo)==0)
	{
		/* Reset File Handle properties */
		fHandle->fileName=NULL;
		fHandle->totalNumPages=0;
		fHandle->curPagePos=0;
		fHandle->mgmtInfo=NULL;
		return RC_OK;
	}
	/* Error Handling: File Stream unsuccessful in closing */
	else
		return RC_FILE_HANDLE_NOT_INIT;
}

/* destroyPageFile() METHOD:
 *
 * Delete a Page File.
 *
 */

RC destroyPageFile(char *fileName)
{
	/* Delete the file */
	if(remove(fileName)==0)
		return RC_OK;
	else
		return RC_FILE_NOT_FOUND;
}

/* -----------------------------------------------------------------*/
/* READING BLOCKS FROM DISK */

/* readBlock() METHOD:
 *
 * Read pageNum block in the opened file and store its records
 * to memory pointed by memPage (Page Handler)
 *
 * pageNum: 'pageNum' Block to be read.
 * fHandle: File Handler for 'filename' File.
 * memPage: Memory Pointer to store the read Block.
 */

RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	/* Error Handling: File Not Initialized */
	if (fHandle == NULL)
		return RC_FILE_HANDLE_NOT_INIT;
	/* Error Handling: Invalid Page*/
	if (memPage == NULL)
		return RC_READ_NON_EXISTING_PAGE;
	/* Error Handling: Requested Page Number exceeds the total Page Count */
	if (pageNum >= fHandle->totalNumPages)
		return RC_READ_NON_EXISTING_PAGE;
	/* Error Handling: Invalid Page */
	if (pageNum < 0)
		return RC_READ_NON_EXISTING_PAGE;
	/* Error Handling: File Not Found */
	if (fHandle->mgmtInfo == NULL)
		return RC_FILE_NOT_FOUND;

	FILE* file = (FILE*)fHandle->mgmtInfo;
	//Seek to beginning of required block
	if(fseek(file,OFFSET_page(pageNum),SEEK_SET)==0)
	{
		/* Read Page Data from file to the pointed (memPage) block of memory */
		fread(memPage,sizeof(char),PAGE_SIZE,file);
		/* Store Page Number to File Handle */
		fHandle->curPagePos = pageNum;
		return RC_OK;
	}
	else
		return RC_READ_NON_EXISTING_PAGE;
}

// Get the Current Page Position in file
int getBlockPos(SM_FileHandle *fHandle)
{
	//Error Handling: File Not Initialized
	if (fHandle == NULL)
		return RC_FILE_HANDLE_NOT_INIT;
	return fHandle->curPagePos;
}

// Read First Page in file
RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	//Error Handling: File Not Initialized
	if (fHandle == NULL)
		return RC_FILE_HANDLE_NOT_INIT;
	return readBlock(0,fHandle,memPage);
}

// Read Previous Page in file
RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	//Error Handling: File Not Initialized
	if (fHandle == NULL)
		return RC_FILE_HANDLE_NOT_INIT;
	return readBlock(fHandle->curPagePos-1,fHandle,memPage);
}

// Read Current Page in file
RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	//Error Handling: File Not Initialized
	if (fHandle == NULL)
		return RC_FILE_HANDLE_NOT_INIT;
	return readBlock(fHandle->curPagePos,fHandle,memPage);
}

// Read Next Page in file
RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	//Error Handling: File Not Initialized
	if (fHandle == NULL)
		return RC_FILE_HANDLE_NOT_INIT;
	return readBlock(fHandle->curPagePos+1,fHandle,memPage);
}

// Read Last Page in file
RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	//Error Handling: File Not Initialized
	if (fHandle == NULL)
		return RC_FILE_HANDLE_NOT_INIT;
	return readBlock(fHandle->totalNumPages-1,fHandle,memPage);
}

/* -----------------------------------------------------------------*/
/* WRITING BLOCKS TO A PAGE FILE */

/* writeBlock() method:
 *
 * Records present in the memory, pointed by memPage (Page Handler)
 * are written to pageNum block in opened file.
 *
 * pageNum: 'pageNum' Block to be read.
 * fHandle: File Handler for 'filename' File.
 * memPage: Memory Pointer to store the read Block.
 */

RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	/* Error Handling */
	if (fHandle == NULL)
		return RC_FILE_HANDLE_NOT_INIT;
	if (memPage == NULL)
		return RC_READ_NON_EXISTING_PAGE;
	if (fHandle->mgmtInfo == NULL)
		return RC_FILE_NOT_FOUND;

	/* Check if pageNum overflows Total Page Count
	 * Call ensureCapacity() to add 1 empty page to the file
	 */
	if (pageNum >= fHandle->totalNumPages)
		if (ensureCapacity(pageNum+1, fHandle) != RC_OK)
			return RC_WRITE_FAILED;

	FILE* file = (FILE*)fHandle->mgmtInfo;

	/* Seek to available start position to write the requested page */
	if(fseek(file,OFFSET_page(pageNum),SEEK_SET)==0)
	{
		int i;
		fwrite(memPage,sizeof(char),PAGE_SIZE,file);
		/* Increment Current Page Position by 1 as we have written a new page */
		fHandle->curPagePos = pageNum + 1;
		return RC_OK;
	}
	else return RC_WRITE_FAILED;
}

/*
 * writeCurrentBlock() method
 *
 * Write records in the memory (pointed by memPage Page Handle)
 * to the CURRENT BLOCK in the opened file.
 */

RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	/* Error Handling: File Not Initialized */
	if (fHandle == NULL)
		return RC_READ_NON_EXISTING_PAGE;
	return writeBlock(fHandle->curPagePos,fHandle, memPage);
}

/* appendEmptyBlock() method:
 *
 * Write an empty block of PAGE_SIZE size to the end of the file.
 * Update File Handle property 'currPagePos' to point to
 * the latest empty block appended.
 *
 */

RC appendEmptyBlock (SM_FileHandle *fHandle)
{
	/* Error Handling */
	if (fHandle == NULL)
		return RC_FILE_HANDLE_NOT_INIT;
	if (fHandle->mgmtInfo == NULL)
		return RC_FILE_NOT_FOUND;

	int pgNo = fHandle->totalNumPages;

	/* Allocate Memory worth PAGE_SIZE bytes to the empty page */
	char* memPage=(char*)calloc(PAGE_SIZE,sizeof(char));

	FILE *file = (FILE*)fHandle->mgmtInfo;

	/* Seek position to the end of file and Write empty block */
	fseek(file, 0, SEEK_END);
	fwrite(memPage,sizeof(char),PAGE_SIZE,file);

	/* curPagePos now points to the recently appended empty block  */
	fHandle->curPagePos = pgNo;

	/* Increment Page Count & update it to the File Header */
	fHandle->totalNumPages++;
	fseek(file,OFFSET_totNoPg,SEEK_SET);
	int noPg = fHandle->totalNumPages;
	fwrite(&noPg, sizeof(int), 1, file);

	/* Free allocated memory */
	free(memPage);
	memPage=NULL;
	return RC_OK;
}

/*
 * ensureCapacity() method:
 *
 * If the file falls short of 'numberOfPages' pages,
 * then, file is expanded by 'numberOfPages' pages by
 * iteratively calling appendEmptyBlock() method.
 */

RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
{
	//Error Handling: File Not Initialized
	if (fHandle == NULL)
		return RC_FILE_HANDLE_NOT_INIT;

	//Append Empty Block if Total No. of Pages is less
	while(fHandle->totalNumPages < numberOfPages)
	{
		if (appendEmptyBlock(fHandle) != RC_OK)
			return RC_WRITE_FAILED;
	}
	return RC_OK;
}
