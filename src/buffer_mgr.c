/*
 * buffer_mgr.c
 *
 *  Created on: 21-Oct-2014
 *      Author: Tejas Dhawale, Deepika Chaudhari, Vaishali Pandurangan
 *
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>

#include "storage_mgr.h"
#include "buffer_mgr.h"

#define SIZE_byte (sizeof(char)) // Size 1 Byte.


/*
 * Structure: Frame -
 * Defines the structure of each individual frame (node) in a Buffer Pool (doubly linked list).
 *
 * dirtyBit: TRUE if changes have been made to the page stored in this frame.
 * fixBit: Number of Clients accessing the frame.
 * next: Pointer to the next frame (node).
 * prev: Pointer to the previous frame (node).
 * page: Page Handler for the page stored in the frame.
 * seq: Sequencer Variable - Used while reading Buffer Pool Management Data (displaying purposes).
 * refBit: Clock Page Replacement Algorithm - Reference Bit.
 */
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


/*
 * Structure: BM_MgmtData -
 * Stores additional mgmtInfo data:
 *
 * frameContents: Array to store pageNum values for all frames in the buffer pool.
 * dirtyFlags: Array to store dirtyBit flags for all frames in the buffer pool.
 * fixCounts: Array to store fixBit values for all frames in the buffer pool.
 * refBits: Array to store refBit values for all frames in the buffer pool.
 * numReadIO: stores total number of reads done on BufferPool.
 * numWriteIO: stores total number of writes done on BufferPool.
 * clkPtr: Tracks the frame to which the clock is pointing at.
 * head: stores head of the doubly linked list (buffer pool).
 * tail: stores tail of the doubly linked list (buffer pool).
 * fHandle: File Handler for the file to be read into the buffer.
 */
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


/*
 * Function activateFrames:
 *
 * Creates and places 'numPages' number of frames (client specified) into the Buffer Pool.
 *
 * frame: Memory Allocated frame (node) to be inserted into the Buffer Pool (Doubly Linked List).
 * bm: Buffer Manager
 * i: Sequence Number of the Frame
 */
RC activateFrames(Frame* frame, BM_BufferPool *const bm, int i)
{
	if(frame==NULL)
		return RC_BM_NULL_FRAME;
	if(bm==NULL)
		return RC_BM_NULL_BUFFER;

	//Initialize Frames
	frame->dirtyBit = FALSE;
	frame->fixBit = 0;
	frame->next = NULL;
	frame->prev = NULL;
	frame->page.pageNum = -1;
	frame->seq = i;
	frame->refBit = 0;
	frame->page.data = (char*)malloc(SIZE_byte * PAGE_SIZE);
	memset(frame->page.data, 0, SIZE_byte * PAGE_SIZE);

	//Backing up Head Frame from BufferPool.
	Frame* head = ((BM_MgmtData*)bm->mgmtData)->head;

	//Placing Subsequent Frames in BufferPool.
	if(head!=NULL && i!=0)
	{
		((BM_MgmtData*)bm->mgmtData)->head = frame; // New Head
		frame->next = head;
		head->prev = frame;
	}
	//Placing First Frame in BufferPool as both Head & Tail.
	else
	{
		((BM_MgmtData*)bm->mgmtData)->head = frame;
		((BM_MgmtData*)bm->mgmtData)->tail = frame;
	}
	return RC_OK;
}

/*
 * Function initBufferPool:
 *
 * Initializes the Buffer Pool for the client-specified Page File using the mentioned Replacement Strategy.
 */

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
		  const int numPages, ReplacementStrategy strategy,
		  void *stratData)
{
	if(pageFileName==NULL)
		return RC_BM_NULL_PGFILE;
	if(bm==NULL)
		return RC_BM_NULL_BUFFER;

	//Populate BM_BufferPool structure's contents, using arguments provided by Client.
	bm->pageFile=(char*)pageFileName;
	bm->numPages=numPages;
	bm->strategy=strategy;

	//Memory Allocation for BM_MgmtData.
	BM_MgmtData* md =(BM_MgmtData*)malloc(sizeof(BM_MgmtData));

	//Store BM_MgmtData within mgmtData of BM_BufferPool
	bm->mgmtData=md;

	//Reset/Initialize BM_MgmtData statistics variables.
	md->numReadIO=0;
	md->numWriteIO=0;
	md->frameContents=(PageNumber*)malloc(sizeof(PageNumber)*numPages);
	md->dirtyFlags=(bool*)malloc(sizeof(bool)*numPages);
	md->fixCounts=(int*)malloc(sizeof(int)*numPages);
	md->refBits=(int*)malloc(sizeof(int)*numPages);

	//Open Client's Page File
	openPageFile(bm->pageFile,&md->fHandle);

	//Create Doubly Linked List with numPages Nodes.
	Frame* frame;
	int i=0;
	while(i<numPages)
	{
		frame=(Frame*)malloc(sizeof(Frame));
		activateFrames(frame,bm,i);
		i++;
	}
	//Convert to Circular Linked List if Clock Replacement Algorithm requested.
	if(bm->strategy == RS_CLOCK)
	{
		((BM_MgmtData*)bm->mgmtData)->clkPtr = md->head;
		md->tail->next = md->head;
		md->head->prev = md->tail;
	}
	return RC_OK;
}

/*
 * Function shutdownBufferPool:
 *
 * Shuts Down the client-specified Buffer Pool after flushing all dirty pages.
 */

RC shutdownBufferPool(BM_BufferPool *const bm)
{
	if(bm==NULL)
		return RC_BM_NULL_BUFFER;

	//Read BufferPool's mgmtData into BM_MgmtData.
	BM_MgmtData* md = (BM_MgmtData*)bm->mgmtData;
	int pgCnt = bm->numPages;

	//Retrieve Head Node(Frame) of the BufferPool.
	Frame* frame=(Frame*)md->head;

	//Check DirtyBit & FixCountBit of all Frames before ShutDown.
	int i;
	for(i=0;i<pgCnt;i++)
	{
		if(frame->fixBit!=0)
					return RC_WRITE_FAILED; //Cannot Shutdown Buffer Pool while a client is still accessing a page.
		else if(frame->dirtyBit==TRUE)
			forceFlushPool(bm); //Write Frame Contents to Disk if the Page was Dirty.
		frame = frame->next;
	}

	//Free BufferPool Memory.
	frame=(Frame*)md->head;
	for(i=0;i<pgCnt;i++)
	{
		free(frame);
		frame = frame->next;
	}
    free(md->frameContents);
    free(md->dirtyFlags);
    free(md->fixCounts);
    free(md->refBits);
    free(md);
    md=NULL;
    return RC_OK;
}

/*
 * Function forceFlushPool:
 *
 * Write Buffer Pool's dirty flagged pages to disk.
 */

RC forceFlushPool(BM_BufferPool *const bm)
{
	if(bm==NULL)
		return RC_BM_NULL_BUFFER;

	//Read BufferPool's mgmtData into BM_MgmtData.
	BM_MgmtData* md = (BM_MgmtData*)bm->mgmtData;
	int pgCnt = bm->numPages;

	//Retrieve Head Node(Frame) of the BufferPool.
	Frame* frame=(Frame*)md->head;
	int i = 0;
	while(i<pgCnt)
	{
		if(frame->dirtyBit && frame->fixBit==0)
		{
			writeBlock(frame->page.pageNum, &md->fHandle, (SM_PageHandle)frame->page.data);
			frame->dirtyBit=FALSE;
		}
		frame = frame->next;
		i++;
	}
	return RC_OK;
}

/*
 * Function markDirty:
 *
 * Marks the Client-specified page within bm BufferPool as Dirty.
 */

RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	if(page==NULL)
		return RC_BM_NULL_PAGE;
	if(bm==NULL)
		return RC_BM_NULL_BUFFER;

	//Read BufferPool's mgmtData into BM_MgmtData.
	BM_MgmtData* md = (BM_MgmtData*)bm->mgmtData;
	int pgCnt = bm->numPages;

	//Retrieve Head Node(Frame) of the BufferPool.
	Frame* frame=(Frame*)md->head;

	int i;
	for(i=0;i<pgCnt;i++)
	{
		if(frame->page.pageNum==page->pageNum) //Search for the required page in the BufferPool
		{
			frame->dirtyBit=TRUE;
			break;
		}
		frame = frame->next;
	}
	return RC_OK;
}

/*
 * Function unpinPage:
 *
 * UnPins a client-specified page from the Buffer Pool.
 */

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	if(page==NULL)
		return RC_BM_NULL_PAGE;
	if(bm==NULL)
		return RC_BM_NULL_BUFFER;

	//Read BufferPool's mgmtData into BM_MgmtData.
	BM_MgmtData* md = (BM_MgmtData*)bm->mgmtData;
	int pgCnt = bm->numPages;

	//Retrieve Head Node(Frame) of the BufferPool.
	Frame* frame=(Frame*)md->head;

	int i;
	for(i=0;i<pgCnt;i++)
	{
		if(frame->page.pageNum==page->pageNum)
		{
			frame->fixBit = frame->fixBit-1; //Decrement fixBit by 1
			if(frame->fixBit<0)
				return RC_WRITE_FAILED; //Cannot UnPin a page which is not pinned in any frame of the Buffer Pool.

			// Write Page to Disk if it is Dirty
			if(frame->dirtyBit==TRUE)
			{
				// Overwrite with Latest page data given by Client.
				frame->page.data = page->data;
				forcePage(bm, page); // Write Dirty Page Data to Disk.
			}
			break;
		}
		frame = frame->next;
	}
	return RC_OK;
}

/*
 * Function forcePage:
 *
 * Writes the specified page to Disk and increments the BufferManager Statistics numWriteIO by 1.
 */

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	if(page==NULL)
		return RC_BM_NULL_PAGE;
	if(bm==NULL)
		return RC_BM_NULL_BUFFER;

	//Read BufferPool's mgmtData into BM_MgmtData.
	BM_MgmtData* md = (BM_MgmtData*)bm->mgmtData;

	writeBlock(page->pageNum, &md->fHandle, (SM_PageHandle)page->data);
	md->numWriteIO = md->numWriteIO+1;
	return RC_OK;
}


/*
 * Function pinPage:
 *
 * Pins the client-specified page to the Buffer Pool.
 * If the page is already found in buffer, it is simply pinned.
 * Else, the page is read from Disk and pinned to the buffer frame.
 *
 * Uses the client specified Page Replacement Algorithm while replacing or pinning pages.
 */

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,
	    const PageNumber pageNum)
{
	if(page==NULL)
		return RC_BM_NULL_PAGE;
	if(bm==NULL)
		return RC_BM_NULL_BUFFER;

	//Read BufferPool's mgmtData into BM_MgmtData.

	BM_MgmtData* md = (BM_MgmtData*)bm->mgmtData;
	int pgCnt = bm->numPages;

	//Retrieve Head Node(Frame) of the BufferPool.
	Frame* frame=(Frame*)md->head;
	Frame* oldHead = (Frame*)md->head;

	/*---------------------------------------------------------------------
	 * Attempting to find Page in Buffer:
	 */
	int i;
	for(i=0;i<pgCnt;i++)
	{
		if(frame->page.pageNum==pageNum)
		{
			frame->fixBit = frame->fixBit + 1;
			frame->refBit = 1;
			page->pageNum = frame->page.pageNum;
			page->data = frame->page.data;

			//Following Process for LRU Replacement Strategy:
			if(bm->strategy == RS_LRU)
			{
				oldHead = (Frame*)md->head;
				if(frame == (Frame*)md->tail)
				{
					frame->prev->next = NULL;
					((BM_MgmtData*)bm->mgmtData)->tail = frame->prev;
					frame->prev = NULL;
				}
				else
				{
					frame->prev->next = frame->next;
					frame->next->prev = frame->prev;
				}
				((BM_MgmtData*)bm->mgmtData)->head = frame;
				frame->next = oldHead;
				oldHead->prev = frame;
			}
			else if(bm->strategy == RS_CLOCK)
			{
				md->clkPtr=frame->next;
			}
			return RC_OK;
		}
		frame = frame->next;
	}

	/*---------------------------------------------------------------------
	// Using Page Replacement if Page not already in BufferPool.
	// Read the page from disk and load into memory.
	*/

	// FIFO or LRU Page Replacement Implementation:
	if(bm->strategy == RS_FIFO || bm->strategy == RS_LRU )
	{
		//Search for a frame from the tail-end whose fixBit is Zero (0) and read the page into this frame.
		frame = (Frame*)md->tail;
		while(frame->prev!=NULL)
		{
			if(frame->fixBit == 0)
				break;
			frame = frame->prev;
		}
		readBlock(pageNum,&md->fHandle,(SM_PageHandle)frame->page.data);

		frame->fixBit = frame->fixBit + 1; //Increment fixBit
		md->numReadIO = md->numReadIO + 1; //Increment BufferManager Statistics numReadIO

		//Re-organizing Linked List - Place the newly pinned Page as the Head of the Linked List.
		oldHead = (Frame*)md->head;

		if(frame == (Frame*)md->tail) //If page was replaced at Tail of the Linked List.
		{
			frame->prev->next = NULL;
			((BM_MgmtData*)bm->mgmtData)->tail = frame->prev;
			frame->prev = NULL;
		}
		else //If page was replaced at an intermediate Node of the Linked List.
		{
			frame->prev->next = frame->next;
			frame->next->prev = frame->prev;
		}
		((BM_MgmtData*)bm->mgmtData)->head = frame; // Make this newly pinned page the new Head.
		frame->next = oldHead; // Old Head should be pointed as the next of the new Head.
		oldHead->prev = frame; // Old Head's prev pointer should point to the new Head.
	}

	// Clock Replacement Algorithm Implementation:
	else if(bm->strategy == RS_CLOCK)
	{
		frame = (Frame*)md->clkPtr; // Start Searching for a frame beginning from the clkPtr Position.
		for(i=0;i<pgCnt;i++)
		{
			if(frame->fixBit == 0 && frame->refBit == 0) // Check for a frame whose fixBit and refBit are both 0.
				break;
			frame->refBit=0; // Continue Resetting refBit as the clkPtr progresses.
			frame = frame->next;
		}
		md->clkPtr=frame->next; //clkPtr should point to the next of the replaced page frame.
		readBlock(pageNum,&md->fHandle,(SM_PageHandle)frame->page.data); //Read the page from disk to the designated frame.
		frame->fixBit = frame->fixBit + 1; // Increment the fixBit of the frame which is used to replace the page by 1.
		frame->refBit = 1; // Set refBit to 1 for the frame which is used to replace a Page.
		md->numReadIO = md->numReadIO + 1; //Increment the BufferManager statistics numReadIO by 1.
	}

	frame->page.pageNum = pageNum;
	page->pageNum=pageNum;
	page->data=frame->page.data;

	return RC_OK;
}


/*
 * Function getFrameContents
 *
 * Returns an array of Page Numbers that are comprised in the client-specified Buffer Manager.
 */

PageNumber *getFrameContents (BM_BufferPool *const bm)
{
	if(bm==NULL)
		return RC_BM_NULL_BUFFER;

	//Read BufferPool's mgmtData into BM_MgmtData.
	BM_MgmtData* md = (BM_MgmtData*)bm->mgmtData;
	int pgCnt = bm->numPages;

	//Retrieve Head Node(Frame) of the BufferPool.
	Frame* frame=(Frame*)md->head;

	PageNumber* data = ((BM_MgmtData*)bm->mgmtData)->frameContents;
	if(data!=NULL && (bm->strategy==RS_FIFO || bm->strategy==RS_LRU))
	{
		int i,j;
		for(i=0;i<pgCnt;i++)
		{
			for(j=0;j<pgCnt;j++)
			{
				if(frame->seq == i)
					data[i] = frame->page.pageNum;
				frame = frame->next;
			}
			frame=(Frame*)md->head;
		}
	}
	else if(data!=NULL && bm->strategy==RS_CLOCK)
	{
		int i;
		for(i=0;i<pgCnt;i++)
		{
			data[i]=frame->page.pageNum;
			frame=frame->next;
		}
	}
	return data;
}


/*
 * Function getDirtyFlags
 *
 * Returns an array of individual page Dirty Flags that are comprised in the client-specified Buffer Manager.
 */

bool *getDirtyFlags (BM_BufferPool *const bm)
{
	if(bm==NULL)
		return RC_BM_NULL_BUFFER;

	//Read BufferPool's mgmtData into BM_MgmtData.
	BM_MgmtData* md = (BM_MgmtData*)bm->mgmtData;
	int pgCnt = bm->numPages;

	//Retrieve Head Node(Frame) of the BufferPool.
	Frame* frame=(Frame*)md->head;

	bool* dirtyBits = ((BM_MgmtData*)bm->mgmtData)->dirtyFlags;
	if(dirtyBits!=NULL && (bm->strategy==RS_FIFO || bm->strategy==RS_LRU))
	{
		int i,j;
		for(i=0;i<pgCnt;i++)
		{
			for(j=0;j<pgCnt;j++)
			{
				if(frame->seq == i)
					dirtyBits[i] = frame->dirtyBit;
				frame = frame->next;
			}
			frame=(Frame*)md->head;
		}
	}
	else if(dirtyBits!=NULL && bm->strategy==RS_CLOCK)
		{
			int i;
			for(i=0;i<pgCnt;i++)
			{
				dirtyBits[i]=frame->dirtyBit;
				frame=frame->next;
			}
		}
	return dirtyBits;
}


/*
 * Function getFixCounts
 *
 * Returns an array of individual page Fix Counts that are comprised in the client-specified Buffer Manager.
 */

int *getFixCounts (BM_BufferPool *const bm)
{
	if(bm==NULL)
		return RC_BM_NULL_BUFFER;

	//Read BufferPool's mgmtData into BM_MgmtData.
	BM_MgmtData* md = (BM_MgmtData*)bm->mgmtData;
	int pgCnt = bm->numPages;

	//Retrieve Head Node(Frame) of the BufferPool.
	Frame* frame=(Frame*)md->head;

	int* fixCnts = ((BM_MgmtData*)bm->mgmtData)->fixCounts;
	if(fixCnts!=NULL && (bm->strategy==RS_FIFO || bm->strategy==RS_LRU))
	{
		int i,j;
		for(i=0;i<pgCnt;i++)
		{
			for(j=0;j<pgCnt;j++)
			{
				if(frame->seq == i)
					fixCnts[i] = frame->fixBit;
				frame = frame->next;
			}
			frame=(Frame*)md->head;
		}
	}
	else if(fixCnts!=NULL && bm->strategy==RS_CLOCK)
		{
			int i;
			for(i=0;i<pgCnt;i++)
			{
				fixCnts[i]=frame->fixBit;
				frame=frame->next;
			}
		}
	return fixCnts;
}


/*
 * Function getRefBits
 *
 * ONLY FOR CLOCK - PAGE REPLACEMENT ALGORITHM
 * Returns an array of individual page Reference Bits that are comprised in the client-specified Buffer Manager.
 */

int *getRefBits (BM_BufferPool *const bm)
{
	if(bm==NULL)
		return RC_BM_NULL_BUFFER;

	//Read BufferPool's mgmtData into BM_MgmtData.
	BM_MgmtData* md = (BM_MgmtData*)bm->mgmtData;
	int pgCnt = bm->numPages;

	//Retrieve Head Node(Frame) of the BufferPool.
	Frame* frame=(Frame*)md->head;

	int* refBits = ((BM_MgmtData*)bm->mgmtData)->refBits;
	if(refBits!=NULL)
		{
			int i;
			for(i=0;i<pgCnt;i++)
			{
				refBits[i]=frame->refBit;
				frame=frame->next;
			}
		}
	return refBits;
}

/*
 * Function getNumReadIO:
 *
 * Returns the count of total number of Disk Read operations performed by the Buffer Manager
 */

int getNumReadIO (BM_BufferPool *const bm)
{
	if(bm==NULL)
		return RC_BM_NULL_BUFFER;
	else
		return ((BM_MgmtData*)bm->mgmtData)->numReadIO;
}


/*
 * Function getNumWriteIO:
 *
 * Returns the count of total number of Disk Write operations performed by the Buffer Manager
 */

int getNumWriteIO (BM_BufferPool *const bm)
{
	if(bm==NULL)
		return RC_BM_NULL_BUFFER;
	else
		return ((BM_MgmtData*)bm->mgmtData)->numWriteIO;
}
