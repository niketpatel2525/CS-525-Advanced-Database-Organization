#include "buffer_mgr.h"
#include "dberror.h"
#include "storage_mgr.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_NO_PAGES 20000
#define MAX_NO_FRAMES 200
#define MAX_K 10


/* Data Structure for frame node consisting one page per frame of a buffer pool*/
typedef struct frameNode{
    int pageNum;            /* Page number of NODE in page File*/
    int frameNum;           /* Frame Number of fram in frame List*/
    int dirty;              /* dirty = 1 or 0*/
    int fixCount;           /* By how many client page node is pinned.*/
    int rf;                 /* reference bit for node with 0 or 1 for clock algorithm.*/
    int pageFrequency;      /* frequency count of node per client request for LFU*/
    char *data;             /* Page data pointer.*/
    struct frameNode *next;
    struct frameNode *previous;
}frameNode;
/* framelist with a pointer to head and tail node.*/
typedef struct frameList{
    frameNode *head;    /* head node of the frame list */
    frameNode *tail;    /* tail node of the frame list*/
}frameList;


/* Buffer Pool information data structure. attached to BM_BufferPool->mgmtData*/
typedef struct BMInfo{

    int numFrames;          /* shows howmany number of frames used in the frame list */
    int numRead;            /* total number of read done in buffer pool */
    int numWrite;           /* total number of write done in buffer pool */
    int countPinning;       /* total number of pinning done for the bm */
    void *startData;
    int pageToFrame[MAX_NO_PAGES];         /* Mapping array for pageNumber to frameNumber.*/
    int frameToPage[MAX_NO_FRAMES];        /* Mapping array for frameNumber to pageNumber.*/
    int pageToFrequency[MAX_NO_PAGES];     /* This array maintain frequency for every page.*/
    bool dirtyFlags[MAX_NO_FRAMES];        /* dirtyflags mapping for all the frames.*/
    int fixedCounts[MAX_NO_FRAMES];        /* fixed count Mapping for all the frames.*/
    int pageHistory[MAX_NO_PAGES][MAX_K];        /* history for reference of each page in memory*/
    frameList *frames;      /* a pointer to the frame list*/
}BMInfo;

/*new node creation of frame list*/
frameNode *newNodeCreation(){

    frameNode *node = malloc(sizeof(frameNode));
    node->pageNum = NO_PAGE;
    node->frameNum = 0;
    node->dirty = 0;
    node->fixCount = 0;
    node->rf = 0;
    node->data =  calloc(PAGE_SIZE, sizeof(SM_PageHandle));
    node->next = NULL;
    node->previous = NULL;
	node->pageFrequency = 0;

    return node;
}
/* Below function will change the Head Node of List.*/
void changeHeadNode(frameList **list, frameNode *changeNode){

    frameNode *head = (*list)->head;

    if(changeNode == (*list)->head || head == NULL || changeNode == NULL){
        return;
    }
    else if(changeNode == (*list)->tail){
        frameNode *temp = ((*list)->tail)->previous;
        temp->next = NULL;
        (*list)->tail = temp;
    }
    else{
        changeNode->previous->next = changeNode->next;
        changeNode->next->previous = changeNode->previous;
    }

    changeNode->next = head;
    head->previous = changeNode;
    changeNode->previous = NULL;

    (*list)->head = changeNode;
    (*list)->head->previous = NULL;
    return;
}

RC updateNextNewFrame(BM_BufferPool *const bm, frameNode *lookUp, BM_PageHandle *const page, const PageNumber pageNum){

    SM_FileHandle fh;
    BMInfo *info = (BMInfo *)bm->mgmtData;
    RC status;

    // check page file availability in disk
    status = openPageFile ((char *)(bm->pageFile), &fh);
    if ( status != RC_OK){
        return status;
    }

    // check the dirty bit of frame and if it is set, write data back to disk.
    if(lookUp->dirty == 1){
            status = ensureCapacity(pageNum, &fh);
        if(status != RC_OK){
            return status;
        }
        status = writeBlock(lookUp->pageNum,&fh, lookUp->data);
        if(status != RC_OK){
            return status;
        }
        (info->numWrite)++;
        //printf("in updateNextNewFrame dirty done..\n");
    }
	// set  value of the page which is going to be replaced by new page to NO_PAGE
    (info->pageToFrame)[lookUp->pageNum] = NO_PAGE;

	// Read the data from page for new Frame  from disk.
    //printf("pageNUm=%d\n",pageNum);
    status = ensureCapacity(pageNum, &fh);
    if(status != RC_OK){
    	//printf("in updateNextNewFrame ensureCapacity done..\n");
        return status;
    }
    status = readBlock(pageNum, &fh, lookUp->data);
    if(status != RC_OK){
    	//printf("in updateNextNewFrame readBlock done..\n");
        return status;
    }
	// set the page handler with new page data and details
    page->pageNum = pageNum;
    page->data = lookUp->data;
    //printf("\nPage->data = %s\n",page->data);
    (info->numRead)++;

	// set parameters of new page as it is recently being read from disk.
    lookUp->dirty = 0;
    lookUp->fixCount = 1;
    lookUp->pageNum = pageNum;
    lookUp->rf = 1;

    (info->pageToFrame)[lookUp->pageNum] = lookUp->frameNum;
    (info->frameToPage)[lookUp->frameNum] = lookUp->pageNum;

    status = closePageFile(&fh);

    return status;

}
/* Below function will search the node in memory by pageNum */
frameNode *searchNodeByPageNum(frameList *list, const PageNumber pageNum){

    frameNode *currentNode = list->head;

    while(currentNode != NULL){
        if(currentNode->pageNum == pageNum){
            return currentNode;
        }
        currentNode = currentNode->next;
    }

    return NULL;
}

/*Below Function will search the page in Memory by page Number.*/
frameNode *searchPageInMemory(BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum){

    frameNode *lookUp;
    BMInfo *info = (BMInfo *)bm->mgmtData;

    if((info->pageToFrame)[pageNum] != NO_PAGE){
        if((lookUp = searchNodeByPageNum(info->frames, pageNum)) == NULL){
            return NULL;
        }

        /* return data and details of page*/
        page->pageNum = pageNum;
        page->data = lookUp->data;

        lookUp->fixCount++;
        lookUp->rf = 1;

        return lookUp;
    }
    return NULL;
}



/* Page replacement strategies.*/

RC pinPageUsingFIFOStrategy (BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum)
{
    frameNode *lookUp;
    BMInfo *info = (BMInfo *)bm->mgmtData;

	// Check the page availability in memory
	lookUp = searchPageInMemory(bm, page, pageNum);
	    if(lookUp != NULL){
		//printf("lookUp ok %d",lookUp->pageNum);
	    	return RC_OK;
	    }


	// If lookup pointer failed to load page from memory

	// if there is space available to insert new page in frame, then add page to first free frame.
    	if((info->numFrames) < bm->numPages){
        lookUp = info->frames->head;
        int i = 0;

        while(i < info->numFrames){
            lookUp = lookUp->next;
            ++i;
        }

	//After adding page to frame, increase frame size by 1.
        (info->numFrames)++;
        changeHeadNode(&(info->frames), lookUp);
        //printf("in IF pinPageUsingFIFOStrategy\n");
    }
    else{

	// if all the frames are filled out, replace frame which is come first in memory as per FIFO strategy.
        lookUp = info->frames->tail;

        while(lookUp != NULL && lookUp->fixCount != 0){
            lookUp = lookUp->previous;
        }

        if (lookUp == NULL){
            return RC_NO_MORE_SPACE_IN_BUFFER;
        }

        changeHeadNode(&(info->frames), lookUp);
    }

    // Update new page to frame
    RC status =updateNextNewFrame(bm, lookUp, page, pageNum);
    if(status  != RC_OK){
        return status;
    }else
		return RC_OK;
}

RC pinPageUsingLRUStrategy (BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum)
{
    frameNode *lookUp;
    BMInfo *info = (BMInfo *)bm->mgmtData;

	// Check the page availability in memory
	lookUp = searchPageInMemory(bm, page, pageNum);
    if(lookUp != NULL){

	//lookup points to the page which is accessed, so put the currently accessed page to the first frame as per LRU strategy.
        changeHeadNode(&(info->frames), lookUp);
        return RC_OK;
    }

	// If lookup pointer failed to load page from memory

	// if there is space available to insert new page in frame, then add page to first free frame.
    	if((info->numFrames) < bm->numPages){
        lookUp = info->frames->head;

        int i = 0;
        while(i < info->numFrames){
            lookUp = lookUp->next;
            ++i;
        }
	//After adding page to frame, increase frame size by 1.
        (info->numFrames)++;
    }
    else{
	// if all the frames are filled out, then replace a frame which is least recently used as per LRU strategy.
        lookUp = info->frames->tail;

        while(lookUp != NULL && lookUp->fixCount != 0){
            lookUp = lookUp->previous;
        }

	// if lookup failed to find a frame which is least recently used then threre is no more space left in buffer.
        if (lookUp == NULL){
            return RC_NO_MORE_SPACE_IN_BUFFER;
        }
    }

    //  put the recently used frame to the first frame as per LRU strategy.
    changeHeadNode(&(info->frames), lookUp);

    RC status = updateNextNewFrame(bm, lookUp, page, pageNum);

    if(status != RC_OK)
        return status;
    else
		return RC_OK;
}

RC pinPageUsingLRU_KStrategy (BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum)
{
    frameNode *lookUp;
    BMInfo *info = (BMInfo *)bm->mgmtData;
    int K = (int)(info->startData);
    int i;
    (info->countPinning)++;

    // Check the page availability in memory
    lookUp = searchPageInMemory(bm, page, pageNum);
    if(lookUp != NULL){

        for(i = K-1; i>0; i--){
            info->pageHistory[lookUp->pageNum][i] = info->pageHistory[lookUp->pageNum][i-1];
        }

        info->pageHistory[lookUp->pageNum][0] = info->countPinning;

        return RC_OK;
    }
    // If lookup pointer failed to load page from memory

    // if there is space available to insert new page in frame, then add page to first free frame.
    if((info->numFrames) < bm->numPages){
        lookUp = info->frames->head;

        int i = 0;
        while(i < info->numFrames){
            lookUp = lookUp->next;
            ++i;
        }

	//After adding page to frame, increase frame size by 1.
        (info->numFrames)++;
    }
    else{
	// if all the frames are filled out, then replace a frame whose backward K-distance is maximum of all pages in buffer as per LRU strategy.
        frameNode *currentNode;
        int dist, max_dist = -1;

        currentNode = info->frames->head;

        while(currentNode != NULL){
            if(currentNode->fixCount == 0 && info->pageHistory[currentNode->pageNum][K] != -1){

                dist = info->countPinning - info->pageHistory[currentNode->pageNum][K];

                if(dist > max_dist){
                    max_dist = dist;
                    lookUp = currentNode;
                }
            }
            currentNode = currentNode->next;
        }

	// if reached to end , it means no frame with fixed count 0 is available.
        if(max_dist == -1){
            currentNode = info->frames->head;

            while(currentNode->fixCount != 0 && currentNode != NULL){
                dist = info->countPinning - info->pageHistory[currentNode->pageNum][0];
                if(dist > max_dist){
                    max_dist = dist;
                    lookUp = currentNode;
                }
                currentNode = currentNode->next;
            }

	    // if max_dist is -1 then no frame with fixed count 0 is available.
            if (max_dist == -1){
                return RC_NO_MORE_SPACE_IN_BUFFER;
            }
        }
    }

    RC status = updateNextNewFrame(bm, lookUp, page, pageNum);

    if(status  != RC_OK){
        return status;
    }

    for(i = K-1; i>0; i--){
        info->pageHistory[lookUp->pageNum][i] = info->pageHistory[lookUp->pageNum][i-1];
    }
    info->pageHistory[lookUp->pageNum][0] = info->countPinning;

    return RC_OK;
}



RC pinPageUsingCLOCKStrategy (BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum)
{
	int j;
    frameNode *lookUp;
    BMInfo *info = (BMInfo *)bm->mgmtData;

    //Search page in memory.
    if((lookUp = searchPageInMemory(bm, page, pageNum)) != NULL){
    	//printf("if hi=%d",lookUp->pageNum);
    	page->i=page->i-1;
    	//printf("Hiii page->i=%d",page->i);
        return RC_OK;
    }

    // if Page is not in memory then do below.
    else{
        frameNode *S = info->frames->head;
        //printf("s->rf = %d 		s->pageNum=%d    page->i=%d\n"
        		//,S->rf,bm->numPages,page->i);


        	for(j=0; j<(page->i)%bm->numPages; j++){
        		if(S->fixCount==0)
        		S->rf=0;
        		S = S->next;
        		//printf("%d < %d",j,page->i%bm->numPages);
        		//printf("S->fixcount=%d\n",S->fixCount);
        		if(S->fixCount==0)
        			//S=S->next;
        			S->rf=0;
        			//if(S->fixCount
        		if(page->i%bm->numPages == 1 && S->fixCount==1)
        		{
        			S=S->next;
        			S->rf=0;
        			//printf("S-rf=%d",S->rf);
        		}

        	}

        	while (S != NULL && S->rf == 1){
        	            S->rf = 0;
        	            S = S->next;
        	}



        if (S == NULL){
            return RC_NO_MORE_SPACE_IN_BUFFER;
        }

        lookUp = S;
    }

    RC status;

    /*call updateNewFrame with the new value of lookUp*/
    if((status = updateNextNewFrame(bm, lookUp, page, pageNum)) != RC_OK){
        return status;
    }

    return RC_OK;
}

RC pinPageUsingLFUStrategy (BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum)
{
     frameNode *lookUp;
     BMInfo *info = (BMInfo *)bm->mgmtData;
     page->data = (SM_PageHandle) malloc(PAGE_SIZE);

     // Check the page availability in memory
     if((info->pageToFrame)[pageNum] != -1){
        lookUp = searchNodeByPageNum(info->frames, pageNum);

	//provide the available page details and its data.
        page->pageNum = pageNum;
        page->data = lookUp->data;

	// increase its fix count value to indicate it is frequently used.
        lookUp->fixCount++;
        lookUp->pageFrequency++;
        lookUp->rf = 1;

        return RC_OK;
    }
	// If lookup pointer failed to load page from memory
	// if there is space available to insert new page in frame, then add page to first free frame.

    if((info->numFrames) < bm->numPages){
        lookUp = info->frames->head;

        int i = 0;
        while(i < info->numFrames){
            lookUp = lookUp->next;
            ++i;
        }

        (info->numFrames)++;
                 changeHeadNode(&(info->frames), lookUp);
    }


	else{
	  // if all the frames are filled out, then replace a frame whose freque used counter is low as per LFU strategy.
          lookUp = info->frames->tail;

          while(lookUp != NULL && lookUp->previous->pageFrequency < lookUp->pageFrequency) {

                 lookUp = lookUp->previous;
          }

          while(lookUp->fixCount != 0 && lookUp != NULL){
                        lookUp = lookUp->previous;
          }

          if (lookUp == NULL){
                        return RC_NO_MORE_SPACE_IN_BUFFER;
          }

          changeHeadNode(&(info->frames), lookUp);

    }

	SM_FileHandle fh;
    RC status =openPageFile (bm->pageFile, &fh);

    if (status != RC_OK){
        return RC_FILE_NOT_FOUND;
    }
   // If the frame is dirty then write it back to the disk.
    if(lookUp->dirty == 1){
        writeBlock(lookUp->pageNum,&fh, lookUp->data);
        (info->numWrite)++;
    }

    (info->pageToFrame)[lookUp->pageNum] = NO_PAGE;

    // Read the data into new frame.
    lookUp->data = (SM_PageHandle) malloc(PAGE_SIZE);
    ensureCapacity(pageNum, &fh);
    readBlock(pageNum, &fh, lookUp->data);

    // provide the client with the data and details of page
    page->pageNum = pageNum;
    page->data = lookUp->data;

    (info->numRead)++;


    // Set all the parameters of the new frame, and update the lookup arrays.
    lookUp->dirty = 0;
    lookUp->fixCount = 1;
    lookUp->pageNum = pageNum;
    lookUp->rf = 1;
    lookUp->pageFrequency = 1;

    (info->pageToFrame)[lookUp->pageNum] = lookUp->frameNum;
    (info->frameToPage)[lookUp->frameNum] = lookUp->pageNum;
	return RC_OK;
}

/*Implementation of Buffer Pool Functions*/

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                  const int numPages, ReplacementStrategy strategy,
                  void *startData)
{
    int i;
    SM_FileHandle fh;

    if(numPages <= 0){
        return RC_INVALID_BM;
    }

    if (openPageFile ((char *)pageFileName, &fh) != RC_OK){
        return RC_FILE_NOT_FOUND;
    }

    // Initialize all the attributes of BMInfo
    BMInfo *info = malloc(sizeof(BMInfo));

    info->numFrames = 0;
    info->numRead = 0;
    info->numWrite = 0;
    info->startData = startData;
    info->countPinning = 0;

    // initialize all the lookup arrays with initial value Zero.
    memset(info->frameToPage,NO_PAGE,MAX_NO_FRAMES*sizeof(int));
    memset(info->pageToFrame,NO_PAGE,MAX_NO_PAGES*sizeof(int));
    memset(info->dirtyFlags,NO_PAGE,MAX_NO_FRAMES*sizeof(bool));
    memset(info->fixedCounts,NO_PAGE,MAX_NO_FRAMES*sizeof(int));
    memset(info->pageHistory, -1, sizeof(&(info->pageHistory)));
	memset(info->pageToFrequency,0,MAX_NO_PAGES*sizeof(int));

    // Creating a list for vacant frames.
    info->frames = malloc(sizeof(frameList));

    info->frames->head = info->frames->tail = newNodeCreation();

    for(i = 1; i<numPages; ++i){
        info->frames->tail->next = newNodeCreation();
        info->frames->tail->next->previous = info->frames->tail;
        info->frames->tail = info->frames->tail->next;
        info->frames->tail->frameNum = i;
    }

    bm->numPages = numPages;
    bm->pageFile = (char*) pageFileName;
    bm->strategy = strategy;
    bm->mgmtData = info;


    closePageFile(&fh);
    //printf("end init buffer manager\n");
    return RC_OK;
}


RC forceFlushPool(BM_BufferPool *const bm)
{
    if (!bm || bm->numPages <= 0){
        return RC_INVALID_BM;
    }

    BMInfo *info = (BMInfo *)bm->mgmtData;
    frameNode *currentNode = info->frames->head;

    SM_FileHandle fh;

    if (openPageFile ((char *)(bm->pageFile), &fh) != RC_OK){
        return RC_FILE_NOT_FOUND;
    }

    while(currentNode != NULL){
        if(currentNode->dirty == 1){
            if(writeBlock(currentNode->pageNum, &fh, currentNode->data) != RC_OK){
                return RC_WRITE_FAILED;
            }
            currentNode->dirty = 0;
            (info->numWrite)++;
        }
        currentNode = currentNode->next;
    }

    closePageFile(&fh);

    return RC_OK;
}
RC shutdownBufferPool(BM_BufferPool *const bm)
{
    if (!bm || bm->numPages <= 0){
        return RC_INVALID_BM;
    }
    RC status;

    if((status = forceFlushPool(bm)) != RC_OK){
        return status;
    }

    BMInfo *info = (BMInfo *)bm->mgmtData;
    frameNode *currentNode = info->frames->head;

    while(currentNode != NULL){
        currentNode = currentNode->next;
        free(info->frames->head->data);
        free(info->frames->head);
        info->frames->head = currentNode;
    }

    info->frames->head = info->frames->tail = NULL;
    free(info->frames);
    free(info);

    bm->numPages = 0;

    return RC_OK;
}

/*Implementation of Page Management Functions*/

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,
            const PageNumber pageNum)
{
    if (!bm || bm->numPages <= 0){
        return RC_INVALID_BM;
    }
    if(pageNum < 0){
        return RC_READ_NON_EXISTING_PAGE;
    }

	if(bm->strategy==RS_FIFO)
	     return pinPageUsingFIFOStrategy(bm,page,pageNum);
	else if(bm->strategy==RS_LRU)
		 return pinPageUsingLRUStrategy(bm,page,pageNum);
	else if(bm->strategy==RS_CLOCK)
		 return pinPageUsingCLOCKStrategy(bm,page,pageNum);
	else if(bm->strategy== RS_LRU_K)
		 return pinPageUsingLRU_KStrategy(bm,page,pageNum);
	else if(bm->strategy==RS_LFU)
		 return pinPageUsingLFUStrategy(bm,page,pageNum);
	else
		 return RC_UNKNOWN_STRATEGY;


}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    if (!bm || bm->numPages <= 0){
        return RC_INVALID_BM;
    }

    BMInfo *info = (BMInfo *)bm->mgmtData;
    frameNode *lookUp;

    // after performing reading/writing operation unpin the page
    if((lookUp = searchNodeByPageNum(info->frames, page->pageNum)) == NULL){
        return RC_NON_EXISTING_PAGE_IN_FRAME;
    }

    // When unpin a page, decrease its fixCount by 1.
    if(lookUp->fixCount > 0){
        lookUp->fixCount--;
    }
    else{
        return RC_NON_EXISTING_PAGE_IN_FRAME;
    }

    return RC_OK;
}
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    if (!bm || bm->numPages <= 0){
        return RC_INVALID_BM;
    }

    BMInfo *info = (BMInfo *)bm->mgmtData;
    frameNode *lookUp;

    // Find the page if the write operation is performed on it after reading from disk.
    if((lookUp = searchNodeByPageNum(info->frames, page->pageNum)) == NULL){
        return RC_NON_EXISTING_PAGE_IN_FRAME;
    }

    //set the dirty flag of page.
    lookUp->dirty = 1;

    return RC_OK;
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)

{
    if (!bm || bm->numPages <= 0){
        return RC_INVALID_BM;
    }

    BMInfo *info = (BMInfo *)bm->mgmtData;
    frameNode *lookUp;
    SM_FileHandle fh;

    if (openPageFile ((char *)(bm->pageFile), &fh) != RC_OK){
        return RC_FILE_NOT_FOUND;
    }
    // Find the page to be forcefully written back to disk
    if((lookUp = searchNodeByPageNum(info->frames, page->pageNum)) == NULL){
        closePageFile(&fh);
        return RC_NON_EXISTING_PAGE_IN_FRAME;

    }
    // Write all the content of page back to disk after identifying it.
    if(writeBlock(lookUp->pageNum, &fh, lookUp->data) != RC_OK){
        closePageFile(&fh);
        return RC_WRITE_FAILED;
    }

    (info->numWrite)++;

    closePageFile(&fh);

    return  RC_OK;
}


/*Implementation of Statistics Functions*/

int *getFixCounts (BM_BufferPool *const bm)
{
    // find the fixCount value of all pages in buffer
    BMInfo *info = (BMInfo *)bm->mgmtData;
    frameNode *cur = info->frames->head;

    while (cur != NULL){
        (info->fixedCounts)[cur->frameNum] = cur->fixCount;
        cur = cur->next;
    }

    return info->fixedCounts;
}

PageNumber *getFrameContents (BM_BufferPool *const bm)
{
    // return the frametoPage array
    return ((BMInfo *)bm->mgmtData)->frameToPage;
}
int getNumWriteIO (BM_BufferPool *const bm)
{
    // find all those pages which are write after adding to buffer
    return ((BMInfo *)bm->mgmtData)->numWrite;
}

bool *getDirtyFlags (BM_BufferPool *const bm)
{
    // find the dirty bit status of all pages in buffer.
    BMInfo *info = (BMInfo *)bm->mgmtData;
    frameNode *cur = info->frames->head;

    while (cur != NULL){
        (info->dirtyFlags)[cur->frameNum] = cur->dirty;
        cur = cur->next;
    }

    return info->dirtyFlags;
}

int getNumReadIO (BM_BufferPool *const bm)
{
    // find all those pages which are read after adding to buffer
    return ((BMInfo *)bm->mgmtData)->numRead;
}

