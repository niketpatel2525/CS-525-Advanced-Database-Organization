#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "storage_mgr.h"
#include "dberror.h"

/* ===========================================
--Storage Manager Initiated.
=============================================*/
void initStorageManager (void)
{
    printf("Storage Manager Created\n");

}
/* ================================================
--Create new file with '\0' initialization.
===================================================*/
RC createPageFile (char *fileName)
{
    FILE *fp=fopen(fileName, "w");

    char *storagestr= (char *) calloc(PAGE_SIZE,sizeof(char));

    memset(storagestr,'\0',PAGE_SIZE);//write '\0' to ops string of PAGE_SIZE

    fwrite(storagestr,sizeof(char),PAGE_SIZE,fp);//write ops string to file.
    free(storagestr);//free the pointer for no memory leak.

    fclose(fp);

    RC_message="File creation successfully";
    return RC_OK;
}
/* =======================================================
--open existing file and set FileHandle structure pointer.
==========================================================*/
RC openPageFile (char *fileName, SM_FileHandle *fHandle){
    FILE *fp = fopen(fileName, "r+");

    if(fp != NULL) // if file not exist i.e NULL
	{
		// File Name:
		fHandle->fileName = fileName;

		//Cur PAGE Position
		fHandle->curPagePos = 0;

		// Total Number of Page present
		fseek(fp,0,SEEK_END);
		int i = (int) ftell(fp);
		int j = i/ PAGE_SIZE;
		fHandle->totalNumPages = j;

		// mgmtInfo
		fHandle->mgmtInfo =fp;
		//fclose(fp);
		RC_message="File Opened and Handler Set Successfully.";
		return RC_OK;
	}
		RC_message="File Not Found.";
		return RC_FILE_NOT_FOUND;
}

/* =========================================================
--closePageFile will check if file open then close that file.
============================================================*/
RC closePageFile (SM_FileHandle *fHandle)
{
	if (fHandle->mgmtInfo != NULL) // check whether file handler initiated of not.
	{
	   if(!fclose(fHandle->mgmtInfo)) //check if file exist of not.
	   {
		   RC_message="File Close Successfully.";
		   return RC_OK;
	   }
	   else{
			RC_message="File Not Found.";
			return RC_FILE_NOT_FOUND;
	   }
	}
	else{
		RC_message="file handler not initiated.";
		return RC_FILE_HANDLE_NOT_INIT;
	}
}
/* =========================================================
--destroyPageFile will remove file from disk.
============================================================*/
RC destroyPageFile (char *fileName)
{
	if(fopen(fileName,"r") != NULL)//check whether file is exist and open or not.
	{
		remove(fileName);//remove file from disk.
		RC_message="File removed Successfully.";
		return RC_OK;
	}
	else
	{
		RC_message="File Not Found";
		return RC_FILE_NOT_FOUND;//else display file not found
	}
}

/* =========================================================
--readBlock will read page from disk to memory.
============================================================*/
RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    FILE *fp = fHandle->mgmtInfo;

    if (fHandle == NULL)//checking whether file handler initialized of not.
    {
        RC_message="file handler not initiated.";
        return RC_FILE_HANDLE_NOT_INIT;
    }
    if(fp == NULL)//checking if the file is exist or not
    {
        RC_message="File Not Found";
        return RC_FILE_NOT_FOUND;
    }
    if(pageNum >= fHandle->totalNumPages || pageNum < 0)
    {
        RC_message="Requested page is not exist.";
        return RC_READ_NON_EXISTING_PAGE;
    }

    fseek(fp, (pageNum+1)*PAGE_SIZE*sizeof(char), SEEK_SET);
    fread(memPage, 1, PAGE_SIZE, fp);

    fHandle->curPagePos = pageNum;

    return RC_OK;
}
/* =========================================================
--getBlockPos will return the current page position.
============================================================*/
int getBlockPos (SM_FileHandle *fHandle)
{
    if(fHandle != NULL)//check whether the file handle is initiated or not.
	{
		if((fopen(fHandle->fileName,"r")) != NULL)// check whether file exist of not.
		{
			return fHandle->curPagePos;
		}
		else
		{
			RC_message="File Not Found";
			return RC_FILE_NOT_FOUND;
		}
	}
	else
	{
		RC_message="file handler not initiated.";
		return RC_FILE_HANDLE_NOT_INIT;
	}
}

/* =========================================================
--readFirstBlock will read first block of file.
============================================================*/
RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    return  readBlock(0,fHandle,memPage);
}
/* =========================================================
--readPreviousBlock will read previous block of file.
============================================================*/
RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    return readBlock(fHandle->curPagePos-1,fHandle,memPage);
}
/* ======================================================================
--readCurrentBlock will read current block where current handler of file.
========================================================================*/
RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    return  readBlock(fHandle->curPagePos,fHandle,memPage);
}
/* ======================================================================
--readCurrentBlock will read current block where current handler of file.
========================================================================*/
RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    return  readBlock(fHandle->curPagePos+1,fHandle,memPage);
}
/*======================================================================
--readLastBlock will read last block of file.
========================================================================*/
RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    return  readBlock(fHandle->totalNumPages-1,fHandle,memPage);
}

/*======================================================================
--writeBlock will write in file from memory at given page position.
========================================================================*/
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{

    int set_pos = (pageNum+1)*PAGE_SIZE; //storing starting position of pageNum

    if (fHandle == NULL) // check whether file handler initiated or not.
    {
        return RC_FILE_HANDLE_NOT_INIT;
    }
    FILE *fp = fHandle->mgmtInfo; // File opened in read+write mode.
    if(fp == NULL)
    {
        RC_message="File Not Found";
        return RC_FILE_NOT_FOUND; // Throw an error file not found
    }

    if((pageNum+1)!=0)  // write in file but not in first page.
    {
        fHandle->curPagePos = set_pos;
        writeCurrentBlock(fHandle,memPage);
    }
    else 	//write content to the first page
    {
    	//printf("write block   Set=%d\n\n\n",pageNum);
        fseek(fp,set_pos,SEEK_SET);
        int i;
        for(i=0; i<PAGE_SIZE; i++)
        {
            if(feof(fp)) // check file is ending in between writing
            {
                appendEmptyBlock(fHandle); // append empty block at the end of file
                //printf("write block   Set=%d\n\n\n",pageNum);
            }
            fputc(memPage[i],fp);// write content to file
        }
        fHandle->curPagePos = ftell(fp)/PAGE_SIZE;// set current file position to curPagePos
        fclose(fp); //closing filepointer
        //printf("write fHandle->curPagePos   Set=%d\n\n\n",fHandle->curPagePos);
    }
    RC_message="File Write block Successfully";
    return RC_OK;
}

/* ======================================================================
--writeCurrentBlock will write in file from memory at current page position.
========================================================================*/

RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if (fHandle == NULL)// check whether file handler initiated or not.
    {
        RC_message="file handler not initiated.";
        return RC_FILE_HANDLE_NOT_INIT;
    }

    FILE *fp = fopen(fHandle->fileName, "r+");// open file in read+write mode.
    if(fp == NULL)
    {
        RC_message="File Not Found";
        return RC_FILE_NOT_FOUND; // Throw an error file not found.
    }
    long int currPosition = fHandle->curPagePos; //Storing current file position.
    //printf("currPosition==%ld",currPosition);
    fseek(fp,currPosition,SEEK_SET); //Seek to the current position.
    fwrite(memPage,1,PAGE_SIZE,fp);//Writing the memPage to our file.

    fHandle->curPagePos = ftell(fp); // set current file position to curPagePos
    fclose(fp); //closing filepointer
    RC_message="File Write current block Successfully";
    return RC_OK;
}
/* ======================================================================
--appendEmptyBlock will append empty block at the end of file.
========================================================================*/
RC appendEmptyBlock (SM_FileHandle *fHandle)
{
	//printf("HI Append..\n");
    if (fHandle == NULL)// check whether file handler initiated or not.
    {
        RC_message="file handler not initiated.";
        return RC_FILE_HANDLE_NOT_INIT;
    }

    SM_PageHandle EmptyPage = (SM_PageHandle)calloc(PAGE_SIZE,sizeof(char)); //creating empty page of PAGE_SIZE bytes
    fseek(fHandle->mgmtInfo, (fHandle->totalNumPages + 1)*PAGE_SIZE*sizeof(char), SEEK_END);

    fwrite(EmptyPage,sizeof(char),PAGE_SIZE,fHandle->mgmtInfo); //Writing Empty page to the file.
    free(EmptyPage); //free memory from EmptyPage.
    fHandle->totalNumPages++; //Increasing total number of pages.
    RC_message = "Append empty block at end of file Successfully.";
    return RC_OK;
}
/* =========================================================================/
--ensureCapacity will check that if new page needed then append at the end.
============================================================================*/
extern RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
{

    if (fHandle == NULL)// check whether file handler initiated or not.
    {
        RC_message="file handler not initiated.";
        return RC_FILE_HANDLE_NOT_INIT;
    }
    while(numberOfPages > fHandle->totalNumPages) //If numberOfPages is greater than totalNumPages then add emptly pages.
        appendEmptyBlock(fHandle);

    //fclose(fHandle->mgmtInfo);
    return RC_OK;
}
