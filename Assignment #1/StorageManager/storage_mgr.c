#include <stdio.h>
#include <stdlib.h>
#include "storage_mgr.h"
#include <math.h>

#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <string.h>

FILE *fp;
/* ===========================================
--Storage Manager Initiated.
=============================================*/
extern void initStorageManager (void)
{
    printf("Storage Manager Created\n");

}
/* ================================================
--Create new file with '\0' initialization.
===================================================*/
extern RC createPageFile (char *fileName)
{
    //FILE *fp;
    char *storagestr= (char *) calloc(PAGE_SIZE,sizeof(char));
    fp = fopen(fileName,"w+");
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
extern RC openPageFile(char *fileName, SM_FileHandle *fHandle)
{
    fp = fopen(fileName, "r+");
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
        fclose(fp);
        RC_message="File Opened and Handler Set Successfully.";
        return RC_OK;
    }
        RC_message="File Not Found.";
        return RC_FILE_NOT_FOUND;

}
/* =========================================================
--closePageFile will check if file open then close that file.
============================================================*/
extern RC closePageFile (SM_FileHandle *fHandle)
{
   if(fopen(fHandle->fileName,"r+") != NULL) //check if file exist of not.
   {
	    if (fHandle->mgmtInfo == NULL) // check whether file handler initiated of not.
	    {
	        RC_message="file handler not initiated.";
            return RC_FILE_HANDLE_NOT_INIT;
	    }
	    else{
            fclose(fHandle->mgmtInfo);
            RC_message="File Close Successfully.";
            return RC_OK;
	    }
   }
   else{
        RC_message="File Not Found.";
        return RC_FILE_NOT_FOUND;
   }
}
/* =========================================================
--destroyPageFile will remove file from disk.
============================================================*/
extern RC destroyPageFile (char *fileName)
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
extern RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    fp = fopen(fHandle->fileName, "r");

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
    if(pageNum > fHandle->totalNumPages-1 || pageNum < 0)
    {
        RC_message="Requested page is not exist.";
        return RC_READ_NON_EXISTING_PAGE;
    }

    fseek(fp, pageNum*PAGE_SIZE*sizeof(char), SEEK_SET);
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
extern RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    return  readBlock(0,fHandle,memPage);
}
/* =========================================================
--readPreviousBlock will read previous block of file.
============================================================*/
extern RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    return readBlock(fHandle->curPagePos-1,fHandle,memPage);
}
/* ======================================================================
--readCurrentBlock will read current block where current handler of file.
========================================================================*/
extern RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    return  readBlock(fHandle->curPagePos,fHandle,memPage);
}
/* ======================================================================
--readCurrentBlock will read current block where current handler of file.
========================================================================*/
extern RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    return  readBlock(fHandle->curPagePos+1,fHandle,memPage);
}
/*======================================================================
--readLastBlock will read last block of file.
========================================================================*/
extern RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    return  readBlock(fHandle->totalNumPages-1,fHandle,memPage);
}
/*======================================================================
--writeBlock will write in file from memory at given page position.
========================================================================*/
extern RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    int set_pos = (pageNum)*PAGE_SIZE; //storing starting position of pageNum
    if (fHandle == NULL) // check whether file handler initiated or not.
    {
        return RC_FILE_HANDLE_NOT_INIT;
    }
    fp = fopen(fHandle->fileName, "r+"); // File opened in read+write mode.
    if(fp == NULL)
    {
        RC_message="File Not Found";
        return RC_FILE_NOT_FOUND; // Throw an error file not found
    }
    if(pageNum!=0)  // write in file but not in first page.
    {
        fHandle->curPagePos = set_pos;
        writeCurrentBlock(fHandle,memPage);
    }
    else 	//write content to the first page
    {
        fseek(fp,set_pos,SEEK_SET);
        int i;
        for(i=0; i<PAGE_SIZE; i++)
        {
            if(feof(fp)) // check file is ending in between writing
            {
                appendEmptyBlock(fHandle); // append empty block at the end of file
            }
            fputc(memPage[i],fp);// write content to file
        }
        fHandle->curPagePos = ftell(fp);// set current file position to curPagePos
        fclose(fp); //closing filepointer
    }
    RC_message="File Write block Successfully";
    return RC_OK;
}
/* ======================================================================
--writeCurrentBlock will write in file from memory at current page position.
========================================================================*/
extern RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if (fHandle == NULL)// check whether file handler initiated or not.
    {
        RC_message="file handler not initiated.";
        return RC_FILE_HANDLE_NOT_INIT;
    }

    fp = fopen(fHandle->fileName, "r+");// open file in read+write mode.
    if(fp == NULL)
    {
        RC_message="File Not Found";
        return RC_FILE_NOT_FOUND; // Throw an error file not found.
    }
    long int currPosition = fHandle->curPagePos; //Storing current file position.

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
extern RC appendEmptyBlock (SM_FileHandle *fHandle)
{


    if (fHandle == NULL)// check whether file handler initiated or not.
    {
        RC_message="file handler not initiated.";
        return RC_FILE_HANDLE_NOT_INIT;
    }

    SM_PageHandle EmptyPage = (SM_PageHandle)calloc(PAGE_SIZE,sizeof(char)); //creating empty page of PAGE_SIZE bytes
    fseek(fp, 0, SEEK_END);
    fwrite(EmptyPage,sizeof(char),PAGE_SIZE,fp); //Writing Empty page to the file.
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

    fclose(fp);
    return RC_OK;
}


