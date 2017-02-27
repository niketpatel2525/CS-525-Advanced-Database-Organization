#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <math.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "record_mgr.h"

/* Copied from the rm_serializer file for extra serialization.*/

typedef struct VarString {
    char *buf;
    int size;
    int bufsize;
} VarString;


#define FREE_VARSTRING(var)			\
do {						\
free(var->buf);				\
free(var);					\
} while (0)

#define MAKE_VARSTRING(var)				\
do {							\
var = (VarString *) malloc(sizeof(VarString));	\
var->size = 0;					\
var->bufsize = 100;					\
var->buf = malloc(100);				\
} while (0)


#define RETURN_STRING(var)			\
do {						\
char *resultStr;				\
GET_STRING(resultStr, var);			\
FREE_VARSTRING(var);			\
return resultStr;				\
} while (0)

#define GET_STRING(result, var)			\
do {						\
result = malloc((var->size) + 1);		\
memcpy(result, var->buf, var->size);	\
result[var->size] = '\0';			\
} while (0)


#define ENSURE_SIZE(var,newsize)				\
do {								\
if (var->bufsize < newsize)					\
{								\
int newbufsize = var->bufsize;				\
while((newbufsize *= 2) < newsize);			\
var->buf = realloc(var->buf, newbufsize);			\
}								\
} while (0)

#define APPEND(var, ...)			\
do {						\
char *tmp = malloc(10000);			\
sprintf(tmp, __VA_ARGS__);			\
APPEND_STRING(var,tmp);			\
free(tmp);					\
} while(0)


#define APPEND_STRING(var,string)					\
do {									\
ENSURE_SIZE(var, var->size + strlen(string));			\
memcpy(var->buf + var->size, string, strlen(string));		\
var->size += strlen(string);					\
} while(0)


/* copied from rm_serializer file for attribute functions */
RC
attrOffset (Schema *schema, int attrNum, int *result)
{
    int offset = 0;
    int attrPos = 0;

    for(attrPos = 0; attrPos < attrNum; attrPos++)
        switch (schema->dataTypes[attrPos])
    {
        case DT_STRING:
            offset += schema->typeLength[attrPos];
            break;
        case DT_INT:
            offset += sizeof(int);
            break;
        case DT_FLOAT:
            offset += sizeof(float);
            break;
        case DT_BOOL:
            offset += sizeof(bool);
            break;
    }

    *result = offset;
    return RC_OK;
}


/* Used in scan functions*/
typedef struct recordInfo {
    Expr *condition;
    int recentSlot;
    int recentPage;
    int totalPages;
    int totalAvailSlot;

}recordInfo;

/* list of tombstones */
typedef struct tableNode {
    RID id;
    struct tableNode *next;
}tableNode;

/* record_mgr starts. */

typedef struct tableInfo{

    int scLen;
    int recoBeginPage;
    int recoEndPage;
    int tupleSize;
    int slotWindow;
    int maxSlots;
    int tNodeLength;
    tableNode *tstone_head;
    BM_BufferPool *bm;
}tableInfo;


int getslotSize(Schema *sc){
    int s_size=0, k=0, temp;
    s_size += (1 + 5 + 1 + 5 + 1 + 1 + 1); //  2 int, 1 hyphen(-), 1 bracket (), 1 space( ),2 square brackets([]).
    for(k=0; k<sc->numAttr; ++k){
        switch (sc->dataTypes[k]){
            case DT_STRING:
                temp = sc->typeLength[k];
                break;
            case DT_INT:
                temp = 5;
                break;
            case DT_FLOAT:
                temp = 10;
                break;
            case DT_BOOL:
                temp = 5;
                break;
            default:
                break;
        }
        s_size += (temp + strlen(sc->attrNames[k]) + 1 + 1);     // comma,colon , dataType_length,   ending bracket or attrname_len.
    }
    return s_size;
}


tableInfo *convertStringToTableInfo(char *info_string){
    tableInfo *info = (tableInfo*) malloc(sizeof(tableInfo));

    char info_data_str[strlen(info_string)];
    strcpy(info_data_str, info_string);

    char *t1, *t2;
    t1 = strtok (info_data_str,"<");
    t1 = strtok (NULL,">");
    info->scLen = strtol(t1, &t2, 10);

    t1 = strtok (NULL,"<");
    t1 = strtok (NULL,">");
    info->recoBeginPage = strtol(t1, &t2, 10);

    t1 = strtok (NULL,"<");
    t1 = strtok (NULL,">");
    info->recoEndPage = strtol(t1, &t2, 10);

    t1 = strtok (NULL,"<");
    t1 = strtok (NULL,">");
    info->tupleSize = strtol(t1, &t2, 10);

    t1 = strtok (NULL,"<");
    t1 = strtok (NULL,">");
    info->slotWindow = strtol(t1, &t2, 10);

    t1 = strtok (NULL,"<");
    t1 = strtok (NULL,">");
    info->maxSlots = strtol(t1, &t2, 10);

    t1 = strtok (NULL,"<");
    t1 = strtok (NULL,">");
    int tnode_len = strtol(t1, &t2, 10);
    info->tNodeLength = tnode_len;
    t1 = strtok (NULL,"<");
    int i, page, slot;

    info->tstone_head = NULL;
    tableNode *temp_node;

    for(i=0; i<tnode_len; ++i){
        t1 = strtok (NULL,":");
        page = strtol(t1, &t2, 10);

        if(i != (tnode_len - 1)){
            t1 = strtok (NULL,",");
        }
        else{
            t1 = strtok (NULL,">");
        }
        slot = strtol(t1, &t2, 10);

        if (info->tstone_head == NULL){
            info->tstone_head = (tableNode *)malloc(sizeof(tableNode));
            info->tstone_head->id.page = page;
            info->tstone_head->id.slot = slot;
            temp_node = info->tstone_head;
        }
        else{
            temp_node->next = (tableNode *)malloc(sizeof(tableNode));
            temp_node->next->id.page = page;
            temp_node->next->id.slot = slot;

            temp_node = temp_node->next;
        }

    }
    return info;
}
char *convertTableInfoToString(tableInfo *info){
    VarString *res;
    MAKE_VARSTRING(res);
    APPEND(res, "SchemaLength <%i> FirstRecordPage <%i> LastRecordPage <%i> NumTuples <%i> SlotSize <%i> MaxSlots <%i> ", info->scLen, info->recoBeginPage, info->recoEndPage, info->tupleSize, info->slotWindow, info->maxSlots);
    tableNode *temp_head;
    temp_head = info->tstone_head;
    int tablenode_len = 0;

    while(temp_head != NULL){
    	temp_head = temp_head->next;
    	tablenode_len++;
    }
    APPEND(res, "tNodeLen <%i> <", tablenode_len);
    temp_head = info->tstone_head;
    while(temp_head != NULL){
        APPEND(res,"%i:%i%s ",temp_head->id.page, temp_head->id.slot, (temp_head->next != NULL) ? ", ": "");
        temp_head = temp_head->next;
    }
    APPEND_STRING(res, ">\n");

    char *restStr;				\
    GET_STRING(restStr, res);
    return restStr;

}

Schema *deserializeSchema(char *sc_str){
    Schema *sc = (Schema *) malloc(sizeof(Schema));
    int i, j;
    int nAttr;

    char *t1;
	char *t2;

    char schema_data_str[strlen(sc_str)];
    strcpy(schema_data_str, sc_str);

    t1 = strtok (schema_data_str,"<");
    t1 = strtok (NULL,">");

    nAttr = strtol(t1, &t2, 10);
    sc->numAttr= nAttr;

    sc->attrNames=(char **)malloc(sizeof(char*)*nAttr);
    sc->dataTypes=(DataType *)malloc(sizeof(DataType)*nAttr);
    sc->typeLength=(int *)malloc(sizeof(int)*nAttr);
    char* str_ref[nAttr];
    t1 = strtok (NULL,"(");

    for(i=0; i<nAttr; ++i){
        t1 = strtok (NULL,": ");
        sc->attrNames[i]=(char *)calloc(strlen(t1), sizeof(char));
        strcpy(sc->attrNames[i], t1);

        if(i == nAttr-1){
            t1 = strtok (NULL,") ");
        }
        else{
            t1 = strtok (NULL,", ");
        }

        str_ref[i] = (char *)calloc(strlen(t1), sizeof(char));

        if (strcmp(t1, "INT") == 0){
            sc->dataTypes[i] = DT_INT;
            sc->typeLength[i] = 0;
        }
        else if (strcmp(t1, "FLOAT") == 0){
            sc->dataTypes[i] = DT_FLOAT;
            sc->typeLength[i] = 0;
        }
        else if (strcmp(t1, "BOOL") == 0){
            sc->dataTypes[i] = DT_BOOL;
            sc->typeLength[i] = 0;
        }
        else{
            strcpy(str_ref[i], t1);
        }
    }

    int keyFlag = 0, keySize = 0;
    char* keyAttrs[nAttr];

    if((t1 = strtok (NULL,"(")) != NULL){
        t1 = strtok (NULL,")");
        char *key = strtok (t1,", ");

        while(key != NULL){
            keyAttrs[keySize] = (char *)malloc(strlen(key)*sizeof(char));
            strcpy(keyAttrs[keySize], key);
            keySize++;
            key = strtok (NULL,", ");
        }
        keyFlag = 1;
    }

    char *temp3;
    for(i=0; i<nAttr; ++i){
        if(strlen(str_ref[i]) > 0){
            temp3 = (char *) malloc(sizeof(char)*strlen(str_ref[i]));
            memcpy(temp3, str_ref[i], strlen(str_ref[i]));
            sc->dataTypes[i] = DT_STRING;
            t1 = strtok (temp3,"[");
            t1 = strtok (NULL,"]");
            sc->typeLength[i] = strtol(t1, &t2, 10);
            free(temp3);
            free(str_ref[i]);
        }
    }

    if(keyFlag == 1){
        sc->keyAttrs=(int *)malloc(sizeof(int)*keySize);
        sc->keySize = keySize;
        for(i=0; i<keySize; ++i){
            for(j=0; j<nAttr; ++j){
                if(strcmp(keyAttrs[i], sc->attrNames[j]) == 0){
                    sc->keyAttrs[i] = j;
                    free(keyAttrs[i]);
                }
            }
        }
    }

    return sc;
}

int getTotalSizeOfSchema(Schema *sc){
    int k, size = 0;

    size = sizeof(int)                          // numAttr
    + sizeof(int)*(sc->numAttr)     // dataTypes
    + sizeof(int)*(sc->numAttr)     // type_lengths
    + sizeof(int)                       // keySize
    + sizeof(int)*(sc->keySize);    // keyAttrs

    for (k=0; k<sc->numAttr; ++k){
        size += strlen(sc->attrNames[k]);
    }

    return size;
}


Record *deserializeRecord(char *record_string, RM_TableData *rel){

	int k;

    Schema *sc = rel->schema;
    Record *rec = (Record *) malloc(sizeof(Record));
    tableInfo *info = (tableInfo *) (rel->mgmtData);
    Value *value;
    rec->data = (char *)malloc(sizeof(char) * info->slotWindow);
    char record_data_str[strlen(record_string)];
    strcpy(record_data_str, record_string);
    char *t1;
	char *t2;

    t1 = strtok(record_data_str,"-");

    t1 = strtok (NULL,"]");

    t1 = strtok (NULL,"(");


    for(k=0; k<sc->numAttr; ++k){
        t1 = strtok (NULL,":");
        if(k == (sc->numAttr - 1)){
            t1 = strtok (NULL,")");
        }
        else{
            t1 = strtok (NULL,",");
        }

        /* set attribute values as per the attributes datatype */
        switch(sc->dataTypes[k]){
            case DT_INT:
            {

                int val = strtol(t1, &t2, 10);

                MAKE_VALUE(value, DT_INT, val);
                setAttr (rec, sc, k, value);
		freeVal(value);
            }
                break;
            case DT_STRING:
            {
                MAKE_STRING_VALUE(value, t1);
                setAttr (rec, sc, k, value);
		freeVal(value);
            }
                break;
            case DT_FLOAT:
            {
                float val = strtof(t1, NULL);
                MAKE_VALUE(value, DT_FLOAT, val);
                setAttr (rec, sc, k, value);
		freeVal(value);
            }
                break;
            case DT_BOOL:
            {
                bool val;
                val = (t1[0] == 't') ? TRUE : FALSE;
                MAKE_VALUE(value, DT_BOOL, val);
                setAttr (rec, sc, k, value);
		freeVal(value);
            }
                break;
        }
    }
    free(record_string);
    return rec;
}


RC storeTableInfoIntoFile(char *file_name, tableInfo *info){

	char *info_str;

    if(access(file_name, F_OK) == -1) {
        return RC_TABLE_NOT_FOUND;
    }

    SM_FileHandle fh;
    int state;

    if ((state=openPageFile(file_name, &fh)) != RC_OK){
        return state;
    }

    info_str = convertTableInfoToString(info);
    if ((state=writeBlock(0, &fh, info_str)) != RC_OK){
        free(info_str);
        return state;
    }

    if ((state=closePageFile(&fh)) != RC_OK){
        free(info_str);
        return state;
    }

    free(info_str);
    return RC_OK;
}
// table and manager

/*These  functions are used  to initialize and shutdown a record manager.
 * These function can create, open and close a table.
 * When a table is created page info is stored in file which contain schema information and free space info also.
 */

extern RC createTable (char *name, Schema *schema){

	/* Check for table existence  */

    if( access(name, F_OK) != -1 ) {
        return RC_TABLE_ALREADY_EXITS;
    }

    int state;
    SM_FileHandle fh;

    char *info_string;

    /* Make file with provided name and make pages for info/schema */
    /* Create a file with the given name and create pages for info and schema*/
    if ((state=createPageFile(name)) != RC_OK)
        return state;

    int sc_size = getTotalSizeOfSchema(schema);
    int s_size = getslotSize(schema);
    int file_size = (int) ceil((float)sc_size/PAGE_SIZE);
    int total_slots = (int) floor((float)PAGE_SIZE/(float)s_size);

    if ((state=openPageFile(name, &fh)) != RC_OK)
        return state;

    if ((state=ensureCapacity((file_size + 1), &fh)) != RC_OK){
        return state;
    }
    /* first page will give info about file */
    tableInfo *info = (tableInfo *) malloc(sizeof(tableInfo));
    info->tupleSize = 0;
    info->scLen = sc_size;
    info->recoBeginPage = file_size + 1;
    info->slotWindow = s_size;
    info->recoEndPage = file_size + 1;
    info->maxSlots = total_slots;
    info->tstone_head = NULL;

    info_string = convertTableInfoToString(info);
    if ((state=writeBlock(0, &fh, info_string)) != RC_OK)
        return state;

    /* from second page actual schema will be stored */
    char *sc_str = serializeSchema(schema);
    if ((state=writeBlock(1, &fh, sc_str)) != RC_OK)
        return state;
    if ((state=closePageFile(&fh)) != RC_OK)
        return state;

    return RC_OK;
}
extern RC initRecordManager (void *mgmtData)
{
    return RC_OK;
}

extern RC openTable (RM_TableData *rel, char *name){


    if(access(name, F_OK) == -1) {
        return RC_TABLE_NOT_FOUND;
    }

    BM_BufferPool *bm = (BM_BufferPool *)malloc(sizeof(BM_BufferPool));
    BM_PageHandle *page = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));

    initBufferPool(bm, name, 3, RS_FIFO, NULL);
    pinPage(bm, page, 0);

    tableInfo *info = convertStringToTableInfo(page->data);

    if(info->scLen < PAGE_SIZE){
        pinPage(bm, page, 1);
    }

    rel->schema = deserializeSchema(page->data);
    rel->name = name;

    info->bm = bm;
    rel->mgmtData = info;

    free(page);

    return RC_OK;
}

extern RC closeTable (RM_TableData *rel){

    shutdownBufferPool(((tableInfo *)rel->mgmtData)->bm);
    free(rel->mgmtData);

    free(rel->schema->dataTypes);

    free(rel->schema->attrNames);

    free(rel->schema->keyAttrs);
    free(rel->schema->typeLength);

    free(rel->schema);

    return RC_OK;
}
extern RC shutdownRecordManager (){
    return RC_OK;
}
extern RC deleteTable (char *name){
    if(access(name, F_OK) == -1) {
        return RC_TABLE_NOT_FOUND;
    }

    if(remove(name) != 0){
        return RC_TABLE_NOT_FOUND;
    }
    return RC_OK;
}

extern int getNumTuples (RM_TableData *rel){
    return ((tableInfo *)rel->mgmtData)->tupleSize;
}

// handling records in a table
/* These are actual CRUD operation of Database.
 *
 * */

extern RC isPrimaryKey(RM_TableData *rel, Record *record) {
	Record *r;
	if (record != NULL) {
		if (getRecord(rel, record->id, r) == RC_RM_NO_MORE_TUPLES)
			return RC_RM_NO_MORE_TUPLES;
	}
	return RC_OK;
}

extern RC insertRecord (RM_TableData *rel, Record *record){
    BM_PageHandle *page = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    tableInfo *info = (tableInfo *) (rel->mgmtData);
    int page_no;
	int slot_no;

    if (info->tstone_head != NULL){
    	page_no = info->tstone_head->id.page;
    	slot_no = info->tstone_head->id.slot;
        info->tstone_head = info->tstone_head->next;
    }
    else{

    	page_no = info->recoEndPage;
    	slot_no = info->tupleSize - ((page_no - info->recoBeginPage)*info->maxSlots) ;

        if (slot_no == info->maxSlots){
        	slot_no = 0;
            page_no++;
        }
        info->recoEndPage = page_no;
    }
    record->id.page = page_no;
    record->id.slot = slot_no;

    char *record_str = serializeRecord(record, rel->schema);

    pinPage(info->bm, page, page_no);
    memcpy(page->data + (slot_no*info->slotWindow), record_str, strlen(record_str));
    free(record_str);

    markDirty(info->bm, page);
    unpinPage(info->bm, page);
    forcePage(info->bm, page);

    record->id.tstone = false;
    (info->tupleSize)++;
    storeTableInfoIntoFile(rel->name, info);
    free(page);
    return RC_OK;
}
extern RC updateRecord (RM_TableData *rel, Record *record){
    BM_PageHandle *page = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    tableInfo *info = (tableInfo *) (rel->mgmtData);
    int page_no, slot_no;
    //isPrimaryKey(rel,record);

    page_no = record->id.page;
    slot_no = record->id.slot;

    char *record_str = serializeRecord(record, rel->schema);

    pinPage(info->bm, page, page_no);
    memcpy(page->data + (slot_no*info->slotWindow), record_str, strlen(record_str));
    free(record_str);

    markDirty(info->bm, page);
    unpinPage(info->bm, page);
    forcePage(info->bm, page);

    storeTableInfoIntoFile(rel->name, info);

    return RC_OK;

}
extern RC getRecord (RM_TableData *rel, RID id, Record *record){
    tableInfo *info = (tableInfo *) (rel->mgmtData);
    BM_PageHandle *page = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));

    int l=0;
    int page_no;
	int slot_no;
    page_no = id.page;
    slot_no = id.slot;

    record->id.page = page_no;
    record->id.slot = slot_no;
    record->id.tstone = 0;

    tableNode *root = info->tstone_head;
    int tombStoneFlag = 0;
    int tombStoneCount = 0;

    for(l=0; l<info->tNodeLength; ++l){
        if (root->id.page == page_no && root->id.slot == slot_no){
            tombStoneFlag = 1;
            record->id.tstone = 1;
            break;
        }
        root = root->next;
        tombStoneCount++;
    }

    //printf("2..\n");
    	/* Read the page and slot*/
    if (tombStoneFlag == 0){
        int tupleNumber = (page_no - info->recoBeginPage)*(info->maxSlots) + slot_no + 1 - tombStoneCount;
        if (tupleNumber > info->tupleSize){
	    free(page);
            return RC_RM_NO_MORE_TUPLES;
        }

        pinPage(info->bm, page, page_no);
        char *record_str = (char *) malloc(sizeof(char) * info->slotWindow);
        memcpy(record_str, page->data + ((slot_no)*info->slotWindow), sizeof(char)*info->slotWindow);
        unpinPage(info->bm, page);

        Record *temp_record = deserializeRecord(record_str, rel);

        record->data = temp_record->data;
        free(temp_record);
    }

    free(page);
    return RC_OK;
}

extern RC deleteRecord (RM_TableData *rel, RID id){
    tableInfo *info = (tableInfo *) (rel->mgmtData);
    tableNode *tstone_iter = info->tstone_head;
    if(info->tupleSize>0){

    	/*save RID to end of tombstone*/
    	if(info->tstone_head == NULL){
            info->tstone_head = (tableNode *)malloc(sizeof(tableNode));
            info->tstone_head->next = NULL;
            tstone_iter = info->tstone_head;
        }
        else{
            while (tstone_iter->next != NULL){
                tstone_iter = tstone_iter->next;
            }
            tstone_iter->next = (tableNode *)malloc(sizeof(tableNode));
            tstone_iter = tstone_iter->next;
            tstone_iter->next = NULL;
        }

        tstone_iter->id.page = id.page;
        tstone_iter->id.slot = id.slot;
        tstone_iter->id.tstone = TRUE;
        (info->tupleSize)--;
        storeTableInfoIntoFile(rel->name, info);
    }
    else{
        return RC_WRITE_FAILED;     // Write Failed in tombstone.
    }

    return RC_OK;

}


// scans

/*These function are used to evaluate condition and scan all the tuples that satisfy condition.
 * */
extern RC next (RM_ScanHandle *scan, Record *record){

    recordInfo *rcordNode;
    Value *val;
    rcordNode = scan->mgmtData;
    RC state;


    record->id.slot = rcordNode->recentSlot;
    record->id.page = rcordNode->recentPage;

    /* fetch the record by the page and slot id */
    state = getRecord(scan->rel, record->id, record);

    if(state == RC_RM_NO_MORE_TUPLES){

        return RC_RM_NO_MORE_TUPLES;
    }

	/* check tombstone id for a deleted record
	 *
	 * and update record node parameters accordingly */
    else if(record->id.tstone == 1){
        if (rcordNode->recentSlot == rcordNode->totalAvailSlot - 1){
        	rcordNode->recentSlot = 0;
            (rcordNode->recentPage)++;
        }
        else{
            (rcordNode->recentSlot)++;
        }
        scan->mgmtData = rcordNode;
        return next(scan, record);
    }
    /* Check Expression for required record */
    else{
        evalExpr(record, scan->rel->schema, rcordNode->condition, &val);
        if (rcordNode->recentSlot == rcordNode->totalAvailSlot - 1){
        	rcordNode->recentSlot = 0;
            (rcordNode->recentPage)++;
        }
        else{
            (rcordNode->recentSlot)++;
        }
        scan->mgmtData = rcordNode;

	/* If the record fetched is not the
	 * required one then call the next
	 * function with the updated record id parameters. */
        if(val->v.boolV != 1){
            return next(scan, record);
        }
        else{
            return RC_OK;
        }
    }

}

extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){

	/* init  RM_ScanHandle data structure */
	scan->rel = rel;

	/* init recordInfo variable for storing info of searching record and its condition  */

	recordInfo *rNode = (recordInfo *) malloc(sizeof(recordInfo));
    rNode->recentPage = ((tableInfo *)rel->mgmtData)->recoBeginPage;
    rNode->recentSlot = 0;
    rNode->condition = cond;
    rNode->totalAvailSlot = ((tableInfo *)rel->mgmtData)->maxSlots;
    rNode->totalPages = ((tableInfo *)rel->mgmtData)->recoEndPage;

	/* assign rNode to scan->mgmtData */
    scan->mgmtData = (void *) rNode;

    return RC_OK;
}
extern RC closeScan (RM_ScanHandle *scan){
    //free(scan);
    return RC_OK;
}

/*
 * Schema function to handle schema related task
 *
 */
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){

    Schema *sc = (Schema *) malloc(sizeof(Schema));

    sc->numAttr = numAttr;
    sc->attrNames = attrNames;
    sc->dataTypes = dataTypes;
    sc->typeLength = typeLength;
    sc->keySize = keySize;
    sc->keyAttrs = keys;

    return sc;
}
extern RC freeSchema (Schema *schema){
    free(schema);
    return RC_OK;
}

// dealing with records and attribute values
extern RC createRecord (Record **record, Schema *schema){

	/* allocate memory for a new record and record data as per the schema. */
    *record = (Record *)  malloc(sizeof(Record));
    (*record)->data = (char *)malloc((getRecordSize(schema)));

    return RC_OK;
}

extern int getRecordSize (Schema *schema){
    int size = 0, tmpSize = 0, i;

    for(i=0; i<schema->numAttr; ++i){
        switch (schema->dataTypes[i]){

            case DT_FLOAT:
            	tmpSize = sizeof(float);
                break;
            case DT_INT:
                       	tmpSize = sizeof(int);
                           break;
            case DT_BOOL:
            	tmpSize = sizeof(bool);
                break;
            case DT_STRING:
                       	tmpSize = schema->typeLength[i];
                           break;
            default:
                break;
        }
        size += tmpSize;
    }
    return size;
}

extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){
    int offset;
    char * attributeData;

	/* get the offset value of different attributes as  per the attribute numbers */
    attrOffset(schema, attrNum, &offset);
    attributeData = record->data + offset;

     /* set attribute values as per the attributes datatype */
    switch(schema->dataTypes[attrNum])
    {
        case DT_INT:
        {
            memcpy(attributeData,&(value->v.intV), sizeof(int));
        }
            break;
        case DT_STRING:
        {
            char *stringV;
            int len = schema->typeLength[attrNum];
            stringV = (char *) malloc(len);
            stringV = value->v.stringV;
            memcpy(attributeData,(stringV), len);
        }
            break;
        case DT_FLOAT:
        {
            memcpy(attributeData,&((value->v.floatV)), sizeof(float));
        }
            break;
        case DT_BOOL:
        {
            memcpy(attributeData,&((value->v.boolV)), sizeof(bool));
        }
            break;
    }

    return RC_OK;
}

extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){

	/* allocate the space to the value data structre where the attribute values are to be fetched */
    *value = (Value *)  malloc(sizeof(Value));
    int offset;
    char *attributeData;

	/* get the offset value of different attributes as per the attribute numbers */
    attrOffset(schema, attrNum, &offset);
    attributeData = (record->data + offset);
    (*value)->dt = schema->dataTypes[attrNum];

	/* attribute data is assigned to the value pointer as per the different data types */
    switch(schema->dataTypes[attrNum])
    {
        case DT_INT:
        {
            memcpy(&((*value)->v.intV),attributeData, sizeof(int));
        }
            break;
        case DT_STRING:
        {
            int len = schema->typeLength[attrNum];
            char *stringV;
            stringV = (char *) malloc(len + 1);
            strncpy(stringV, attributeData, len);
            stringV[len] = '\0';
            //MAKE_STRING_VALUE(*value, stringV);
            //(*value)->v.stringV = (char *) malloc(len);
            //strncpy((*value)->v.stringV, stringV, len);
            (*value)->v.stringV = stringV;
            //free(stringV);
        }
            break;
        case DT_FLOAT:
        {
            memcpy(&((*value)->v.floatV),attributeData, sizeof(float));
        }
            break;
        case DT_BOOL:
        {
            memcpy(&((*value)->v.boolV),attributeData, sizeof(bool));
        }
            break;
    }

    return RC_OK;

}

extern RC freeRecord (Record *record){
	 /* free the memory space allocated to record and its data */
    free(record->data);
    free(record);

    return RC_OK;
}
