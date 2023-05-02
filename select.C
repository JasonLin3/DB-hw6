#include "catalog.h"
#include "query.h"


// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[],
		       const attrInfo *attr, 
		       const Operator op, 
		       const char *attrValue)
{
   // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;

	Status status;

	// get relation info
	int relAttrCnt;
	AttrDesc* relDesc;
	attrCat->getRelInfo(projNames->relName, relAttrCnt, relDesc);

	// convert projNames to type AttrDesc
	AttrDesc* projDesc = new AttrDesc[projCnt];
	// AttrDesc projDesc [projCnt];
	int reclen = 0;
	for(int i = 0; i < projCnt; i++) {
		attrCat->getInfo(projNames->relName, projNames[i].attrName, projDesc[i]);
		reclen += projDesc[i].attrLen;
	}

	// convert search attr to AttrDesc
	AttrDesc* desc = new AttrDesc();
	if(attr != 0) {
		attrCat->getInfo(projNames->relName, attr->attrName, *desc);
		status = ScanSelect(result, projCnt, projDesc, desc, op, attrValue, reclen);
	} else {
		// perform scan
		status = ScanSelect(result, projCnt, projDesc, 0, op, attrValue, reclen);
	}

	return status;
}


const Status ScanSelect(const string & result, 
#include "stdio.h"
#include "stdlib.h"
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;

	Status status;
	AttrDesc* relDesc;
	RID outRid;
	Record nextRec;

	// get relation info
	int relAttrCnt;
	attrCat->getRelInfo(projNames->relName, relAttrCnt, relDesc);

	// create scan
	HeapFileScan* hfs = new HeapFileScan(relDesc->relName, status);
	if(attrDesc == 0) {
		hfs->startScan(0, 0, STRING, NULL, EQ);
	} else {
		void* sendVal;
		int placeHolder;
		float placeHolderF;
		if((Datatype)attrDesc->attrType == STRING) {
			sendVal = (void*)filter;
		}
		else if((Datatype)attrDesc->attrType == INTEGER) {
			placeHolder = stoi(filter);
			sendVal = &placeHolder;
		}
		else {
			placeHolderF = stof(filter);
			sendVal = &placeHolderF;
		}
		hfs->startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype)attrDesc->attrType, (char*)sendVal, op);
		// cout << "OFFSET" <<attrDesc->attrOffset<< endl;
		// cout << "RECLEN" << reclen << endl;
		// cout << "(Datatype)attrDesc->attrType" <<(Datatype)attrDesc->attrType<< endl;
		// cout << "FILTER" << filter << endl;
		// cout << "OP" << op << endl;
	}

	// create InsertHeapFile for result
	InsertFileScan* ifs = new InsertFileScan(result, status);
	// cout << "ERROR STATUS" <<hfs->scanNext(outRid)<< endl;
	// start scan
	while(hfs->scanNext(outRid) == OK) {
		// get next record
		hfs->getRecord(nextRec);
		
		// Create new record for reordered attributes
		void* dataPointer = new char[reclen];
		Record* newRec = new Record();
		newRec->data = dataPointer;
		newRec->length = reclen;
		int offset = 0;
		//cout << nextRec.length << endl;
		//insert record
		for(int i = 0; i < projCnt; i++) {
			for(int j = 0; j < relAttrCnt; j++) {
				if(strcmp(relDesc[j].attrName, projNames[i].attrName) == 0) {
					memcpy((char*)(newRec->data)+offset, (char*)nextRec.data+relDesc[j].attrOffset, relDesc[j].attrLen);
					offset += relDesc[j].attrLen;
					break;
				}
			}
		}
		//insert record
		ifs->insertRecord(*newRec, outRid);
	}

	hfs->endScan();
	delete hfs;
	delete ifs;

	return OK;
}
