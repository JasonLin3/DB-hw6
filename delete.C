#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{
// part 6

	Status status;
	AttrDesc* relDesc;
	AttrDesc attrDesc;
	RID outRid;
	Record nextRec;

	// get relation info
	int relAttrCnt;
	attrCat->getRelInfo(relation, relAttrCnt, relDesc);
	attrCat->getInfo(relation, attrName, attrDesc);

	// create scan
	HeapFileScan* hfs = new HeapFileScan(relation, status);
	if(attrName.empty()){
		hfs->startScan(0,0,STRING, NULL, EQ);
	} else {
		void* sendVal;
		int placeHolder;
		float placeHolderF;
		if(type == STRING) {
			sendVal = (void*)attrValue;
		}
		else if(type == INTEGER) {
			placeHolder = stoi(attrValue);
			sendVal = &placeHolder;
		}
		else {
			placeHolderF = stof(attrValue);
			sendVal = &placeHolderF;
		}
		hfs->startScan(attrDesc.attrOffset, attrDesc.attrLen, type, (char*)sendVal, op);
	}

	// start scan
	while(hfs->scanNext(outRid) == OK) {
		// get next record
		hfs->getRecord(nextRec);
		// delete
		hfs->deleteRecord();
		
	}

	hfs->endScan();

	delete hfs;

	return OK;

}


