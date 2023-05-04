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
	int relAttrCnt;

	// get desc of relation
	attrCat->getRelInfo(relation, relAttrCnt, relDesc);

	// get AttrDesc for search attribute
	attrCat->getInfo(relation, attrName, attrDesc);

	// create heap file scan on active relation
	HeapFileScan* hfs = new HeapFileScan(relation, status);

	//Create variables for data casting
	void* sendVal;
	int placeHolderI;
	float placeHolderF;

	// Handle delete for no specified search attribute
	if(attrName.empty()){
		// initalize scanner
		hfs->startScan(0,0,STRING, NULL, EQ);
	} else { // handle delete with search attribute
		//Cast data for usage in filter
		//Assign void pointer to string attribute value for use in memcpy
		if(type == STRING) {
			sendVal = (void*)attrValue;
		}
		//Convert string attribute value to integer
		//Assign void pointer to point at integer for use in memcpy
		else if(type == INTEGER) {
			placeHolderI = stoi(attrValue);
			sendVal = &placeHolderI;
		}
		//Convert string attribute value to float
		//Assign void pointer to point at float for use in memcpy
		else {
			placeHolderF = stof(attrValue);
			sendVal = &placeHolderF;
		}
		// initialize scanner
		hfs->startScan(attrDesc.attrOffset, attrDesc.attrLen, type, (char*)sendVal, op);
	}

	// scan next record until no more records
	while(hfs->scanNext(outRid) == OK) {
		// get next record
		hfs->getRecord(nextRec);
		// delete
		status = hfs->deleteRecord();
		if(status != OK) {
			return status;
		}
	}

	//Cleanup
	hfs->endScan();
	delete hfs;

	return OK;

}


