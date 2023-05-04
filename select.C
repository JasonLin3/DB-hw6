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
	int relAttrCnt;
	AttrDesc* relDesc;
	int reclen = 0;

	// get desc of relation
	attrCat->getRelInfo(projNames->relName, relAttrCnt, relDesc);

	// create array of AttrDesc
	AttrDesc* projDesc = new AttrDesc[projCnt];
	for(int i = 0; i < projCnt; i++) { // loop through projection names
		// convert projName to type AttrDesc
		attrCat->getInfo(projNames->relName, projNames[i].attrName, projDesc[i]);
		// count length of total record
		reclen += projDesc[i].attrLen;
	}

	// if search attribute exists
	if(attr != 0) {
		// convert search attr to AttrDesc
		AttrDesc* desc = new AttrDesc();
		attrCat->getInfo(projNames->relName, attr->attrName, *desc);
		// initialize scanner
		status = ScanSelect(result, projCnt, projDesc, desc, op, attrValue, reclen);
	} else { // if no search attribute
		// initialize scanner
		status = ScanSelect(result, projCnt, projDesc, 0, op, attrValue, reclen);
	}

	return status;
}

/*
 * Scans specified relation and puts them into the result relation.
 * Uses filtering to find all records matching selection criteria.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */ 
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

	// get desc of relation
	int relAttrCnt;
	attrCat->getRelInfo(projNames->relName, relAttrCnt, relDesc);

	// create HeapFileScan with relation
	HeapFileScan* hfs = new HeapFileScan(relDesc->relName, status);


	//Create variables for data casting
	void* sendVal;
	int placeHolderI;
	float placeHolderF;

	// if no filter attr
	if(attrDesc == 0) {
		hfs->startScan(0, 0, STRING, NULL, EQ);
	} else { // if filter attr exists
		//Data casting
		//Assign void pointer to string attribute value for use in memcpy
		if((Datatype)attrDesc->attrType == STRING) {
			sendVal = (void*)filter;
		}
		//Convert string attribute value to integer
		//Assign void pointer to point at integer for use in memcpy
		else if((Datatype)attrDesc->attrType == INTEGER) {
			placeHolderI = stoi(filter);
			sendVal = &placeHolderI;
		}
		//Convert string attribute value to float
		//Assign void pointer to point at float for use in memcpy
		else {
			placeHolderF = stof(filter);
			sendVal = &placeHolderF;
		}
		// initialize scanner
		hfs->startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype)attrDesc->attrType, (char*)sendVal, op);
	}

	// create InsertFileScan for inserting to result table
	InsertFileScan* ifs = new InsertFileScan(result, status);
	
	// scan until no more matching records
	while(hfs->scanNext(outRid) == OK) {
		// get next record
		hfs->getRecord(nextRec);
		
		// Create new record for reordered attributes
		void* dataPointer = new char[reclen];
		Record* newRec = new Record();
		newRec->data = dataPointer;
		newRec->length = reclen;

		//Create offset for locations in new record
		int offset = 0;
		
		// create new record for result table
		for(int i = 0; i < projCnt; i++) {
			for(int j = 0; j < relAttrCnt; j++) {
				// Find index of next projected attribute
				if(strcmp(relDesc[j].attrName, projNames[i].attrName) == 0) {
					//Copy attribute data into new record
					memcpy((char*)(newRec->data)+offset, (char*)nextRec.data+relDesc[j].attrOffset, relDesc[j].attrLen);
					//Update location of next data added to new record
					offset += relDesc[j].attrLen;
					break;
				}
			}
		}
		
		// insert record
		status = ifs->insertRecord(*newRec, outRid);
		if(status != OK) {
			return status;
		}
	}

	// cleanup
	hfs->endScan();
	delete hfs;
	delete ifs;

	return OK;
}
