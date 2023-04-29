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

	// convert search attr to AttrDesc
	AttrDesc desc;
	attrCat->getInfo(result, attr->attrName, desc);

	// convert projNames to type AttrDesc
	AttrDesc projDesc[projCnt];
	for(int i = 0; i < projCnt; i++) {
		attrCat->getInfo(result, projNames[i].attrName, projDesc[i]);
	}

	// perform scan
	ScanSelect(result, projCnt, projDesc, &desc, op, attrValue, desc.attrLen);
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
	attrCat->getRelInfo(result, relAttrCnt, relDesc);

	// create scan
	HeapFileScan* hfs = new HeapFileScan(result, status);
	hfs->startScan(0, reclen, (Datatype)attrDesc->attrType, filter, op);

	// create InsertHeapFile for result
	InsertFileScan* ifs = new InsertFileScan(result, status);

	// start scan
	while(hfs->scanNext(outRid) == OK) {
		// get next record
		hfs->getRecord(nextRec);

		// project 
		Record* newRec = new Record();
		int offset = 0;

		//insert record
		for(int i = 0; i < projCnt; i++) {
			for(int j = 0; j < relAttrCnt; j++) {
				if(relDesc[i].attrName == projNames[j].attrName) {
					// ADD TYPE CAST
					memcpy(newRec+offset, (char*)nextRec.data+relDesc[i].attrOffset, relDesc[i].attrLen);
					offset += relDesc[i].attrLen;
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
