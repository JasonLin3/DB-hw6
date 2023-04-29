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

	// get relation info
	int relAttrCnt;
	int reclen;
	AttrDesc* relDesc;
	attrCat->getRelInfo(result, relAttrCnt, relDesc);
	for(int i = 0; i<relAttrCnt; i++) {
		reclen += relDesc[i].attrLen;
	}

	cout << "AH2"<< endl; 

	// convert projNames to type AttrDesc
	AttrDesc projDesc[projCnt];
	for(int i = 0; i < projCnt; i++) {
		attrCat->getInfo(result, projNames[i].attrName, projDesc[i]);
	}

	// convert search attr to AttrDesc
	AttrDesc* desc;
	if(attr != 0) {
		attrCat->getInfo(result, attr->attrName, *desc);
		cout << "YAHHHHH"<< endl;
		ScanSelect(result, projCnt, projDesc, desc, op, attrValue, reclen);
	} else {
		// perform scan
		cout << "AH4"<< endl; 
		ScanSelect(result, projCnt, projDesc, 0, op, attrValue, reclen);
	}
	

	
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
	cout << attrDesc << endl;
	if(attrDesc == 0) {
		cout << "DEFAULT START SCAN " << endl;
		hfs->startScan(0, 0, STRING, NULL, EQ);
	} else {
		hfs->startScan(attrDesc->attrOffset, reclen, (Datatype)attrDesc->attrType, filter, op);
		cout << "OFFSET" <<attrDesc->attrOffset<< endl;

		cout << "RECLEN" << reclen << endl;

		cout << "(Datatype)attrDesc->attrType" <<(Datatype)attrDesc->attrType<< endl;

		cout << "FILTER" << filter << endl;
		cout << "OP" << op << endl;
	}

	// create InsertHeapFile for result
	InsertFileScan* ifs = new InsertFileScan(result, status);
	cout << "ERROR STATUS" <<hfs->scanNext(outRid)<< endl;
	// start scan
	while(hfs->scanNext(outRid) == OK) {
		cout<< "STARTED SCAN"<<endl;
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
