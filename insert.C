#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
// part 6
	// get relation info
	int relAttrCnt;
	AttrDesc* desc;
	attrCat->getRelInfo(relation, relAttrCnt, desc);

	// reorder attributes
	Record* newRec = new Record();
	for(int i = 0; i<relAttrCnt; i++) {
		for(int j = 0; j<attrCnt; j++) {
			if(desc[i].attrName == attrList[j].attrName) {
				memcpy(newRec+desc[i].attrOffset, attrList[j].attrValue, attrList[j].attrLen);
				break;
			}
		}
	}

	// create InsertHeapFile for result
	Status status;
	RID rid;
	InsertFileScan* ifs = new InsertFileScan(relation, status);

	//insert record
	ifs->insertRecord(*newRec, rid);

	delete ifs;

	return OK;

}

