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

	//Find total record length
	int length = 0;
	for(int k =0; k < relAttrCnt; k++) {
		length += desc[k].attrLen;
	}

	// Create new record for reordered attributes
	void* dataPointer = new char[length];
	Record* newRec = new Record();
	newRec->data = dataPointer;
	newRec->length = length;

	//Reorder attributes
	for(int i = 0; i<relAttrCnt; i++) {
		for(int j = 0; j<attrCnt; j++) {
			//Check for ordering match
			if(strcmp(desc[i].attrName, attrList[j].attrName) == 0) {
				//Cast data to be written into new record
				void* sendVal;
				int placeHolder;
				float placeHolderF;
				if(desc[i].attrType == STRING) {
					sendVal = attrList[j].attrValue;
				}
				else if(desc[i].attrType == INTEGER) {
					placeHolder = stoi((char*)attrList[j].attrValue);
					sendVal = &placeHolder;
				}
				else {
					placeHolderF = stof((char*)attrList[j].attrValue);
					sendVal = &placeHolderF;
				}

				//Copy over to the new record
				memcpy((char*)(newRec->data)+desc[i].attrOffset, sendVal, desc[i].attrLen);
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
	
	// cout << "record: " << newRec->data  << endl;

	delete ifs;

	return OK;

}

