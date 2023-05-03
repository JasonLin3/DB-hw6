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
	// get desc of relation
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

	// Insert attributes in order
	for(int i = 0; i<relAttrCnt; i++) {
		for(int j = 0; j<attrCnt; j++) {
			// Find index of next projected attribute
			if(strcmp(desc[i].attrName, attrList[j].attrName) == 0) {
				//Cast data to be written into new record
				void* sendVal;
				int placeHolderI;
				float placeHolderF;
				//Assign void pointer to string for memcpy
				if(desc[i].attrType == STRING) {
					sendVal = attrList[j].attrValue;
				}
				//Convert value from string to integer
				//Assign void pointer to point at integer for use in memcpy
				else if(desc[i].attrType == INTEGER) {
					placeHolderI = stoi((char*)attrList[j].attrValue);
					sendVal = &placeHolderI;
				}
				//Convert value from string to float
				//Assign void pointer to point at float for use in memcpy
				else {
					placeHolderF = stof((char*)attrList[j].attrValue);
					sendVal = &placeHolderF;
				}

				//Copy value over to the new record
				memcpy((char*)(newRec->data)+desc[i].attrOffset, sendVal, desc[i].attrLen);
				break;
			}
		}
	}

	// create InsertFileScan for inserting the new record into the relation
	Status status;
	InsertFileScan* ifs = new InsertFileScan(relation, status);
	if(status != OK) {
		return status;
	}


	// insert record into relation
	RID rid;
	status = ifs->insertRecord(*newRec, rid);
	if(status != OK) {
		return status;
	}


	delete ifs;

	return OK;

}

