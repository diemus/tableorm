package tableorm

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

//批量保存，并返回保存后的对象，方便获取ID
func (db *DB) Save(objList ...interface{}) ([]interface{}, error) {
	batchWriteReq := &tablestore.BatchWriteRowRequest{}

	for _, obj := range objList {
		rowChange, err := GetSaveRowChange(obj)
		if err != nil {
			return nil, err
		}
		batchWriteReq.AddRowChange(rowChange)
	}

	resp, err := db.client.BatchWriteRow(batchWriteReq)
	if err != nil {
		return nil, err
	}

	return objList, GetBatchWriteResult(resp)
}

func (db *DB) Delete(objList ...interface{}) error {
	batchWriteReq := &tablestore.BatchWriteRowRequest{}

	for _, obj := range objList {
		rowChange, err := GetDeleteRowChange(obj)
		if err != nil {
			return err
		}
		batchWriteReq.AddRowChange(rowChange)
	}

	resp, err := db.client.BatchWriteRow(batchWriteReq)
	if err != nil {
		return err
	}

	return GetBatchWriteResult(resp)
}
