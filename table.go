package tableorm

import (
	"fmt"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"log"
	"reflect"
)

//创建表，表名为结构体小写，主键约定为 _id string
func (db *DB) CreateTable(obj interface{}) error {
	tableMeta := new(tablestore.TableMeta)
	tableMeta.TableName = GetTableName(obj)
	tableMeta.AddPrimaryKeyColumn("_id", tablestore.PrimaryKeyType_STRING)

	tableOption := new(tablestore.TableOption)
	tableOption.TimeToAlive = -1
	tableOption.MaxVersion = 1

	//预留吞吐量按小时收费，因此设为0，不预留
	reservedThroughput := new(tablestore.ReservedThroughput)
	reservedThroughput.Readcap = 0
	reservedThroughput.Writecap = 0

	createTableRequest := new(tablestore.CreateTableRequest)
	createTableRequest.TableMeta = tableMeta
	createTableRequest.TableOption = tableOption
	createTableRequest.ReservedThroughput = reservedThroughput

	_, err := db.client.CreateTable(createTableRequest)
	if err != nil {
		return err
	}

	return nil
}

//删除表
func (db *DB) DeleteTable(obj interface{}) error {
	deleteReq := new(tablestore.DeleteTableRequest)
	deleteReq.TableName = GetTableName(obj)
	_, err := db.client.DeleteTable(deleteReq)
	if err != nil {
		return err
	}

	return nil
}

//根据tag创建索引，默认所有字段都创建对应类型的索引，可以通过index tag覆盖默认
func (db *DB) CreateIndex(obj interface{}) error {
	request := &tablestore.CreateSearchIndexRequest{}
	tableName := GetTableName(obj)
	schemas, err := CreateIndexSchema(obj)
	if err != nil {
		return err
	}

	request.TableName = tableName
	request.IndexName = fmt.Sprintf("%s_index", tableName)
	request.IndexSchema = &tablestore.IndexSchema{
		FieldSchemas: schemas,
	}

	_, err = db.client.CreateSearchIndex(request)
	if err != nil {
		return err
	}
	return nil
}

func (db *DB) DeleteIndex(obj interface{}) error {
	request := &tablestore.DeleteSearchIndexRequest{}
	tableName := GetTableName(obj)
	request.TableName = tableName
	request.IndexName = fmt.Sprintf("%s_index", tableName)
	_, err := db.client.DeleteSearchIndex(request)
	if err != nil {
		return err
	}
	return nil
}

//查询相关的表是否创建
func (db *DB) isTableExist(obj interface{}) (bool, error) {
	tables, err := db.client.ListTable()
	if err != nil {
		return false, err
	}

	for _, table := range tables.TableNames {
		if table == GetTableName(obj) {
			return true, nil
		}
	}

	return false, nil
}

//查询相关的表的索引是否创建
func (db *DB) isIndexExist(obj interface{}) (bool, error) {
	tableName := GetTableName(obj)
	request := &tablestore.ListSearchIndexRequest{}
	request.TableName = tableName
	resp, err := db.client.ListSearchIndex(request)
	if err != nil {
		return false, err
	}

	for _, index := range resp.IndexInfo {
		if index.IndexName == fmt.Sprintf("%s_index", tableName) {
			return true, nil
		}
	}

	return false, nil
}

//对比schema是否变更
func (db *DB) isIndexSchemaChange(obj interface{}) (bool, error) {
	tableName := GetTableName(obj)
	request := &tablestore.DescribeSearchIndexRequest{}
	request.TableName = tableName
	request.IndexName = fmt.Sprintf("%s_index", tableName)
	resp, err := db.client.DescribeSearchIndex(request)
	if err != nil {
		return false, err
	}

	currentSchema := resp.Schema.FieldSchemas
	targetSchema, err := CreateIndexSchema(obj)
	if err != nil {
		return false, err
	}

	return !reflect.DeepEqual(currentSchema, targetSchema), nil
}

//自动根据结构体创建或者更新表和索引
func (db *DB) AutoMigrate(models ...interface{}) {
	for _, obj := range models {
		err := db.syncModel(obj)
		if err != nil {
			return
		}
	}
}

//同步model，如果不存在则创建，存在则检查索引是否有变动，有变动删除重建
func (db *DB) syncModel(obj interface{}) error {
	//先检查模型是否定义正确
	err := CheckModel(obj)
	if err != nil {
		return err
	}

	tableName := GetTableName(obj)
	log.Printf("start sync model %s", tableName)
	defer log.Printf("end sync model %s", tableName)

	tableExist, err := db.isTableExist(obj)
	if err != nil {
		log.Printf("check table %s error %s", tableName, err)
		return err
	}

	//表不存在直接创建表，已经存在就不作修改了
	if !tableExist {
		log.Printf("table not exist, create table %s", tableName)
		err := db.CreateTable(obj)
		if err != nil {
			log.Printf("create table %s error %s", tableName, err)
			return err
		}
	} else {
		log.Printf("table %s exist", tableName)
	}

	//检查索引是否已经创建
	indexExist, err := db.isIndexExist(obj)
	if err != nil {
		log.Printf("check index %s_index error %s", tableName, err)
		return err
	}

	//索引与表不一样，在存在索引的情况下，直接删除旧的索引，创建新的，同步最新索引信息
	//因为索引无法修改，只能删除重建
	if indexExist {
		log.Printf("index %s_index already exist", tableName)

		//对比索引schema，查看是否有变动
		schemaChanged, err := db.isIndexSchemaChange(obj)
		if err != nil {
			log.Printf("check index %s_index schema error %s", tableName, err)
			return err
		}

		//schema有变化，删除重建
		if schemaChanged {
			log.Printf("index %s_index schema changed, recreate index", tableName)
			err = db.DeleteIndex(obj)
			if err != nil {
				log.Printf("delete index %s_index error %s", tableName, err)
				return err
			}
			log.Printf("delete index %s_index success", tableName)
		} else {
			log.Printf("index %s_index schema not changed", tableName)
			return nil
		}
	} else {
		log.Printf("index %s_index not exist", tableName)
	}

	log.Printf("create index %s_index", tableName)
	err = db.CreateIndex(obj)
	if err != nil {
		log.Printf("create index %s_index error %s", tableName, err)
		return err
	}

	return nil
}
