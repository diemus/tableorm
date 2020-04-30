package tableorm

import (
	"fmt"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/search"
	"reflect"
	"tableorm/query"
)

func (db *DB) Count(num *int) error {
	db.limit = 0
	db.offset = -1
	db.getTotalCount = true
	return nil
}

//只有First，如果需要Last，则自行将排序反过来，即可获取最后一个
func (db *DB) First(obj interface{}) error {
	db.limit = 1

	resp, err := db.search(GetTableName(obj), true)
	if err != nil {
		return err
	}

	if err := LoadData(obj, resp.Rows[0]); err != nil {
		return err
	}
	return nil
}

func (db *DB) Find(obj interface{}) error {
	typ := reflect.TypeOf(obj)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
		if typ.Kind() != reflect.Slice {
			return fmt.Errorf("not a slice")
		}
	} else {
		return fmt.Errorf("not a pointer")
	}

	//根据传入类型动态创建一个空slice
	result := reflect.MakeSlice(reflect.SliceOf(typ.Elem()), 0, 0)

	tableName := GetTableName(reflect.New(typ.Elem()).Interface())
	resp, err := db.search(tableName, true)
	if err != nil {
		return err
	}

	//将row转换为对应结构，插入result
	for _, row := range resp.Rows {
		item := reflect.New(typ.Elem()).Interface()
		if err := LoadData(item, row); err != nil {
			return err
		}
		result = reflect.Append(result, reflect.ValueOf(item).Elem())
	}

	//将obj指向result
	reflect.ValueOf(obj).Elem().Set(result)
	return nil
}

func (db *DB) FindByToken(obj interface{}, token []byte) (nextToken []byte, err error) {
	db.offset = -1
	db.sorters = nil
	db.token = token
	return []byte(""), nil
}

func (db *DB) search(tableName string, getColumns bool) (*tablestore.SearchResponse, error) {
	//前置检查，防止传参错误
	if err := db.checkRequest(); err != nil {
		return nil, err
	}

	//构造searchQuery
	searchQuery := search.NewSearchQuery()
	searchQuery.SetQuery(db.query)
	searchQuery.SetLimit(int32(db.limit))
	searchQuery.SetOffset(int32(db.offset))
	searchQuery.SetToken(db.token)
	searchQuery.SetSort(&search.Sort{Sorters: db.sorters})
	//searchQuery.SetCollapse(true)
	searchQuery.SetGetTotalCount(db.getTotalCount)

	//通过obj提取表名，索引名默认为tableName_index
	searchRequest := &tablestore.SearchRequest{}
	searchRequest.SetTableName(tableName)
	searchRequest.SetIndexName(fmt.Sprintf("%s_index", tableName))
	searchRequest.SetSearchQuery(searchQuery)
	//是否返回所有列，delete时只需要返回主键即可
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll: getColumns,
	})

	//重置请求条件为默认值，防止影响下次请求
	db.reset()

	//发出请求
	resp, err := db.client.Search(searchRequest)
	if err != nil {
		return resp, err
	}

	//后置检查，检查结果是否满足要求
	if err := db.checkResponse(resp); err != nil {
		return resp, err
	}

	return resp, nil
}

//前置检查，防止传参错误
func (db *DB) checkRequest() error {
	//_id是否存在
	//是否有不是int64的
	return nil
}

//后置检查，检查结果是否满足要求
func (db *DB) checkResponse(resp *tablestore.SearchResponse) error {
	//结果为空时报错，保证结果是大于一个的
	if len(resp.Rows) == 0 {
		return NotResultFound
	}

	//没有全部成功时报错
	if !resp.IsAllSuccess {
		return NotAllSuccess
	}
	return nil
}

//请求后重置条件，防止影响下次请求
func (db *DB) reset() {
	db.limit = -1
	db.offset = -1
	db.query = query.MatchAllQuery()
	db.getTotalCount = false
	db.sorters = nil
	db.token = nil
}
