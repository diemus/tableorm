package tableorm

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/search"
	"tableorm/query"
)

func NewDB(endPoint, instanceName, accessKeyId, accessKeySecret string, options ...tablestore.ClientOption) *DB {
	client := tablestore.NewClient(endPoint, instanceName, accessKeyId, accessKeySecret, options...)
	return &DB{
		client:        client,
		//没传入query时使用MatchAllQuery
		query:         query.MatchAllQuery(),
		offset:        -1,
		limit:         -1,
		getTotalCount: false,
	}
}

type DB struct {
	client        *tablestore.TableStoreClient
	query         search.Query
	offset        int
	limit         int
	getTotalCount bool
	sorters       []search.Sorter
	token         []byte
	//Collapse      *Collapse
}

func (db *DB) Query(queries ...search.Query) *DB {
	if len(queries) > 1 {
		db.query = query.And(queries...)
	} else if len(queries) == 1 {
		db.query = queries[0]
	} else {
		//没传入query时使用MatchAllQuery
		db.query = query.MatchAllQuery()
	}
	return db
}

//当需要获取的总条数小于2000行时，可以通过limit和offset进行翻页，limit+offset <= 2000。
func (db *DB) Offset(n int) *DB {
	db.offset = n
	return db
}

//当需要获取的总条数小于2000行时，可以通过limit和offset进行翻页，limit+offset <= 2000。
func (db *DB) Limit(n int) *DB {
	db.limit = n
	return db
}

func (db *DB) Token(token []byte) *DB {
	db.offset = -1
	db.limit = -1
	db.token = token
	return db
}
