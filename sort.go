package tableorm

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/search"
)

func (db *DB) SortByField(field string, asc bool) *DB {
	var order *search.SortOrder
	if asc {
		order = search.SortOrder_ASC.Enum()
	} else {
		order = search.SortOrder_DESC.Enum()
	}

	sorter := &search.FieldSort{
		FieldName: field,
		Order:     order,
	}

	db.sorters = append(db.sorters, sorter)
	return db
}

func (db *DB) SortByPrimaryKey(asc bool) *DB {
	var order *search.SortOrder
	if asc {
		order = search.SortOrder_ASC.Enum()
	} else {
		order = search.SortOrder_DESC.Enum()
	}

	sorter := &search.PrimaryKeySort{
		Order: order,
	}

	db.sorters = append(db.sorters, sorter)
	return db
}

func (db *DB) SortByScore(asc bool) *DB {
	var order *search.SortOrder
	if asc {
		order = search.SortOrder_ASC.Enum()
	} else {
		order = search.SortOrder_DESC.Enum()
	}

	sorter := &search.ScoreSort{
		Order: order,
	}

	db.sorters = append(db.sorters, sorter)
	return db
}

func (db *DB) SortByGeoDistance(field string, points []string) *DB {
	sorter := &search.GeoDistanceSort{
		FieldName: field,
		Points: points,
	}

	db.sorters = append(db.sorters, sorter)
	return db
}
