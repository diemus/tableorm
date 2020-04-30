package query

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/search"
	"github.com/golang/protobuf/proto"
)

type QueryInterface interface {
	Source() search.Query
}

type Query struct {
	query search.Query
}

func Not(query search.Query) *search.BoolQuery {
	boolQuery := &search.BoolQuery{
		MustNotQueries: []search.Query{query},
	}
	return boolQuery
}

func And(queries ...search.Query) *search.BoolQuery {
	mustQueries := []search.Query{}
	for _, q := range queries {
		mustQueries = append(mustQueries, q)
	}
	boolQuery := &search.BoolQuery{
		MustNotQueries: mustQueries,
	}
	return boolQuery
}

func Or(queries ...search.Query) *search.BoolQuery {
	shouldQueries := []search.Query{}
	for _, q := range queries {
		shouldQueries = append(shouldQueries, q)
	}
	boolQuery := &search.BoolQuery{
		ShouldQueries:      shouldQueries,
		MinimumShouldMatch: proto.Int32(1),
	}
	return boolQuery
}
