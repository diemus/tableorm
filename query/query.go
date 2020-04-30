package query

import "github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/search"

func TermQuery(field string, term interface{}) *search.TermQuery {
	query := &search.TermQuery{
		FieldName: field,
		Term:      term,
	}
	return query
}

func TermsQuery(field string, terms ...interface{}) *search.TermsQuery {
	query := &search.TermsQuery{
		FieldName: field,
		Terms:     terms,
	}
	return query
}

func RangeQuery(field string,operator string, value interface{}) *search.RangeQuery {
	query := &search.RangeQuery{
		FieldName: field,
	}

	switch operator {
	case ">":
		query.GT(value)
	case "<":
		query.LT(value)
	case ">=":
		query.GTE(value)
	case "<=":
		query.LTE(value)
	}

	return query
}

func MatchAllQuery() *search.MatchAllQuery {
	query := &search.MatchAllQuery{}
	return query
}

func MatchQuery(field, text string, minimumShouldMatch *int32, operator *search.QueryOperator) *search.MatchQuery {
	query := &search.MatchQuery{
		FieldName:          field,
		Text:               text,
		MinimumShouldMatch: minimumShouldMatch,
		Operator:           operator,
	}
	return query
}

func MatchPhraseQuery(field, text string) *search.MatchPhraseQuery {
	query := &search.MatchPhraseQuery{
		FieldName: field,
		Text:      text,
	}
	return query
}

func PrefixQuery(field, prefix string) *search.PrefixQuery {
	query := &search.PrefixQuery{
		FieldName: field,
		Prefix:    prefix,
	}
	return query
}

func WildcardQuery(field, value string) *search.WildcardQuery {
	query := &search.WildcardQuery{
		FieldName: field,
		Value:     value,
	}
	return query
}

func NestedQuery(path string, query Query, scoreMode search.ScoreModeType) *search.NestedQuery {
	q := &search.NestedQuery{
		Path:      path,
		Query:     query.query,
		ScoreMode: scoreMode,
	}
	return q
}

/*
地理长方形范围查询
地理边界框查询。根据一个矩形范围的地理位置边界条件查询表中的数据，当一个地理位置点落在给出的矩形范围内时，满足查询条件。

参数
fieldName：字段名。
topLeft：矩形框的左上角的坐标。
bottomRight：矩形框的右下角的坐标，通过左上角和右下角就可以确定一个唯一的矩形。格式：“纬度，经度”，纬度在前，经度在后，例如“35.8,-45.91”。
*/
func GeoBoundingBoxQuery(field, topLeft, bottomRight string) *search.GeoBoundingBoxQuery {
	q := &search.GeoBoundingBoxQuery{
		FieldName:   field,
		TopLeft:     topLeft,
		BottomRight: bottomRight,
	}
	return q
}

/*
地理距离查询
根据一个中心点和距离条件查询表中的数据，当一个地理位置点到指定的中心点的距离不超过指定的值时，满足查询条件。

参数
fieldName：字段名。
centerPoint：中心地理坐标点，是一个经纬度值。格式：“纬度，经度”，纬度在前，经度在后。例如“35.8,-45.91”坐标点，是一个经纬度值。
distanceInMeter：Double类型，距离中心点的距离，单位是米。
*/
func GeoDistanceQuery(field, centerPoint string, distanceInMeter float64) *search.GeoDistanceQuery {
	q := &search.GeoDistanceQuery{
		FieldName:       field,
		CenterPoint:     centerPoint,
		DistanceInMeter: distanceInMeter,
	}
	return q
}

/*
地理多边形范围查询
根据一个多边形范围条件查询表中的数据，当一个地理位置点落在指定的多边形内时满足查询条件。

参数
fieldName：字段名。
points：组成多边形的距离坐标点。格式为“纬度,经度”，纬度在前，经度在后，例如“35.8,-45.91”。
*/
func GeoPolygonQuery(field string, points []string) *search.GeoPolygonQuery {
	q := &search.GeoPolygonQuery{
		FieldName: field,
		Points:    points,
	}
	return q
}
