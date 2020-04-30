package tableorm

import (
	"fmt"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/golang/protobuf/proto"
	"github.com/oleiade/reflections"
	uuid "github.com/satori/go.uuid"
	"reflect"
	"strings"
)

var (
	tagToIndexTypeMap = map[string]tablestore.FieldType{
		"bool":   tablestore.FieldType_BOOLEAN,
		"float":  tablestore.FieldType_DOUBLE,
		"string": tablestore.FieldType_KEYWORD,
		"int":    tablestore.FieldType_LONG,
		"nested": tablestore.FieldType_NESTED,
		"text":   tablestore.FieldType_TEXT,
		"geo":    tablestore.FieldType_GEO_POINT,
	}

	kindToIndexTypeMap = map[reflect.Kind]tablestore.FieldType{
		reflect.Bool:    tablestore.FieldType_BOOLEAN,
		reflect.Float64: tablestore.FieldType_DOUBLE,
		reflect.String:  tablestore.FieldType_KEYWORD,
		reflect.Int64:   tablestore.FieldType_LONG,
		//reflect.Uint8:   tablestore.FieldType_KEYWORD, //[]bytes?
	}
)

func GetTableName(obj interface{}) string {
	v := reflect.Indirect(reflect.ValueOf(obj))
	return strings.ToLower(v.Type().Name())
}

func CreateIndexSchema(obj interface{}) ([]*tablestore.FieldSchema, error) {
	schemas := []*tablestore.FieldSchema{}

	fieldToJSONMap, _, err := GetFieldNameMap(obj)
	if err != nil {
		return nil, err
	}

	fields, err := reflections.Fields(obj)
	if err != nil {
		return nil, err
	}

	for _, field := range fields {
		// _id为约定主键，不能创建索引 / json未导出字段也设置索引
		if v := fieldToJSONMap[field]; v == "_id" || v == "" {
			continue
		}
		//标记为"-"则不创建索引
		tag, _ := reflections.GetFieldTag(obj, field, "index")
		if tag == "-" {
			continue
		}

		var fieldType tablestore.FieldType
		var ok bool
		if tag != "" {
			//指定了索引类型，则按指定类型设置
			fieldType, ok = tagToIndexTypeMap[tag]
			if !ok {
				return nil, fmt.Errorf("unexpected field tag %s %s", field, tag)
			}
		} else {
			//其余的默认根据字段类型推断索引类型
			kind, _ := reflections.GetFieldKind(obj, field)
			fieldType, ok = kindToIndexTypeMap[kind]
			if !ok {
				return nil, fmt.Errorf("unexpected field kind %s %s", field, kind)
			}
		}
		schemas = append(schemas, &tablestore.FieldSchema{
			FieldName:        proto.String(fieldToJSONMap[field]),
			FieldType:        fieldType,
			Index:            proto.Bool(true),
			EnableSortAndAgg: proto.Bool(true),
			Store:            proto.Bool(true),
		})
	}

	return schemas, nil
}

func GetFieldNameMap(obj interface{}) (map[string]string, map[string]string, error) {
	tags, err := reflections.Tags(obj, "json")
	if err != nil {
		return nil, nil, err
	}

	fieldToJSONMap := map[string]string{}
	jsonToFieldMap := map[string]string{}
	for field, tag := range tags {
		//json tag可有有2个字段，逗号分隔，只取第一个字段。
		if jsonName := strings.Split(tag, ",")[0]; jsonName != "-" {
			fieldToJSONMap[field] = strings.Split(tag, ",")[0]
			jsonToFieldMap[strings.Split(tag, ",")[0]] = field
		}
	}
	return fieldToJSONMap, jsonToFieldMap, nil
}

func LoadData(obj interface{}, row *tablestore.Row) error {
	_, jsonToFieldMap, err := GetFieldNameMap(obj)
	if err != nil {
		return fmt.Errorf("get field map err: %w", err)
	}

	for _, pk := range row.PrimaryKey.PrimaryKeys {
		err = reflections.SetField(obj, jsonToFieldMap[pk.ColumnName], pk.Value)
		if err != nil {
			//TODO:可以更详细 target type receive type
			return err
		}
	}

	for _, attr := range row.Columns {
		err = reflections.SetField(obj, jsonToFieldMap[attr.ColumnName], attr.Value)
		if err != nil {
			return err
		}
	}

	return err
}

//检查模型定义是否符合要求，防止出错
func CheckModel(obj interface{}) error {
	items, err := reflections.Items(obj)
	if err != nil {
		return err
	}

	isIDFieldExist := false

	//检查字段类型是否符合TableStore要求，目前仅支持int64,float64,string,[]byte,bool
	for field, value := range items {
		//只检查包含json tag的字段，因为只有这些字段会存入数据库
		tag, _ := reflections.GetFieldTag(obj, field, "json")
		if tag != "" && strings.Split(tag, ",")[0] != "-" {
			//标注"_id"是否存在
			if strings.Split(tag, ",")[0] == "_id" {
				isIDFieldExist = true
			}

			//类型检查
			switch value.(type) {
			case int64, float64, string, []byte, bool:
			default:
				return fmt.Errorf("field %s must be one of (int64,float64,string,[]byte,bool), it's %s now", field, reflect.ValueOf(value).Type())
			}
		}
	}

	//"_id"是否存在
	if !isIDFieldExist {
		return IDFieldNotExist
	}

	//TODO: 索引是否设置正确

	return err
}

//检查ID是否为空，为空自动生成一个uuid
func EnsureID(obj interface{}) (string, error) {
	_, jsonToFieldMap, err := GetFieldNameMap(obj)
	if err != nil {
		return "", err
	}

	fieldName, ok := jsonToFieldMap["_id"]
	if !ok {
		return "", fmt.Errorf("error finding Field Name for _id")
	}

	valueID, err := reflections.GetField(obj, fieldName)
	if err != nil {
		return "", err
	}

	id := valueID.(string)
	if reflect.ValueOf(id).IsZero() {
		id = uuid.NewV4().String()
		err := reflections.SetField(obj, fieldName, id)
		if err != nil {
			return "", err
		}
	}

	return id, err
}

func GetSaveRowChange(obj interface{}) (*tablestore.PutRowChange, error) {
	//没有ID则创建ID，有则忽略
	_, err := EnsureID(obj)
	if err != nil {
		return nil, err
	}

	fields, err := reflections.Fields(obj)
	if err != nil {
		return nil, err
	}

	fieldToJSONMap, _, err := GetFieldNameMap(obj)
	if err != nil {
		return nil, err
	}

	putRowChange := new(tablestore.PutRowChange)

	for _, field := range fields {
		value, _ := reflections.GetField(obj, field)
		column := fieldToJSONMap[field]
		if column == "_id" {
			pk := new(tablestore.PrimaryKey)
			pk.AddPrimaryKeyColumn(column, value)
			putRowChange.PrimaryKey = pk
		} else if column != "" && strings.Split(column, "-")[0] != "-" {
			putRowChange.AddColumn(column, value)
		} else {
			continue
		}
	}

	putRowChange.TableName = GetTableName(obj)
	putRowChange.SetCondition(tablestore.RowExistenceExpectation_IGNORE)
	return putRowChange, nil
}

func GetDeleteRowChange(obj interface{}) (*tablestore.DeleteRowChange, error) {
	id, _ := EnsureID(obj)

	pk := new(tablestore.PrimaryKey)
	pk.AddPrimaryKeyColumn("_id", id)

	deleteRowChange := new(tablestore.DeleteRowChange)
	deleteRowChange.TableName = GetTableName(obj)
	deleteRowChange.PrimaryKey = pk
	deleteRowChange.SetCondition(tablestore.RowExistenceExpectation_IGNORE)

	return deleteRowChange, nil
}

//判断是否部分失败，一般概率较小，因为所有请求都是统一格式的且为Ignore，不容易有这种错误
//但是一旦发生不好排查，因为pk是空的，无法确认是那个对象失败了
func GetBatchWriteResult(resp *tablestore.BatchWriteRowResponse) error {
	for _, results := range resp.TableToRowsResult {
		for _, result := range results {
			if !result.IsSucceed {
				return fmt.Errorf("write row error, error: %s", result.Error)
			}
		}
	}

	return nil
}
