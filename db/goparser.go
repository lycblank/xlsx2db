package db

import (
	"context"
	"fmt"
)

type goParser struct {

}

func NewGoParser() *goParser {
	return &goParser{}
}

func (g *goParser) Parse(ctx context.Context, line XLSXLine) (field Field, err error) {
	field.Name = CaseName(line.FieldName)
	field.TypeName = line.FieldTypeName
	// gorm tag
	gormTag := FieldTagInfo{
		Key:"gorm",
	}
	if line.IsPrimaryKey {
		gormTag.Value = fmt.Sprintf("column:%s;primaryKey;comment:%s", line.FieldName, line.FieldComment)
	} else {
		gormTag.Value = fmt.Sprintf("column:%s;comment:%s", line.FieldName, line.FieldComment)
	}
	field.TagList = append(field.TagList, gormTag)
	return field, nil
}

