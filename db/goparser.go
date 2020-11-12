package db

import (
	"bytes"
	"context"
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
	var buf bytes.Buffer
	buf.WriteString("column:")
	buf.WriteString(line.FieldName)
	if line.IsPrimaryKey {
		buf.WriteString(";primaryKey")
	}

	if line.IsAutoIncrement {
		buf.WriteString(";autoIncrement")
	}

	if line.UniqueIndexName != "" {
		buf.WriteString(";not null;uniqueIndex:")
		buf.WriteString(line.UniqueIndexName)
	}
	if line.DBFieldTypeName != "" {
		buf.WriteString(";")
		buf.WriteString(line.DBFieldTypeName)
	}

	if line.FieldComment != "" {
		buf.WriteString(";comment:")
		buf.WriteString(line.FieldComment)
	}

	gormTag.Value = buf.String()
	field.TagList = append(field.TagList, gormTag)

	// jsonTag
	jsonTag := FieldTagInfo{
		Key:"json",
	}
	jsonTag.Value = line.FieldName
	field.TagList = append(field.TagList, jsonTag)
	return field, nil
}

