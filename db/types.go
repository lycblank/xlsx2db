package db

import (
	"bytes"
	"fmt"
)

type XLSXLine struct {
	FieldName string
	FieldTypeName string
	DBFieldTypeName string
	FieldComment string
	IsPrimaryKey bool
	IsCacheKeyElem bool
	IsAutoIncrement bool
	UniqueIndexName string
}

type Field struct {
	Name string
	TypeName string
	TagList []FieldTagInfo
	line XLSXLine
}

func (f Field) String() string {
	return fmt.Sprintf("%s %s `%s`", f.GetName(), f.TypeName, FileTagInfoList(f.TagList))
}

func (f Field) GetName() string {
	prefix := ""
	if f.line.IsPrimaryKey {
		prefix += "Pri"
	}
	if f.line.IsCacheKeyElem {
		prefix += "Cache"
	}
	return prefix+f.Name
}

type FieldTagInfo struct {
	Key string
	Value string
}
func (ft FieldTagInfo) String() string {
	return fmt.Sprintf("%s:\"%s\"", ft.Key, ft.Value)
}

type FileTagInfoList []FieldTagInfo
func (f FileTagInfoList) String() string {
	var buf bytes.Buffer
	for i,cnt:=0,len(f); i<cnt;i++{
		buf.WriteString(f[i].String())
		if i < cnt - 1 {
			buf.WriteString(" ")
		}
	}
	return buf.String()
}

