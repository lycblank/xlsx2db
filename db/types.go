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
}

type Field struct {
	Name string
	TypeName string
	TagList []FieldTagInfo
}

func (f Field) String() string {
	return fmt.Sprintf("%s %s `%s`", f.Name, f.TypeName, FileTagInfoList(f.TagList))
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

