package db

import (
	"context"
	"github.com/tealeg/xlsx/v3"
)

type XLSXParser interface {
	Parse(ctx context.Context, line XLSXLine) (field Field, err error)
}

func GetXLSXLine(r *xlsx.Row) XLSXLine {
	line := XLSXLine{}
	var num int
	r.ForEachCell(func(c *xlsx.Cell) error{
		switch num {
		case 0:
			line.FieldName = c.Value
		case 1:
			line.FieldTypeName = c.Value
		case 2:
			val := c.Value
			if c.Value == "" {
				val = line.FieldTypeName
			}
			line.DBFieldTypeName = val
		case 3:
			line.IsPrimaryKey = c.Value == "1"
		case 4:
			line.IsCacheKeyElem = c.Value == "1"
		case 5:
			line.FieldComment = c.Value
		}
		num++
		return nil
	})
	return line
}