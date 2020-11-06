package db

import (
	"context"
	"fmt"
	"github.com/tealeg/xlsx/v3"
	"os"
	"path/filepath"
)

type Gorm struct {
	parser XLSXParser
}

func NewGorm(parser XLSXParser) *Gorm {
	g := &Gorm{
		parser:parser,
	}
	if parser == nil {
		g.parser = NewGoParser()
	}
	return g
}

func (g *Gorm) TransXLSXDir(ctx context.Context, xlsxDir string, targetDir string, pkgName string) error {
	err := filepath.Walk(xlsxDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info == nil || info.IsDir() {
			return nil
		}
		fname := info.Name()
		if fname == "" || fname[0] == '~' { // 临时文件直接忽略掉
			return nil
		}
		ext := filepath.Ext(fname)
		if ext != ".xlsx" && ext != ".xls" { // 不是excel文件
			return nil
		}
		err = g.TransXLSXFile(ctx, path, targetDir, pkgName)
		return err
	})
	return err
}

func (g *Gorm) TransXLSXFile(ctx context.Context, xlsxFileName string, targetDir string, pkgName string) error {
	tables, err := g.GetTables(ctx, xlsxFileName)
	if err != nil {
		return err
	}
	for i,cnt:=0,len(tables);i<cnt;i++{
		filename := fmt.Sprintf("%s.go", SnakeName(tables[i].Name))
		if err := tables[i].Write(filepath.Join(targetDir, filename), pkgName); err != nil {
			return err
		}
	}
	return nil
}

func (g *Gorm) GetTables(ctx context.Context, xlsxFileName string) ([]Table, error) {
	f, err := xlsx.OpenFile(xlsxFileName)
	if err != nil {
		return nil, err
	}
	tables := make([]Table, 0, len(f.Sheets))
	for _, sheet := range f.Sheets {
		table, err := g.getTable(ctx, sheet)
		if err != nil {
			return tables, err
		}
		tables = append(tables, table)
	}
	return tables, nil
}

func (g *Gorm) getTable(ctx context.Context, sheet *xlsx.Sheet) (Table, error) {
	table := Table{
		Name:   CaseName(sheet.Name),
		Fields: make([]Field, 0, 16),
	}
	var row int
	err := sheet.ForEachRow(func(r *xlsx.Row) error {
		defer func() {
			row++
		}()
		if row == 0 { // 第1行过滤掉
			return nil
		}
		line := GetXLSXLine(r)
		if g.parser == nil {
			return nil
		}
		if line.FieldName == "" {
			return nil
		}
		if line.IsPrimaryKey {
			table.PriKeyNames = append(table.PriKeyNames, CaseName(line.FieldName))
		}
		if line.IsCacheKeyElem {
			table.CacheKeyElem = append(table.CacheKeyElem, CaseName(line.FieldName))
		}

		field, err := g.parser.Parse(ctx, line)
		if err != nil {
			return err
		}
		table.Fields = append(table.Fields, field)
		row++
		return nil
	})
	fmt.Println(table)
	return table, err
}





