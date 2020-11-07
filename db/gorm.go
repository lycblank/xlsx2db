package db

import (
	"bufio"
	"context"
	"fmt"
	"github.com/tealeg/xlsx/v3"
	"os"
	"path"
	"path/filepath"
	"strings"
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
	// db file
	dbfilename := path.Join(targetDir, "db.go")
	f, err := os.OpenFile(dbfilename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer f.Close()
	dbBuff := bufio.NewWriter(f)
	defer dbBuff.Flush()
	dbBuff.WriteString("package ")
	dbBuff.WriteString(pkgName)
	dbBuff.WriteString("\n\n")
	dbBuff.WriteString("import (\n")
	dbBuff.WriteString("\t\"gorm.io/gorm\"\n")
	dbBuff.WriteString("\t\"context\"\n")
	dbBuff.WriteString(")\n\n")
	dbBuff.WriteString("func Init(gdb *gorm.DB) {\n")
	err = filepath.Walk(xlsxDir, func(path string, info os.FileInfo, err error) error {
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
		err = g.TransXLSXFile(ctx, path, targetDir, pkgName, dbBuff)
		return err
	})
	dbBuff.WriteString("}\n")
	return err
}

func (g *Gorm) TransXLSXFile(ctx context.Context, xlsxFileName string, targetDir string,
	pkgName string, dbBuff *bufio.Writer) error {
	tables, err := g.GetTables(ctx, xlsxFileName)
	if err != nil {
		return err
	}
	for i,cnt:=0,len(tables);i<cnt;i++{
		filename := fmt.Sprintf("%s.go", SnakeName(tables[i].Name))
		if err := tables[i].Write(filepath.Join(targetDir, filename), pkgName); err != nil {
			return err
		}
		dbBuff.WriteString("\t(&")
		dbBuff.WriteString(tables[i].Name)
		dbBuff.WriteString("{}).SyncScheme(context.Background(), gdb)\n")
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
		fmt.Printf("export %s:%s\n", strings.Split(path.Base(xlsxFileName), ".")[0], sheet.Name)
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
	return table, err
}





