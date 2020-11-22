package db

import (
	"bufio"
	"bytes"
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

	cfg := TransXLSXFileConfig{
		DBFileImport:&bytes.Buffer{},
		DBFileFuncInit :&bytes.Buffer{},
		DBFileMapData: &bytes.Buffer{},
	}
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
		err = g.TransXLSXFile(ctx, path, targetDir, pkgName, cfg)
		return err
	})

	dbBuff.WriteString("package ")
	dbBuff.WriteString(pkgName)

	dbBuff.WriteString("\n\n")
	dbBuff.WriteString("import (\n")
	dbBuff.WriteString("\t\"gorm.io/gorm\"\n")
	dbBuff.WriteString("\t\"context\"\n")
	dbBuff.WriteString("\t\"reflect\"\n")
	dbBuff.WriteString("\t\"github.com/golang/protobuf/proto\"\n")
	dbBuff.WriteString("\t\"github.com/go-redis/redis/v8\"\n")
	dbBuff.WriteString("\tprotocol \"github.com/withlin/canal-go/protocol\"\n")

	if cfg.DBFileImport.Len() > 0 {
		dbBuff.WriteString(cfg.DBFileImport.String())
	}
	dbBuff.WriteString(")\n\n")

	dbBuff.WriteString("var gdb *gorm.DB\n")
	dbBuff.WriteString("var rdb *redis.Client\n")
	dbBuff.WriteString("func SetMysqlDB(db *gorm.DB) {\n")
	dbBuff.WriteString("\tgdb = db\n")
	dbBuff.WriteString("}\n")

	dbBuff.WriteString("func SetRedisDB(db *redis.Client) {\n")
	dbBuff.WriteString("\trdb = db\n")
	dbBuff.WriteString("}\n")

	dbBuff.WriteString("func GetMysqlDB() *gorm.DB {\n")
	dbBuff.WriteString("\treturn gdb\n")
	dbBuff.WriteString("}\n")

	dbBuff.WriteString("func GetRedisDB() *redis.Client {\n")
	dbBuff.WriteString("\treturn rdb\n")
	dbBuff.WriteString("}\n")


	dbBuff.WriteString("type Data interface{\n")
	dbBuff.WriteString("\tParseCanalEntryColumns(ctx context.Context, columns []*protocol.Column) error\n")
	dbBuff.WriteString("\tDataKey() string\n")
	dbBuff.WriteString("\tSync(ctx context.Context, gdb *gorm.DB) error\n")
	dbBuff.WriteString("\tFind(ctx context.Context, rdb *redis.Client, gdb *gorm.DB) error\n")
	dbBuff.WriteString("\tFindByDB(ctx context.Context, gdb *gorm.DB) error\n")

	dbBuff.WriteString("}\n")

	if cfg.DBFileFuncInit.Len() > 0 {
		dbBuff.WriteString("func Init(gdb *gorm.DB) {\n")
		dbBuff.WriteString(cfg.DBFileFuncInit.String())
		dbBuff.WriteString("}\n")
	}

	if cfg.DBFileMapData.Len() > 0 {
		dbBuff.WriteString("var datas = map[string]reflect.Type{\n")
		dbBuff.WriteString(cfg.DBFileMapData.String())
		dbBuff.WriteString("}\n")
	}

	dbBuff.WriteString(fmt.Sprintf("func DelCache(ctx context.Context, entrys []protocol.Entry, rdb *redis.Client) {\n"))
	dbBuff.WriteString(fmt.Sprintf("\tpipe := rdb.Pipeline()\n"))
	dbBuff.WriteString(fmt.Sprintf("\tdefer pipe.Close()\n"))
	dbBuff.WriteString(fmt.Sprintf("\tfor _, entry := range entrys {\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\tentryType := entry.GetEntryType()\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\tif entryType == protocol.EntryType_TRANSACTIONBEGIN || entryType == protocol.EntryType_TRANSACTIONEND {\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\t\tcontinue\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\t}\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\ttableName := entry.GetHeader().GetTableName()\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\tdataType, ok := datas[tableName]\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\tif !ok {\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\t\tcontinue\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\t}\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\trowChange := new(protocol.RowChange)\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\tif err := proto.Unmarshal(entry.GetStoreValue(), rowChange); err != nil {\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\t\tcontinue\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\t}\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\teventType := rowChange.GetEventType()\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\tfor _, rowData := range rowChange.GetRowDatas() {\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\t\tif eventType == protocol.EventType_DELETE {\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\t\t\tval := reflect.New(dataType).Interface().(Data)\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\t\t\tif err := val.ParseCanalEntryColumns(ctx, rowData.GetBeforeColumns()); err != nil {\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\t\t\t\tcontinue\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\t\t\t}\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\t\t\tpipe.Del(ctx, val.DataKey())\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\t\t} else if eventType == protocol.EventType_INSERT {\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\t\t\tval := reflect.New(dataType).Interface().(Data)\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\t\t\tif err := val.ParseCanalEntryColumns(ctx, rowData.GetAfterColumns()); err != nil {\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\t\t\t\tcontinue\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\t\t\t}\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\t\t\tpipe.Del(ctx, val.DataKey())\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\t\t} else {\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\t\t\tval := reflect.New(dataType).Interface().(Data)\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\t\t\tif err := val.ParseCanalEntryColumns(ctx, rowData.GetBeforeColumns()); err != nil {\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\t\t\t\tcontinue\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\t\t\t}\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\t\t\tpipe.Del(ctx, val.DataKey())\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\t\t\tval = reflect.New(dataType).Interface().(Data)\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\t\t\tif err := val.ParseCanalEntryColumns(ctx, rowData.GetAfterColumns()); err != nil {\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\t\t\t\tcontinue\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\t\t\t}\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\t\t\tpipe.Del(ctx, val.DataKey())\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\t\t}\n"))
	dbBuff.WriteString(fmt.Sprintf("\t\t}\n"))
	dbBuff.WriteString(fmt.Sprintf("\t}\n"))
	dbBuff.WriteString(fmt.Sprintf("\tpipe.Exec(ctx)\n"))
	dbBuff.WriteString(fmt.Sprintf("}\n"))
	return err
}

type TransXLSXFileConfig struct {
	DBFileImport *bytes.Buffer
	DBFileFuncInit *bytes.Buffer
	DBFileMapData *bytes.Buffer
}

func (g *Gorm) TransXLSXFile(ctx context.Context, xlsxFileName string, targetDir string,
	pkgName string, cfg TransXLSXFileConfig) error {
	tables, err := g.GetTables(ctx, xlsxFileName)
	if err != nil {
		return err
	}
	for i,cnt:=0,len(tables);i<cnt;i++{
		filename := fmt.Sprintf("%s.go", SnakeName(tables[i].Name))
		if err := tables[i].Write(filepath.Join(targetDir, filename), pkgName); err != nil {
			return err
		}
		if cfg.DBFileFuncInit != nil {
			cfg.DBFileFuncInit.WriteString("\t(&")
			cfg.DBFileFuncInit.WriteString(tables[i].Name)
			cfg.DBFileFuncInit.WriteString("{}).SyncScheme(context.Background(), gdb)\n")
		}
		if cfg.DBFileMapData != nil {
			cfg.DBFileMapData.WriteString(fmt.Sprintf("\t\"%s\":reflect.TypeOf(&%s{}).Elem(),\n",
				SnakeName(tables[i].Name), CaseName(tables[i].Name)))
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

		field, err := g.parser.Parse(ctx, line)
		if err != nil {
			return err
		}
		if line.IsPrimaryKey {
			table.PriKeyNames = append(table.PriKeyNames, field.GetName())
		}
		if line.IsCacheKeyElem {
			table.CacheKeyElem = append(table.CacheKeyElem, field.GetName())
		}

		table.Fields = append(table.Fields, field)
		row++
		return nil
	})
	return table, err
}





