package db

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

type Table struct {
	Name string
	Fields []Field
	PriKeyNames []string
	CacheKeyElem []string
}

func (t Table) Write(filename string, pkgName string) error {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer file.Close()
	return t.write(file, pkgName)
}

func (t Table) write(writer io.Writer, pkgName string) error {
	buf := bufio.NewWriter(writer)
	defer buf.Flush()
	// comment
	buf.WriteString("\n//Note that it is automatically generated, please do not modify\n\n")

	// package
	buf.WriteString("package ")
	buf.WriteString(pkgName)
	buf.WriteString("\n\n")

	// import
	buf.WriteString("import (\n")
	buf.WriteString("\t\"gorm.io/gorm\"\n")
	buf.WriteString("\t\"bytes\"\n")
	buf.WriteString("\t\"fmt\"\n")
	buf.WriteString("\t\"context\"\n")
	buf.WriteString("\t\"strconv\"\n")
	buf.WriteString("\t\"github.com/go-redis/redis/v8\"\n")
	buf.WriteString("\tprotocol \"github.com/withlin/canal-go/protocol\"\n")
	buf.WriteString(")\n\n")

	// struct
	buf.WriteString(`type `)
	buf.WriteString(t.Name)
	buf.WriteString(" struct {\n")
	for i,cnt:=0,len(t.Fields); i < cnt; i++ {
		buf.WriteString("\t")
		buf.WriteString(t.Fields[i].String())
		buf.WriteString("\n")
	}
	buf.WriteString("\tVersion int32 `gorm:\"column:version\" json:\"version\"`\n")
	buf.WriteString("\tCreateTime int64 `gorm:\"column:create_time\" json:\"create_time\"`\n")
	buf.WriteString("\tDeleted int32 `gorm:\"column:deleted;comment:1:表示已删除，0:表示未删除\" json:\"deleted\"`\n")
	buf.WriteString("\tDeleteTime int64 `gorm:\"column:delete_time\"json:\"delete_time\"`\n")
	buf.WriteString("}\n")

	shortName := SnakeName(t.Name[:1])

	// SyncScheme
	buf.WriteString(fmt.Sprintf("\nfunc (%s *%s) SyncScheme(ctx context.Context, gdb *gorm.DB) error {\n", shortName, t.Name))
	buf.WriteString(fmt.Sprintf("\treturn gdb.AutoMigrate(%s)\n", shortName))
	buf.WriteString(fmt.Sprintf("}\n"))

	// TableName
	buf.WriteString(fmt.Sprintf("\nfunc (%s) TableName() string {\n", t.Name))
	buf.WriteString(fmt.Sprintf("\treturn \"%s\"\n", SnakeName(t.Name)))
	buf.WriteString(fmt.Sprintf("}\n"))

	// DataKey
	buf.WriteString(fmt.Sprintf("\nfunc (%s *%s) DataKey() string {\n", shortName, t.Name))
	buf.WriteString(fmt.Sprintf("\tvar buf bytes.Buffer\n"))
	buf.WriteString(fmt.Sprintf("\tbuf.WriteString(\"%s\")\n", SnakeName(t.Name)))
	for _, elem := range t.CacheKeyElem {
		buf.WriteString(fmt.Sprintf("\tbuf.WriteString(\":\")\n"))
		buf.WriteString(fmt.Sprintf("\tbuf.WriteString(fmt.Sprint(%s.%s))\n", shortName, elem))
	}
	buf.WriteString(fmt.Sprintf("\treturn buf.String()\n"))
	buf.WriteString(fmt.Sprintf("}\n"))

	// OK
	buf.WriteString(fmt.Sprintf("\nfunc (%s *%s) OK() bool {\n", shortName, t.Name))
	buf.WriteString(fmt.Sprintf("\treturn %s.Deleted == 0\n", shortName))
	buf.WriteString(fmt.Sprintf("}\n"))

	// Sync
	buf.WriteString(fmt.Sprintf("\nfunc (%s *%s) Sync(ctx context.Context, gdb *gorm.DB) error {\n", shortName, t.Name))
	buf.WriteString(fmt.Sprintf("\treturn gdb.Save(%s).Error\n", shortName))
	buf.WriteString(fmt.Sprintf("}\n"))

	// Find
	buf.WriteString(fmt.Sprintf("\nfunc (%s *%s) Find(ctx context.Context, rdb *redis.Client, gdb *gorm.DB) error {\n", shortName, t.Name))
	buf.WriteString(fmt.Sprintf("\tif err := %s.FindByCache(ctx, rdb); err == nil {\n", shortName))
	buf.WriteString(fmt.Sprintf("\t\treturn nil\n"))
	buf.WriteString(fmt.Sprintf("\t}\n"))
	buf.WriteString(fmt.Sprintf("\tif err := %s.FindByDB(ctx, gdb); err != nil {\n", shortName))
	buf.WriteString(fmt.Sprintf("\t\treturn err\n"))
	buf.WriteString(fmt.Sprintf("\t}\n"))
	buf.WriteString(fmt.Sprintf("\t_ = %s.SaveCache(ctx, rdb)\n", shortName))
	buf.WriteString(fmt.Sprintf("\treturn nil\n"))
	buf.WriteString(fmt.Sprintf("}\n"))

	// FindByDB根据主键查询
	buf.WriteString(fmt.Sprintf("\nfunc (%s *%s) FindByDB(ctx context.Context, gdb *gorm.DB) error {\n", shortName, t.Name))
	//keys := make([]string, 0, len(t.PriKeyNames))
	//for _, key := range t.PriKeyNames {
	//	keys = append(keys, fmt.Sprintf("%s.%s", shortName, key))
	//}
	buf.WriteString(fmt.Sprintf("\terr := gdb.Where(%s).First(%s).Error\n", shortName, shortName))
	buf.WriteString(fmt.Sprintf("\treturn err\n"))
	buf.WriteString(fmt.Sprintf("}\n"))

	// FindByCache
	buf.WriteString(fmt.Sprintf("\nfunc (%s *%s) FindByCache(ctx context.Context, rdb *redis.Client) error {\n", shortName, t.Name))
	buf.WriteString(fmt.Sprintf("\tdataKey := %s.DataKey()\n", shortName))
	buf.WriteString(fmt.Sprintf("\tpipe := rdb.Pipeline()\n"))
	for idx, field := range t.Fields {
		buf.WriteString(fmt.Sprintf("\tcmd%d := pipe.HGet(ctx, dataKey, \"%s\")\n", idx+1, SnakeName(field.Name)))
	}
	buf.WriteString(fmt.Sprintf("\tif _, err := pipe.Exec(ctx); err != nil {\n"))
	buf.WriteString(fmt.Sprintf("\t\t return err\n"))
	buf.WriteString(fmt.Sprintf("\t}\n"))
	for idx, field := range t.Fields {
		switch field.TypeName {
		case "string":
			buf.WriteString(fmt.Sprintf("\t%s.%s = cmd%d.Val()\n", shortName, field.Name, idx+1))
		case "int64":
			buf.WriteString(fmt.Sprintf("\t%s.%s, _ = cmd%d.Int64()\n", shortName, field.Name, idx+1))
		case "int32":
			buf.WriteString(fmt.Sprintf("\tval, _ := cmd%d.Int64()\n", idx+1))
			buf.WriteString(fmt.Sprintf("\t%s.%s = int32(val)\n", shortName, field.Name))
		}
	}
	buf.WriteString(fmt.Sprintf("\treturn nil\n"))
	buf.WriteString(fmt.Sprintf("}\n"))

	// SaveCache
	buf.WriteString(fmt.Sprintf("\nfunc (%s *%s) SaveCache(ctx context.Context, rdb *redis.Client) error {\n", shortName, t.Name))
	buf.WriteString(fmt.Sprintf("\tdataKey := %s.DataKey()\n", shortName))
	buf.WriteString(fmt.Sprintf("\tpipe := rdb.Pipeline()\n"))
	for _, field := range t.Fields {
		buf.WriteString(fmt.Sprintf("\tpipe.HSet(ctx, dataKey, \"%s\", %s.%s)\n", SnakeName(field.Name), shortName, field.Name))
	}
	buf.WriteString(fmt.Sprintf("\tif _, err := pipe.Exec(ctx); err != nil {\n"))
	buf.WriteString(fmt.Sprintf("\t\t return err\n"))
	buf.WriteString(fmt.Sprintf("\t}\n"))
	buf.WriteString(fmt.Sprintf("\treturn nil\n"))
	buf.WriteString(fmt.Sprintf("}\n"))

	// SetReadRedisCmd
	buf.WriteString(fmt.Sprintf("\nfunc (%s *%s) SetReadRedisCmd(ctx context.Context, pipe *redis.Pipeline) error {\n", shortName, t.Name))
	buf.WriteString(fmt.Sprintf("\tdataKey := %s.DataKey()\n", shortName))
	for _, field := range t.Fields {
		buf.WriteString(fmt.Sprintf("\tpipe.HGet(ctx, dataKey, \"%s\")\n", SnakeName(field.Name)))
	}
	buf.WriteString(fmt.Sprintf("\treturn nil\n"))
	buf.WriteString(fmt.Sprintf("}\n"))

	// ParseRedisCmd
	buf.WriteString(fmt.Sprintf("\nfunc (%s *%s) ParseRedisCmd(ctx context.Context, cmds []redis.Cmder) (cs []redis.Cmder, err error) {\n", shortName, t.Name))
	for _, field := range t.Fields {
		buf.WriteString(fmt.Sprintf("\tif len(cmds) > 0 {\n"))
		buf.WriteString(fmt.Sprintf("\t\tif terr := cmds[0].Err(); terr != nil {\n"))
		buf.WriteString(fmt.Sprintf("\t\t\terr = terr\n"))
		buf.WriteString(fmt.Sprintf("\t\t}\n"))
		switch field.TypeName {
		case "string":
			buf.WriteString(fmt.Sprintf("\t\t%s.%s = cmds[0].(*redis.StringCmd).Val()\n", shortName, field.Name))
		case "int64":
			buf.WriteString(fmt.Sprintf("\t\t%s.%s, _ = cmds[0].(*redis.StringCmd).Int64()\n", shortName, field.Name))
		case "int32":
			buf.WriteString(fmt.Sprintf("\t\tval, _ := cmds[0].(*redis.StringCmd).Int64()\n"))
			buf.WriteString(fmt.Sprintf("\t\t%s.%s = int32(val)\n", shortName, field.Name))
		}
		buf.WriteString(fmt.Sprintf("\t\tcmds = cmds[1:]\n"))
		buf.WriteString(fmt.Sprintf("\t}\n"))
	}
	buf.WriteString(fmt.Sprintf("\treturn cmds, err\n"))
	buf.WriteString(fmt.Sprintf("}\n"))

	// ParseCanalEntryColumns
	buf.WriteString(fmt.Sprintf("\nfunc (%s *%s) ParseCanalEntryColumns(ctx context.Context, columns []*protocol.Column) error {\n", shortName, t.Name))
	//buf.WriteString(fmt.Sprintf("\tif entry.GetEntryType() == protocol.EntryType_TRANSACTIONBEGIN || entry.GetEntryType() == protocol.EntryType_TRANSACTIONEND {\n"))
	//buf.WriteString(fmt.Sprintf("\t\treturn errors.New(\"entry type is trans header\")\n"))
	//buf.WriteString(fmt.Sprintf("\t}\n"))
	//buf.WriteString(fmt.Sprintf("\trowChange := new(protocol.RowChange)\n"))
	//buf.WriteString(fmt.Sprintf("\terr := proto.Unmarshal(entry.GetStoreValue(), rowChange)\n"))
	//buf.WriteString(fmt.Sprintf("\tif err != nil {\n"))
	//buf.WriteString(fmt.Sprintf("\t\treturn err\n"))
	//buf.WriteString(fmt.Sprintf("\t}\n"))
	//buf.WriteString(fmt.Sprintf("\teventType := rowChange.GetEventType()\n"))
	//buf.WriteString(fmt.Sprintf("\tfor _, rowData := range rowChange.GetRowDatas() {\n"))
	//buf.WriteString(fmt.Sprintf("\t\tswitch eventType {\n"))
	//buf.WriteString(fmt.Sprintf("\t\t\tcase protocol.EventType_DELETE:\n"))
	//buf.WriteString(fmt.Sprintf("\t\t\tcase protocol.EventType_INSERT:\n"))
	//buf.WriteString(fmt.Sprintf("\t\t\tdefault:\n"))
	//buf.WriteString(fmt.Sprintf("\t\t}\n"))
	buf.WriteString(fmt.Sprintf("\tfor _, col := range columns {\n"))
	buf.WriteString(fmt.Sprintf("\t\tk,v := col.GetName(), col.GetValue()\n"))
	buf.WriteString(fmt.Sprintf("\t\tswitch k {\n"))
	for _, field := range t.Fields {
		buf.WriteString(fmt.Sprintf("\t\tcase \"%s\":\n", SnakeName(field.Name)))
		if field.TypeName == "string" {
			buf.WriteString(fmt.Sprintf("\t\t\t%s.%s = v\n", shortName, CaseName(field.Name)))
		} else if field.TypeName == "int32" || field.TypeName == "int64" {
			buf.WriteString(fmt.Sprintf("\t\t\ttmp, _ := strconv.ParseInt(v, 10, 64)\n"))
			buf.WriteString(fmt.Sprintf("\t\t\t%s.%s = %s(tmp)\n", shortName, CaseName(field.Name), field.TypeName))
		} else {
			panic(fmt.Sprintf("%s not support", field.TypeName))
		}
	}
	buf.WriteString(fmt.Sprintf("\t\tcase \"version\":\n"))
	buf.WriteString(fmt.Sprintf("\t\t\ttmp, _ := strconv.ParseInt(v, 10, 64)\n"))
	buf.WriteString(fmt.Sprintf("\t\t\t%s.Version = int32(tmp)\n", shortName))

	buf.WriteString(fmt.Sprintf("\t\tcase \"create_time\":\n"))
	buf.WriteString(fmt.Sprintf("\t\t\ttmp, _ := strconv.ParseInt(v, 10, 64)\n"))
	buf.WriteString(fmt.Sprintf("\t\t\t%s.CreateTime = tmp\n", shortName))

	buf.WriteString(fmt.Sprintf("\t\tcase \"deleted\":\n"))
	buf.WriteString(fmt.Sprintf("\t\t\ttmp, _ := strconv.ParseInt(v, 10, 64)\n"))
	buf.WriteString(fmt.Sprintf("\t\t\t%s.Deleted = int32(tmp)\n", shortName))

	buf.WriteString(fmt.Sprintf("\t\tcase \"delete_time\":\n"))
	buf.WriteString(fmt.Sprintf("\t\t\ttmp, _ := strconv.ParseInt(v, 10, 64)\n"))
	buf.WriteString(fmt.Sprintf("\t\t\t%s.DeleteTime = tmp\n", shortName))

	buf.WriteString(fmt.Sprintf("\t\t}\n"))
	buf.WriteString(fmt.Sprintf("\t}\n"))
	buf.WriteString(fmt.Sprintf("\treturn nil\n"))
	buf.WriteString("}\n")
	return nil
}





