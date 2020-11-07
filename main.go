package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/lycblank/xlsx2db/db"
)

func main() {
	src := flag.String("s", "", "源目录")
	dst := flag.String("d", "", "目的目录")
	pkg := flag.String("pkg", "db", "包名")
	flag.Parse()
	if *src == "" {
		panic("源目录不存在")
	}
	if *dst == "" {
		panic("目的目录不存在")
	}
	g := db.NewGorm(nil)
	if err := g.TransXLSXDir(context.Background(), *src, *dst, *pkg); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("success")
	}
	return
}

