package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/lycblank/xlsx2db/db"
)

func main() {
	g := db.NewGorm(nil)
	fmt.Println(g.TransXLSXDir(context.Background(), "sample/data", "sample/go", "db"))
	return
	rdb := redis.NewClient(&redis.Options{
		Addr:     "10.10.104.223:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	ctx := context.Background()
	pipe := rdb.Pipeline()

	cmd1 := pipe.HGet(ctx, "hello", "world")
	cmd2 := pipe.HGet(ctx, "hello", "world")
	cmd3 := pipe.HGet(ctx, "hello", "world")
	cmd4 := pipe.HGet(ctx, "hello", "world")
	pipe.Process(ctx, cmd1)
	pipe.Process(ctx, cmd2)
	pipe.Process(ctx, cmd3)
	pipe.Process(ctx, cmd4)


	cmd5 := pipe.HGet(ctx, "blank", "good")
	cmd6 := pipe.HGet(ctx, "blank", "good1")
	cmd7 := pipe.HGet(ctx, "blank", "good2")
	cmd8 := pipe.HGet(ctx, "blank", "good3")
	pipe.Process(ctx, cmd5)
	pipe.Process(ctx, cmd6)
	pipe.Process(ctx, cmd7)
	pipe.Process(ctx, cmd8)
	pipe.Exec(ctx)
	//fmt.Println(err)
	fmt.Println(cmd1.String())
	fmt.Println(cmd2.String())
	fmt.Println(cmd3.String())
	fmt.Println(cmd4.String())
	fmt.Println(cmd5.String())
	fmt.Println(cmd6.String())
	fmt.Println(cmd7.String())
	fmt.Println(cmd8.String())
}

