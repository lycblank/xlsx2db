
//Note that it is automatically generated, please do not modify

package db

import (
	"gorm.io/gorm"
	"bytes"
	"fmt"
	"context"
	"github.com/go-redis/redis/v8"
)

type User struct {
	Uid int32 `gorm:"column:uid;primaryKey;comment:用户uid"`
	Version int32 `gorm:"column:version"`
	CreateTime int64 `gorm:"column:create_time"`
	Deleted int32 `gorm:"column:deleted;comment:1:表示已删除，2:表示未删除"`
	DeleteTime int64 `gorm:"column:delete_time"`
}

func (u *User) SyncScheme(ctx context.Context, gdb *gorm.DB) error {
	return gdb.AutoMigrate(u)
}

func (User) TableName() string {
	return "user"
}

func (u *User) DataKey() string {
	var buf bytes.Buffer
	buf.WriteString("user")
	buf.WriteString(":")
	buf.WriteString(fmt.Sprint(u.Uid))
	return buf.String()
}

func (u *User) Find(ctx context.Context, rdb *redis.Client, gdb *gorm.DB) error {
	if err := u.FindByCache(ctx, rdb); err == nil {
		return nil
	}
	if err := u.FindByDB(ctx, gdb); err != nil {
		return err
	}
	_ = u.SaveCache(ctx, rdb)
	return nil
}

func (u *User) FindByDB(ctx context.Context, gdb *gorm.DB) error {
	err := gdb.First(u, u.Uid).Error
	return err
}

func (u *User) FindByCache(ctx context.Context, rdb *redis.Client) error {
	dataKey := u.DataKey()
	pipe := rdb.Pipeline()
	cmd1 := pipe.HGet(ctx, dataKey, "uid")
	if err := pipe.Process(ctx, cmd1); err != nil {
		 return err
	}
	if _, err := pipe.Exec(ctx); err != nil {
		 return err
	}
	val, _ := cmd1.Int64()
	u.Uid = int32(val)
	return nil
}

func (u *User) SaveCache(ctx context.Context, rdb *redis.Client) error {
	dataKey := u.DataKey()
	pipe := rdb.Pipeline()
	cmd1 := pipe.HSet(ctx, dataKey, "uid", u.Uid)
	if err := pipe.Process(ctx, cmd1); err != nil {
		 return err
	}
	if _, err := pipe.Exec(ctx); err != nil {
		 return err
	}
	return nil
}

func (u *User) SetReadRedisCmd(ctx context.Context, pipe *redis.Pipeline) error {
	dataKey := u.DataKey()
	cmd1 := pipe.HGet(ctx, dataKey, "uid")
	if err := pipe.Process(ctx, cmd1); err != nil {
		 return err
	}
	return nil
}

func (u *User) ParseRedisCmd(ctx context.Context, cmds []redis.Cmder) (cs []redis.Cmder, err error) {
	if len(cmds) > 0 {
		if terr := cmds[0].Err(); terr != nil {
			err = terr
		}
		val, _ := cmds[0].(*redis.StringCmd).Int64()
		u.Uid = int32(val)
		cmds = cmds[1:]
	}
	return cmds, err
}
