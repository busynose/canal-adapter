package main

import (
	"canal-adapter/config"
	"canal-adapter/service"
	"log"
	"os"
	"time"

	"gitlab.com/makeblock-go/mysql/v2"
	"gorm.io/gorm/logger"
)

const (
	batchSize            = 100 // 每次请求binlog数量
	sleepTimeMillisecond = 300 // 请求间隔
)

func main() {
	config.Init()
	cnf := mysql.NewConfig(
		config.Env.Mysql.User,
		config.Env.Mysql.Pwd,
		config.Env.Mysql.Host,
		config.Env.Mysql.Port,
		config.Env.Mysql.DBName,
		config.Env.Mysql.Charset,
		logger.Info)
	mysql.Register(cnf)
	db := mysql.GetDB()

	// 创建服务对象
	rdbServiceInstance := service.NewRdbMirrorService(db, config.Env.Canal.Address, config.Env.Canal.Port, config.Env.Canal.UserName, config.Env.Canal.Password, config.Env.Canal.Destination, config.Env.Canal.SoTimeOut, config.Env.Canal.IdleTimeOut, config.Env.Canal.IsDebugEnable)
	// 创建canal连接
	if err := rdbServiceInstance.Connect(); err != nil {
		panic(err)
	}
	// 订阅消息
	if err := rdbServiceInstance.Subscribe("canal_demo_test\\..*"); err != nil {
		panic(err)
	}
	// 读取binlog消息
	for {
		message, err := rdbServiceInstance.Get(batchSize, nil, nil)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}
		batchId := message.Id
		if batchId == -1 || len(message.Entries) <= 0 {
			time.Sleep(sleepTimeMillisecond * time.Millisecond)
			continue
		}
		// 同步dml和ddl语句
		rdbServiceInstance.Mirror(message.Entries)
	}
}
