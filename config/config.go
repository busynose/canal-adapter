package config

import (
	"encoding/json"
	"fmt"

	"github.com/jinzhu/configor"
)

var Env Config

type Config struct {
	APIVersion string `env:"API_VERSION" default:"Commit ID"`
	Mysql      struct {
		Host    string `default:"127.0.0.1"`
		Port    string `default:"3306"`
		DBName  string `default:"canal_demo_test"`
		User    string `default:"root"`
		Pwd     string `default:"123"`
		Charset string `default:"utf8mb4"`
	}

	Canal struct {
		IsDebugEnable bool   `default:"true"`                 // 是否开启debug模式，打印DML和DDL
		Address       string `default:"172.16.50.207"`        // canal-server的host
		Port          int    `default:"11111"`                // canal-server的端口
		UserName      string `default:"canal"`                // canal-server的用户名
		Password      string `default:"canal"`                // canal-server的密码
		Destination   string `default:"canal_demo_test"`      // instance的名称
		SoTimeOut     int32  `default:"60000"`                // 读取超时
		IdleTimeOut   int32  `default:"3600000"`              // 连接超时
		Subscribe     string `default:"canal_demo_test\\..*"` // 订阅的正则
	}
}

func Init() {
	if err := configor.Load(&Env, "config.yml"); err != nil {
		panic(err)
	}

	printJson(Env)
}

func printJson(i interface{}) {
	out, err := json.Marshal(i)
	if err != nil {
		return
	}
	fmt.Println(string(out))
}
