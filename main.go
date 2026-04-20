package main

import (
	"log"
	"shardingSphere-go/config"
	"shardingSphere-go/server"
)

func main() {
	// 加载配置文件
	if err := config.LoadConfig("config/config.yaml"); err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// 打印配置信息用于调试
	log.Printf("Config loaded: databaseName='%s'", config.GetDatabaseName())

	// 启动中间件服务
	if err := server.Start(":3307"); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
