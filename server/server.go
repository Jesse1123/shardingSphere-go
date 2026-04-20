package server

import (
	"log"
	"net"
	"shardingSphere-go/protocol"
)

func Start(address string) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	defer listener.Close()

	log.Printf("Server started on %s", address)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Connection error: %v", err)
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	log.Printf("New connection from %s", conn.RemoteAddr())

	// MySQL 协议握手
	if err := protocol.HandleHandshake(conn); err != nil {
		log.Printf("Handshake error: %v", err)
		return
	}

	// 进入命令阶段，处理查询/心跳/退出
	if err := protocol.RunSession(conn); err != nil {
		log.Printf("Session closed for %s: %v", conn.RemoteAddr(), err)
	}
}
