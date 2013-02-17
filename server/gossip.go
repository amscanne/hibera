package server

import (
	"fmt"
	"net"
)

func Serve(conn *net.UDPConn) {
	packet := make([]byte, 1024)
	for {
		n, addr, err := conn.ReadFromUDP(packet)
		if err != nil {
			continue
		}
		fmt.Printf("%d bytes received @ %s\n", n, addr)
	}
}

func RunGossip(addr string, port int) error {
	udpaddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", addr, port))
	if err != nil {
		return err
	}
	conn, err := net.ListenUDP("udp", udpaddr)
	if err != nil {
		return err
	}

	// Start up the handler.
	go Serve(conn)

	// No error occured.
	return nil
}
