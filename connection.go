package fdfs_client

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"time"
)

var ErrClosed = errors.New("pool is closed")

type PoolConn struct {
	net.Conn
	pool *ConnectionPool
}

func (c *PoolConn) Close() error {
	return c.pool.put(c.Conn)
}

type ConnectionPool struct {
	Hosts    []string
	Port     int
	MinConns int
	MaxConns int
	conns    chan net.Conn
}

func NewConnectionPool(hosts []string, port int, minConns int, maxConns int) (*ConnectionPool, error) {
	if minConns < 0 || maxConns <= 0 || minConns > maxConns {
		return nil, errors.New("invalid conns settings")
	}
	cp := &ConnectionPool{
		Hosts:    hosts,
		Port:     port,
		MinConns: minConns,
		MaxConns: maxConns,
		conns:    make(chan net.Conn, maxConns),
	}
	for i := 0; i < minConns; i++ {
		conn, err := cp.makeConn()
		if err != nil {
			cp.Close()
			return nil, err
		}
		cp.conns <- conn
	}
	return cp, nil
}

func (this *ConnectionPool) Get() (net.Conn, error) {
	if this.conns == nil {
		return nil, ErrClosed
	}
	for {
		select {
		case conn := <-this.conns:
			if conn == nil {
				break
				//return nil, ErrClosed
			}
			if err := this.activeConn(conn); err != nil {
				break
			}
			return this.wrapConn(conn), nil
		default:
			if this.Len() >= this.MaxConns {
				return nil, fmt.Errorf("Too many connctions %d", this.Len())
			}
			conn, err := this.makeConn()
			if err != nil {
				return nil, err
			}

			return this.wrapConn(conn), nil
		}
	}

}

func (this *ConnectionPool) Close() {
	conns := this.conns
	this.conns = nil

	if conns == nil {
		return
	}

	close(conns)

	for conn := range conns {
		conn.Close()
	}
}

func (this *ConnectionPool) Len() int {
	return len(this.conns)
}

func (this *ConnectionPool) makeConn() (net.Conn, error) {
	host := this.Hosts[rand.Intn(len(this.Hosts))]
	addr := fmt.Sprintf("%s:%d", host, this.Port)
	return net.DialTimeout("tcp", addr, time.Minute)
}

func (this *ConnectionPool) put(conn net.Conn) error {
	if conn == nil {
		return errors.New("connection is nil")
	}
	if this.conns == nil {
		return conn.Close()
	}

	select {
	case this.conns <- conn:
		return nil
	default:
		return conn.Close()
	}
}

func (this *ConnectionPool) wrapConn(conn net.Conn) net.Conn {
	c := PoolConn{pool: this}
	c.Conn = conn
	return &c
}

func (this *ConnectionPool) activeConn(conn net.Conn) error {
	th := &TrackerHeader{}
	th.Cmd = FDFS_PROTO_CMD_ACTIVE_TEST
	th.sendHeader(conn)
	th.recvHeader(conn)
	if th.Cmd == 100 && th.Status == 0 {
		return nil
	}
	return errors.New("Conn unaliviable")
}

func TcpRecvResponse(conn net.Conn, bufferSize int64) ([]byte, int64, error) {
	bb := bytes.NewBuffer(make([]byte, 0, bufferSize))
	total, err := io.CopyN(bb, conn, bufferSize)
	return bb.Bytes(), total, err
}
