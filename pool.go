package redisque

import (
	"time"

	"github.com/garyburd/redigo/redis"
)

const (
	_DEFAULT_POOLSIZE = 4
)

type Pool struct {
	hostport string
	auth     string
	pool     chan redis.Conn
	timeout  time.Duration
}

func NewPool(hostport, auth string, poolsize int, timeout time.Duration) (p *Pool, err error) {
	if poolsize <= 0 {
		poolsize = _DEFAULT_POOLSIZE
	}

	p = &Pool{
		hostport: hostport,
		auth:     auth,
		timeout:  timeout,
		pool:     make(chan redis.Conn, poolsize),
	}

	for i := 0; i < poolsize; i++ {
		conn, err := p.dial()
		if err != nil {
			return nil, err
		}
		p.pool <- conn
	}
	return
}

func (w *Pool) dial() (conn redis.Conn, err error) {
	conn, err = redis.DialTimeout("tcp", w.hostport, w.timeout, w.timeout, w.timeout)
	if err != nil && w.auth != "" {
		_, err = conn.Do("AUTH", w.auth)
	}
	return
}

func (w *Pool) Do(cmd string, args ...interface{}) (reply interface{}, err error) {
	conn := w.getConn()
	reply, err = conn.Do(cmd, args...)
	if err != nil && conn.Err() != nil {
		connNew, errNew := w.dial()
		if errNew == nil {
			conn.Close()
			conn = connNew
		}
	}
	w.putConn(conn)
	return
	// TODO
	// what will happen when close twice or called after closed
}

func (w *Pool) getConn() redis.Conn {
	return <-w.pool
}

func (w *Pool) putConn(conn redis.Conn) {
	w.pool <- conn
}
