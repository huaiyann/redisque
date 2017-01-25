/*
 * Copyright (C) Zhiyu Yin
 * Copyright (C) xiaomi.com
 */

package redisque

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

const (
	_DEFAULT_OUT_CHAN_SIZE = 4096
)

var (
	QUEUE_ALREADY_SUBSCRIBED_ERR = errors.New("Queue already subscribed.")
	CONSUMER_ALREADY_STOPPED     = errors.New("Consumer already stopped.")
)

type Consumer struct {
	stopSig  bool
	stopChan chan bool
	outChans map[string]chan []byte
	chanLock sync.RWMutex
	wg       sync.WaitGroup
	pool     *Pool
}

func NewConsumer(hostport, auth string, poolsize int, timeout time.Duration) (c *Consumer, err error) {
	c = &Consumer{
		stopChan: make(chan bool),
		outChans: make(map[string]chan []byte),
	}

	c.pool, err = NewPool(hostport, auth, poolsize, timeout)

	return
}

// Sub the coded data which is sended by the Producer{}'s Publish()
func (c *Consumer) Subscribe(queue string, goroutineCnt int) (out chan []byte, err error) {
	return c.subscribe(queue, goroutineCnt)
}

func (c *Consumer) subscribe(queue string, goroutineCnt int) (out chan []byte, err error) {
	if c.stopSig {
		return nil, CONSUMER_ALREADY_STOPPED
	}

	c.chanLock.RLock()
	out = c.outChans[queue]
	c.chanLock.RUnlock()
	if out != nil {
		err = QUEUE_ALREADY_SUBSCRIBED_ERR
		return
	}

	out = make(chan []byte, _DEFAULT_OUT_CHAN_SIZE)
	c.chanLock.Lock()
	c.outChans[queue] = out
	c.chanLock.Unlock()

	if goroutineCnt < 1 {
		goroutineCnt = 1
	}
	for i := 0; i < goroutineCnt; i++ {
		c.wg.Add(1)
		go func(queue string, out chan []byte) {
			defer c.wg.Done()

			for !c.stopSig {
				payload, err := redis.Bytes(c.pool.Do("RPOP", queue))
				if err == redis.ErrNil {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				if err != nil {
					continue
				}

				transData, err := unmarshal(payload)
				if err != nil {
					log.Println(err)
					continue
				}

				for _, pkt := range transData.pkts {
					select {
					case out <- pkt:
					case <-c.stopChan:
						// some pkts maybe dropped if the out chan blocked when stopping
						return
					}
				}
			}
		}(queue, out)
	}
	return
}

func (c *Consumer) GetOutChan(queue string) (out chan []byte, ok bool) {
	c.chanLock.RLock()
	out, ok = c.outChans[queue]
	c.chanLock.RUnlock()
	return
}

func (c *Consumer) Stop() {
	c.stopSig = true
	close(c.stopChan)
	c.wg.Wait()

	c.chanLock.Lock()
	for _, c := range c.outChans {
		close(c)
	}
	c.chanLock.Unlock()
}
