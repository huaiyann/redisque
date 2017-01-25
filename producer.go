package redisque

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/spaolacci/murmur3"
)

var (
	PRODUCER_ALREADY_STOPPED = errors.New("Producer already stopped.")
	PUB_CHAN_BLOCKED_ERR     = errors.New("Pub chan blocked to timeout.")
	DATA_TOO_LONG            = errors.New(fmt.Sprintf("Data should not longer than %d bytes.", _DEFAULT_MAX_DATA_SIZE))
)

const (
	_DEFAULT_IN_CHAN_SIZE           = 102400
	_DEFAULT_LPUSH_BATCH_SIZE       = 300
	_DEFAULT_LPUSH_BATCH_BYTES_SIZE = 5242880 // 5MB
	_DEFAULT_REDIS_POOL_EXTEND_SIZE = 3
	_DEFAULT_MAX_DATA_SIZE          = 4294967295 // max uint32
)

type Producer struct {
	goroutineCnt   int
	batchSize      int
	batchBytesSize int
	timeout        time.Duration
	timebatch      time.Duration

	pool *Pool

	chans   []chan PubData
	stopSig bool

	waitPublish  sync.WaitGroup
	waitRunLpush sync.WaitGroup
}

type PubData struct {
	queue   string
	payload []byte
}

func NewProducer(hostport, auth string, poolsize, batchsize int, timeout, timebatch time.Duration) (p *Producer, err error) {
	if poolsize <= 0 {
		poolsize = 1
	}
	if batchsize <= 0 {
		batchsize = _DEFAULT_LPUSH_BATCH_SIZE
	}
	if timebatch < 10*time.Millisecond {
		timebatch = 10 * time.Millisecond
	}
	rand.Seed(time.Now().UnixNano())

	p = &Producer{
		goroutineCnt:   poolsize,
		batchSize:      batchsize,
		batchBytesSize: _DEFAULT_LPUSH_BATCH_BYTES_SIZE,
		timeout:        timeout,
		timebatch:      timebatch,
	}

	p.pool, err = NewPool(hostport, auth, poolsize+_DEFAULT_REDIS_POOL_EXTEND_SIZE, timeout)
	if err != nil {
		return
	}

	for i := 0; i < poolsize; i++ {
		c := make(chan PubData, _DEFAULT_IN_CHAN_SIZE)
		p.chans = append(p.chans, c)
		go p.runLpush(c)
	}
	return
}

func (p *Producer) Stop() {
	p.stopSig = true
	p.waitPublish.Wait()
	for _, c := range p.chans {
		close(c)
	}
	p.waitRunLpush.Wait()
}

func (p *Producer) Publish(queue string, data []byte) (err error) {
	return p.publish(queue, data, false)
}

func (p *Producer) PublishDisorderAndFaster(queue string, data []byte) (err error) {
	return p.publish(queue, data, true)
}

func (p *Producer) publish(queue string, data []byte, disorderAndFaster bool) (err error) {
	p.waitPublish.Add(1)
	defer p.waitPublish.Done()
	if p.stopSig {
		return PRODUCER_ALREADY_STOPPED
	}

	if len(data) > _DEFAULT_MAX_DATA_SIZE {
		return DATA_TOO_LONG
	}

	var target int
	if disorderAndFaster {
		target = rand.Intn(p.goroutineCnt)
	} else {
		target = int(murmur3.Sum32([]byte(queue))) % p.goroutineCnt
	}

	select {
	case p.chans[target] <- PubData{queue, data}:
	default:
		select {
		case p.chans[target] <- PubData{queue, data}:
		case <-time.After(p.timeout):
			return PUB_CHAN_BLOCKED_ERR
		}
	}
	return
}

func (p *Producer) runLpush(c chan PubData) {
	p.waitRunLpush.Add(1)
	defer p.waitRunLpush.Done()

	ticker := time.NewTicker(p.timebatch)
	datas := make(map[string][][]byte)
	bytesSize := make(map[string]int)
	for {
		select {
		case data, ok := <-c:
			if !ok {
				goto RETURN
			}
			datas[data.queue] = append(datas[data.queue], data.payload)
			bytesSize[data.queue] += len(data.payload)
			if len(datas[data.queue]) >= p.batchSize || bytesSize[data.queue] >= p.batchBytesSize {
				err := p.doLpush(data.queue, datas[data.queue])
				if err != nil {
				}
				datas[data.queue] = nil
				bytesSize[data.queue] = 0
			}
		case <-ticker.C:
			for k, v := range datas {
				if len(v) > 0 {
					err := p.doLpush(k, v)
					if err != nil {
						log.Println(err)
					}
					datas[k] = nil
					bytesSize[k] = 0
				}
			}
		}
	}
RETURN:
	for k, v := range datas {
		if len(v) > 0 {
			err := p.doLpush(k, v)
			if err != nil {
				log.Println(err)
			}
			datas[k] = nil
			bytesSize[k] = 0
		}
	}
}

func (p *Producer) doLpush(queue string, data [][]byte) (err error) {
	transData := &TransData{
		pkts: data,
		ts:   time.Now().Unix(),
	}
	payload, err := marshal(transData)
	if err != nil {
		return
	}
	_, err = p.pool.Do("LPUSH", queue, payload)
	if err != nil {
		return
	}
	return
}
