package redisque

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestRedisqueBase(t *testing.T) {
	consumer, err := NewConsumer("127.0.0.1:6379", "", 4, time.Second*5)
	if err != nil {
		t.Errorf("Need a useable redis: %v", err)
	}
	producer, err := NewProducer("127.0.0.1:6379", "", 4, 5, time.Second*10, time.Millisecond*5)
	if err != nil {
		t.Errorf("Need a useable redis: %v", err)
	}

	err = producer.Publish("TestRedisqueBase", []byte("data"))
	if err != nil {
		t.Errorf("Publish fail: %v", err)
	}

	out, err := consumer.Subscribe("TestRedisqueBase", 1)
	if err != nil {
		t.Errorf("Subscribe fail: %v", err)
	}

	select {
	case data := <-out:
		if string(data) != "data" {
			t.Errorf("Data unexpected: %s", string(data))
		}
	case <-time.After(time.Second * 2):
		t.Errorf("No data readed.")
	}

	producer.Stop()
	consumer.Stop()
}

func TestRedisqueCount(t *testing.T) {
	consumer, err := NewConsumer("127.0.0.1:6379", "", 4, time.Second*5)
	if err != nil {
		t.Errorf("Need a useable redis: %v", err)
	}
	producer, err := NewProducer("127.0.0.1:6379", "", 4, 5, time.Second*10, time.Millisecond*5)
	if err != nil {
		t.Errorf("Need a useable redis: %v", err)
	}

	var pubCnt, subCnt int64
	go func() {
		for {
			err = producer.PublishDisorderAndFaster("TestRedisqueCount", []byte("data"))
			if err != nil {
				return
			}
			pubCnt++
		}
	}()

	<-time.After(time.Millisecond * 100)
	producer.Stop()

	_, err = consumer.Subscribe("TestRedisqueCount", 1)
	if err != nil {
		t.Errorf("Subscribe fail: %v", err)
	}

	out, ok := consumer.GetOutChan("TestRedisqueCount")
	if !ok {
		t.Errorf("Subscribe get out chan fail.")
	}

	for {
		select {
		case data, ok := <-out:
			if !ok {
				// check count when consumer stopped
				goto CHECK
			}
			if string(data) != "data" {
				t.Errorf("Data unexpected: %s", string(data))
			}
			atomic.AddInt64(&subCnt, 1)
		case <-time.After(time.Second):
			// stop the consumer when queue empty
			consumer.Stop()
		}
	}

CHECK:
	if subCnt != pubCnt {
		t.Errorf("Pub count %d not equal Sub count %d", pubCnt, subCnt)
	}
}
