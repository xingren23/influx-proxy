package backend

import (
	"fmt"
	"log"

	"github.com/joncrlsn/dque"
)

type QueueItem struct {
	Data []byte
}

func QueueItemBuilder() interface{} {
	return &QueueItem{}
}

type QueueBackend struct {
	name        string
	path        string
	segmentSize int
	turbo       bool
	queue       *dque.DQue
}

func NewQueueBackend(name, path string, turbo bool) (qb *QueueBackend, err error) {
	qb = &QueueBackend{
		name:        name,
		path:        path,
		segmentSize: 10000,
		turbo:       turbo,
	}

	qb.queue, err = dque.NewOrOpen(qb.name, qb.path, qb.segmentSize, QueueItemBuilder)
	if err != nil {
		log.Print("Error creating new dque ", err)
		return
	}

	if qb.turbo {
		err = qb.queue.TurboOn()
		if err != nil {
			log.Print("Error turbo on ", err)
		}
	}
	return
}

func (qb *QueueBackend) Enqueue(p []byte) (err error) {
	err = qb.queue.Enqueue(&QueueItem{Data: p})
	if err != nil {
		log.Print("write enqueue failed ", err)
	}
	return
}

func (qb *QueueBackend) Dequeue() (p []byte, err error) {
	var iface interface{}
	if iface, err = qb.queue.Dequeue(); err != nil && err != dque.ErrEmpty {
		log.Fatal("Error peeking at item", err)
		return nil, err
	}

	item, ok := iface.(*QueueItem)
	if !ok {
		err = fmt.Errorf("item is not an QueueItem pointer")
	}
	return item.Data, err
}

func (qb *QueueBackend) Peek() (p []byte, err error) {
	var iface interface{}
	if iface, err = qb.queue.Peek(); err != nil && err != dque.ErrEmpty {
		log.Fatal("Error peeking at item", err)
		return nil, err
	}

	item, ok := iface.(*QueueItem)
	if !ok {
		err = fmt.Errorf("item is not an QueueItem pointer")
	}
	return item.Data, err
}

func (qb *QueueBackend) Size() int {
	return qb.queue.Size()
}

func (qb *QueueBackend) Close() {
	qb.queue.Close()
}
