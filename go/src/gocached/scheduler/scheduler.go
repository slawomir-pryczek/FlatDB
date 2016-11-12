package scheduler

import (
	"fmt"
	"hash/crc32"
)

const (
	Type_Add       EventType = 0 // modify events
	Type_Delete              = 1
	Type_Set                 = 100 // set events
	Type_Increment           = 101
	Type_Decrement           = 102
	Type_Get                 = 200 // here we'll have reading (get) only events

	Type_Nil = 255
)

type EventType int

func (this *worker_thread) processEvent(e *sched_event) string {
	return "XX"
}

func Schedule(t EventType, key string, data *string) string {

	cs := crc32.ChecksumIEEE([]byte(key))
	w := workerThreads[int(cs)%len(workerThreads)]

	receiver := make(chan string)
	ev := &sched_event{t, key, data, receiver}
	w.ch <- ev
	fmt.Println(3)

	ret := <-receiver
	fmt.Println(3)
	return ret

}

/*
var mu sync.Mutex
var sched_add_del, sched_set, sched_get []sched_event

func init() {

}

func classify(t EventType) *[]sched_event {

	var list *[]sched_event
	if t < 100 {
		list = &sched_add_del
	}
	if list == nil && t < 200 {
		list = &sched_set
	}
	if list == nil {
		list = &sched_get
	}

	return list

}

func Schedule(t EventType, key string, data *string, receiver chan string) string {

	// classify the type
	list := classify(t)
	ev := sched_event{t, key, data, receiver}

	mu.Lock()
	*list = append(*list, ev)
	mu.Unlock()

	ret := <-receiver
	return ret
}
*/
