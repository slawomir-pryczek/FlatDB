package scheduler

import (
	"fmt"
)

type sched_event struct {
	t        EventType
	key      string
	data_in  *string
	receiver chan<- string
}

type worker_thread struct {
	ch chan *sched_event

	events_postponed []*sched_event
}

var workerThreads []*worker_thread

func init() {

	workerThreads = make([]*worker_thread, 10)
	for i := 0; i < len(workerThreads); i++ {
		workerThreads[i] = MakeWorkerThread()
	}

}

func (this *worker_thread) processEvents() {

	for k := range this.events_postponed {

		ev := this.events_postponed[k]
		if ev.t == Type_Nil {
			continue
		}

		data := this.processEvent(ev)

		// process single event
		if ev.t != Type_Get {
			ev.receiver <- data
			fmt.Println(">>>")
			continue
		}

		// return for current item
		ev.receiver <- data

		// maybe we'll be able to process range...
		for_key := ev.key
		for kk := range this.events_postponed[k+1:] {

			evv := this.events_postponed[kk]
			if evv.key != for_key {
				continue
			}

			if evv.key == for_key && evv.t != Type_Get {
				break
			}

			evv.receiver <- data
			this.events_postponed[kk].t = Type_Nil
		}

	}

}

func MakeWorkerThread() *worker_thread {

	ch := make(chan *sched_event, 100)
	events_postponed := make([]*sched_event, 0, 10)

	wt := worker_thread{ch: ch}

	go func(ch <-chan *sched_event) {

		for true {
			events_postponed = events_postponed[:0]

			e := <-ch
			events_postponed = append(events_postponed, e)

			cont := true
			for cont {
				select {
				case e := <-ch:
					events_postponed = append(events_postponed, e)
				default:
					cont = false
				}
			}

			wt.events_postponed = events_postponed
			wt.processEvents()
		}

	}(ch)

	return &wt
}
