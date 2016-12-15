package sync_task

import (
	"sync"
	"time"
)

const SYNC_KEY_DELETE = 100
const SYNC_KEY_SET = 101
const SYNC_KEY_TOUCH = 102
const SYNC_FLUSH = 200

type Sync_task struct {
	id       int
	started  int64
	finished int64
	status   string

	stat_item_pending int
	stat_key_set      int
	stat_key_touch    int
	stat_key_delete   int
	stat_flush        int

	stat_uncompressed int
	stat_compressed   int

	items            []sync_item
	_getkey_callback func(name string, get_data bool) (uint32, uint32, []byte)

	Data_Packets    [][]byte
	data_dispatched bool
}

var sync_tasks_mutex sync.Mutex
var sync_tasks = make([]*Sync_task, 0)

func NewSyncTask(task_id int, item_count int, _getkey_callback func(name string, get_data bool) (uint32, uint32, []byte)) *Sync_task {
	new_task := &Sync_task{id: task_id, started: time.Now().UnixNano(), status: "Gathering Items",
		stat_item_pending: item_count, _getkey_callback: _getkey_callback, Data_Packets: make([][]byte, 0)}
	sync_tasks_mutex.Lock()
	sync_tasks = append(sync_tasks, new_task)
	sync_tasks_mutex.Unlock()

	return new_task
}

func (this *Sync_task) SetStatus(status string) {
	sync_tasks_mutex.Lock()
	this.status = status
	sync_tasks_mutex.Unlock()
}

func (this *Sync_task) AddItem(name string, cas uint32, operation byte) {

	// flush should be always processed first!
	if operation == SYNC_FLUSH {
		cas = 0
	}

	this.items = append(this.items, sync_item{name, cas, operation})
	this.stat_item_pending--

	switch {
	case operation == SYNC_KEY_SET:
		this.stat_key_set++
	case operation == SYNC_KEY_TOUCH:
		this.stat_key_touch++
	case operation == SYNC_KEY_DELETE:
		this.stat_key_delete++
	case operation == SYNC_FLUSH:
		this.stat_flush++
	}
}

func (this *Sync_task) Process() []*Sync_task {

	this.prepareData()

	// go through the history list and dispatch finished items
	// dispatch tasks in order, and also if there are 0 items, just mark the task as "virtually" dispatched

	ret := make([]*Sync_task, 0, 0)
	sync_tasks_mutex.Lock()

	can_dispatch := true
	for _, v := range sync_tasks {

		if v.finished == 0 {
			can_dispatch = false
		}

		if can_dispatch && v.finished != 0 && !v.data_dispatched {
			v.data_dispatched = true
			if v.stat_key_delete > 0 || v.stat_key_set > 0 || v.stat_key_touch > 0 || v.stat_flush > 0 {
				ret = append(ret, v)
			}
		}
	}

	//limit tasks in history to 10 max
	_tmp := make([]*Sync_task, 0, len(sync_tasks))
	for _, v := range sync_tasks {
		if v.stat_key_delete > 0 || v.stat_key_set > 0 || v.stat_key_touch > 0 || v.stat_flush > 0 {
			_tmp = append(_tmp, v)
		}
	}
	_e := len(_tmp)
	_s := _e - 16
	if _s < 0 {
		_s = 0
	}
	sync_tasks = _tmp[_s:_e]
	sync_tasks_mutex.Unlock()

	return ret
}

func (this *Sync_task) GetStats() (int, int, int, int, int, int, int) {

	sync_tasks_mutex.Lock()
	defer sync_tasks_mutex.Unlock()

	return this.stat_item_pending, this.stat_key_set, this.stat_key_touch, this.stat_key_delete, this.stat_flush,
		this.stat_uncompressed, this.stat_compressed

}

func (this *Sync_task) GetStatsSmall() (int, int, int, int) {
	return this.id, this.stat_uncompressed, this.stat_compressed, this.stat_item_pending
}
