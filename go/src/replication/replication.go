package replication

import (
	"replication/replica"
	"replication/replicator_client"
	"replication/sync_task"
)

const SYNC_KEY_DELETE = sync_task.SYNC_KEY_DELETE
const SYNC_KEY_SET = sync_task.SYNC_KEY_SET
const SYNC_KEY_TOUCH = sync_task.SYNC_KEY_TOUCH
const SYNC_FLUSH = sync_task.SYNC_FLUSH

func AddReplica(host string, port int) int {

	if host == "/dev/null" {
		port = -1
	}
	return replica.AddReplica(host, port)
}

func RemoveReplica(host string, port int) int {
	return replica.RemoveReplica(host, port)
}

func GetStatus(_mode string) string {
	return replica.GetStatus(_mode) + "<br>" + sync_task.GetStatus() + "<br>" + replicator_client.GetStatus()
}

type si struct {
	s *sync_task.Sync_task
}

func NewReplicationTask(task_id int, item_count int, _getkey_callback func(name string, get_data bool) (uint32, uint32, []byte)) (this *si) {
	return &si{s: sync_task.NewSyncTask(task_id, item_count, _getkey_callback)}
}

func (this *si) AddItem(name string, cas uint32, operation byte) {
	this.s.AddItem(name, cas, operation)
}

func (this *si) Process() {

	finished_tasks := this.s.Process()
	for _, v := range finished_tasks {
		replica.AddSyncTask(v)
	}

}

func HashMapShouldSynchronize(kvstore_num int) bool {
	return replica.HashMapShouldSynchronize(kvstore_num)
}

func HashMapForceResync(host string, port int) int {
	return replica.HashMapForceResync(host, port)
}
