package sync_task

import (
	"fmt"
	"handler_socket2/hscommon"
	"time"
)

func GetStatus() string {

	ret := "Last replication tasks that were created<br>"

	// process sync tasks
	tab := hscommon.NewTableGen("ID", "Status",
		"OP-Set", "OP-Touch", "OP-Delete", "OP-Flush", "Items Pending",
		"Compressed Size", "Uncompressed", "Processing Time", "Data Packets", "Created")
	tab.SetClass("tab")

	sync_tasks_mutex.Lock()
	for _, v := range sync_tasks {

		_id := fmt.Sprintf("#%d", v.id)
		_set := fmt.Sprintf("%d", v.stat_key_set)
		_touch := fmt.Sprintf("%d", v.stat_key_touch)
		_delete := fmt.Sprintf("%d", v.stat_key_delete)
		_flush := fmt.Sprintf("%d", v.stat_flush)
		_items_pending := fmt.Sprintf("%d", v.stat_item_pending)
		_size := hscommon.FormatBytes(uint64(v.stat_compressed))
		_size_uncompr := hscommon.FormatBytes(uint64(v.stat_uncompressed))
		_proctime := fmt.Sprintf("%.2fms", float64(v.finished-v.started)/1000000)
		_packets := fmt.Sprintf("%d", len(v.Data_Packets))
		_creatime := time.Unix(0, v.started).Format(time.StampMilli)

		tab.AddRow(_id, v.status,
			_set, _touch, _delete, _flush, _items_pending,
			_size, _size_uncompr, _proctime, _packets, _creatime)
	}
	sync_tasks_mutex.Unlock()
	ret += tab.Render()

	return ret
}
