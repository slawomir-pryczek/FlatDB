package replicator

import (
	"fmt"
	"handler_socket2/hscommon"
	"time"
)

func GetStatus(_mode, replication_skip_prefix string) string {

	help := "Replication info<br>"
	help += "--------------------------<br>"
	help += "Currently the replication model is <b>paralell</b> and <b>eventually consistent</b> so snapshot of changes is taken in paralell every 100ms.<br>"
	help += "Then data is gathered (in paralell), compressed, send and re-played <b>in order, but also in paralell</b> on the slave(s).<br>"
	help += "Everything is transfered, that includes original expire time and CAS token. Sorting is just to improve, not to provide<br>"
	help += "strong consistency.<br><br>"

	help += "Paralellism & Agregation<br>--------------------------<br>"
	help += "Snapshot is re-played using 4 cpu threads, which can be changed in code. So you can know that state<br>"
	help += "is <b>eventually consistent after re-playing snapshot T.</b>. Also during the 100ms replication windows, the changes are<br>"
	help += "aggregated (eg. 1000x add to single item will be sent as single command to slave).<br><br>"

	help += "Replication concepts<br>--------------------------<br>"
	help += "Most important concept as replication goes, is a replication window T, which is set in code to take 100ms.<br>"
	help += "We gather all changes done during that window, atomically, so at the end we know that item X need to be set to Y,<br>"
	help += "even if the item was changed multiple times, it'll result only in single replication opcode and single set of data<br>"
	help += "for single item during T. It's called aggregation.<br><br>"

	help += "Then after changelist T is ready, we move to gathering item's data, expire time and CAS tokens, which are also replicated<br>"
	help += "We sort the list by CAS, to have better changes consistency and then replay it, in paralell, on slave<br><br>"

	help += "Some things needs to be explained further, so the replication on slave is eventually consistent:<br>"
	help += "So, after the whole snapshot is replayed you'll have exact data on slave as you have on replica... during replaying, however,<br>"
	help += "items can be created in different order, and not all changes will be reflected, you can consider 3 cases:<br>"
	help += " A. Items could be re-played in different order, so if on master item A was created in T+10ms, and B in T+40ms,<br>"
	help += "    on the slave item B can be created <b>before</b> A<br>"
	help += " B. If on master, in single T window, you set the item X to be AAA, then BBB, then CCC... on the slave you'll only<br>"
	help += "    see it's value as CCC. So if you made changes very rapidly, some of them won't be seen, because of aggregation.<br>"
	help += "    Still, at the end they'll both be the same on master and slave at the end of T or T+1 window.<br>"
	help += " C. If you're chaning the items very rapidly, item X could have state from T+1 window in T snapshot.<br>"
	help += "	 It means, that under some circumstances, on slave which is on window T, some of the data can be from 100ms in the 'future'"

	help_mode := "Supported modes<br>"
	help_mode += "--------------------------<br>"
	help_mode += "Supported modes are <b>OPTIMISTIC</b> and <b>SAFE</b>. In SAFE mode, TOUCH operations are carried out using SET,<br>"
	help_mode += "so even for changing expiration time, the replica will receive full item's data. This is safer if you're afraid that<br>"
	help_mode += "you might run low on memory on the slave"

	_pending_tasks := 0
	_sending_tasks := 0
	for _, v := range replicas {
		v.mu.Lock()
		_pending_tasks += len(v.sync_tasks)
		if v.sync_task_sending != nil {
			_pending_tasks++
			_sending_tasks++
		}
		v.mu.Unlock()
	}
	mutex_replica.Lock()
	_slave_count := len(replicas)
	mutex_replica.Unlock()

	ret := "<b>Flatdb Replication Plugin</b><br>"
	ret += "This <span class='tooltip'>( ? ) Replication Plugin <div><pre>" + help + "</pre></div></span> supports hot-adding and hot-removing nodes, runs in master-slave mode.<br>"
	ret += "Currently selected mode: <span class='tooltip'> ( ? ) " + _mode + " <div style='width: 600px;'>" + help_mode + "</div></span> ccccc"
	ret += fmt.Sprintf("Sync Tasks Pending %d with %d Sending for %d Slaves<br>", _pending_tasks, _sending_tasks, _slave_count)

	ret += "<br>"
	ret += "Last replication tasks that were created<br>"

	// ######
	// ######
	// process sync tasks
	tab := hscommon.NewTableGen("ID", "Status",
		"OP-Set", "OP-Touch", "OP-Delete", "OP-Flush", "Items Pending",
		"Compressed Size", "Uncompressed", "Processing Time", "Data Packets", "Created")
	tab.SetClass("tab")

	mutex_replica.Lock()
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
		_packets := fmt.Sprintf("%d", len(v.data_packets))
		_creatime := time.Unix(0, v.started).Format(time.StampMilli)

		tab.AddRow(_id, v.status,
			_set, _touch, _delete, _flush, _items_pending,
			_size, _size_uncompr, _proctime, _packets, _creatime)
	}
	mutex_replica.Unlock()
	ret += tab.Render()

	// ######
	// ######
	// process replica status
	tab = hscommon.NewTableGen("Host", "Pending Sync Tasks", "Sending Now",
		"OP-Set", "OP-Touch", "OP-Delete", "OP-Flush", "Sent Data", "Sent Failed")
	tab.SetClass("tab")

	mutex_replica.Lock()
	for _, v := range replicas {
		v.mu.Lock()

		_host := fmt.Sprintf("%s:%d", v.host, v.port)
		_pending := fmt.Sprintf("%d", len(v.sync_tasks))
		if len(v.sync_tasks) > 0 {
			_pending += " ("
			for k, vv := range v.sync_tasks {
				if k > 0 {
					_pending += ", "
				}
				_pending += fmt.Sprintf("#%d", vv.id)
			}
			_pending += ")"
		}

		_sending := "-"
		if v.sync_task_sending != nil {
			_sending = fmt.Sprintf("#%d (%s of %s)", v.sync_task_sending.id,
				hscommon.FormatBytes(uint64(v.sync_task_sending.stat_compressed)),
				hscommon.FormatBytes(uint64(v.stat_bytes_pending)))
		}
		_set := fmt.Sprintf("%d", v.stat_key_set)
		_touch := fmt.Sprintf("%d", v.stat_key_touch)
		_delete := fmt.Sprintf("%d", v.stat_key_delete)
		_flush := fmt.Sprintf("%d", v.stat_flush)
		_sent := hscommon.FormatBytes(uint64(v.stat_bytes_sent))
		_sent_uncompressed := hscommon.FormatBytes(uint64(v.stat_bytes_sent_uncompressed))
		_skipped_bytes := hscommon.FormatBytes(uint64(v.stat_bytes_skipped))

		_failtime := "-"
		if v.last_send_failed_timestamp > 0 {
			_failtime = time.Unix(v.last_send_failed_timestamp, 0).Format(time.RFC850)
		}
		_worker_tooltip := "Here you can see detailed stats about data sent and skipped<br>" +
			"--------------------------<br><br>" +
			"Number of times data was sent OK: %d (%s)<br>" +
			" Uncompressed Data: %s<br>" +
			" Sent Items: %d<br><br>" +
			"Number of times send was skipped or failed: %d (last time: %s)<br>" +
			" Skipped bytes: %s<br>" +
			" Skipped SET ops: %d<br>" +
			" Skipped TOUCH ops: %d<br>" +
			" Skipped DELETE ops: %d<br>" +
			" Skipped FLUSH ops: %d"

		_worker_tooltip = fmt.Sprintf(_worker_tooltip,
			v.stat_sends, _sent, _sent_uncompressed,
			v.stat_key_set+v.stat_key_touch+v.stat_key_delete+v.stat_flush,
			v.stat_skips, _failtime, _skipped_bytes,
			v.stat_skipped_key_set, v.stat_skipped_key_touch, v.stat_skipped_key_delete, v.stat_skipped_flush)

		_host = "<span class='tooltip'>(?)&nbsp;" + _host +
			"<div style='width: 650px;'><pre>" + _worker_tooltip + "</pre></div></span>"

		tab.AddRow(_host, _pending, _sending, _set, _touch, _delete, _flush, _sent, _skipped_bytes)
		v.mu.Unlock()
	}
	mutex_replica.Unlock()

	ret += "<br>"
	ret += "Current replication status<br>"
	ret += tab.Render()
	return ret
}
