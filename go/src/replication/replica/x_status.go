package replica

import (
	"fmt"
	"handler_socket2/hscommon"
	"time"
)

func GetStatus(_mode string) string {

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
	ret += "Currently selected mode: <span class='tooltip'> ( ? ) " + _mode + " <div style='width: 600px;'>" + help_mode + "</div></span><br>"
	ret += fmt.Sprintf("Sync Tasks Pending %d with %d Sending for %d Slaves<br>", _pending_tasks, _sending_tasks, _slave_count)

	// ######
	// ######
	// process replica status
	SRFSk := "Communication by status:<br>------------------<br>"
	SRFSk += "<span class='s1'>Sent ok</span><br>"
	SRFSk += "<span class='s2'>Retries</span><br>"
	SRFSk += "<span class='s3'>Sent failed</span> - network issues or wrong response from server<br>"
	SRFSk += "<span class='s4'>Skipped</span> - sync buffer was discarded and sent wasn't tried because we already had too much data in buffers"

	tab := hscommon.NewTableGen("Host", "Pending Sync Tasks", "Sending Now",
		"OP-Set", "OP-Touch", "OP-Delete", "OP-Flush", "Sent Data", "Sent Failed", "<span class='tooltip'>( ? )SRFSk<div style='width: 400px; margin-left: -400px;'>"+SRFSk+"</div></span> ")
	tab.SetClass("tab replicas")

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

				_id, _, _, _ := vv.GetStatsSmall()
				_pending += fmt.Sprintf("#%d", _id)
			}
			_pending += ")"
		}

		_sending := "-"
		if v.sync_task_sending != nil {

			_id, _, _comp, _pend := v.sync_task_sending.GetStatsSmall()
			_sending = fmt.Sprintf("#%d (%s of %s)", _id,
				hscommon.FormatBytes(uint64(_comp)),
				hscommon.FormatBytes(uint64(_pend)))
		}
		_set := fmt.Sprintf("%d", v.stat_key_set)
		_touch := fmt.Sprintf("%d", v.stat_key_touch)
		_delete := fmt.Sprintf("%d", v.stat_key_delete)
		_flush := fmt.Sprintf("%d", v.stat_flush)
		_sent := hscommon.FormatBytes(uint64(v.stat_bytes_sent))
		_sent_uncompressed := hscommon.FormatBytes(uint64(v.stat_bytes_sent_uncompressed))
		_skipped_bytes := hscommon.FormatBytes(uint64(v.stat_bytes_skipped))
		_failed_bytes := hscommon.FormatBytes(uint64(v.stat_bytes_failed))
		_retries := fmt.Sprintf("<b class='s1'>%d</b>&nbsp;/&nbsp;<b class='s2'>%d</b>&nbsp;/&nbsp;<b class='s3'>%d</b>&nbsp;/&nbsp;<b class='s4'>%d</b>",
			v.stat_sends, v.stat_send_retried, v.stat_send_fail, v.stat_skips)

		_skiptime := "-"
		if v.last_send_skipped_timestamp > 0 {
			_skiptime = time.Unix(v.last_send_skipped_timestamp, 0).Format(time.RFC850)
		}
		_failtime := "-"
		if v.last_send_failed_timestamp > 0 {
			_failtime = time.Unix(v.last_send_failed_timestamp, 0).Format(time.RFC850)
		}

		sync_data := ""
		_max_num := 0
		for k, _ := range v.hashmap_synchronization_progress {
			if _max_num < k {
				_max_num = k
			}
		}
		sync_status := "<b class='cs1'>finished</b>"
		for i := 0; i < _max_num; i++ {
			if i > 0 {
				sync_data += ", "
			}
			if i%12 == 11 {
				sync_data += "<br> "
			}
			_tmp := v.hashmap_synchronization_progress[i]
			sync_data += fmt.Sprintf("<b class='cs1'>#%.02d:</b> %d", i, _tmp)
			if _tmp != 2 {
				sync_status = "pending"
			}
		}
		if _max_num == 0 {
			sync_status = "pending"
		}

		sync_data = fmt.Sprintf("Synchronization status (%s):<br> %s ", sync_status, sync_data)

		_worker_tooltip := "Here you can see detailed stats about data sent and skipped<br>" +
			"--------------------------<br><br>" +
			"Number of times data was sent OK: <span class='s1'>%d</span> (<span class='s1'>%s</span>)<br>" +
			" Uncompressed Data: <span class='s1'>%s</span><br>" +
			" Sent Items: <span class='s1'>%d</span><br><br>" +
			"Number of times send was failed or skipped: <b class='s3'>%d</b> (last time: <span class='s3'>%s</span> / <span class='s4'>%s</span>)<br>" +
			" Failed/skipped bytes: <b class='s3'>%s</b> / <b class='s4'>%s</b><br>" +
			" Failed/skipped SET ops: <b class='s3'>%d</b> / <b class='s4'>%d</b><br>" +
			" Failed/skipped TOUCH ops: <b class='s3'>%d</b> / <b class='s4'>%d</b><br>" +
			" Failed/skipped DELETE ops: <b class='s3'>%d</b> / <b class='s4'>%d</b><br>" +
			" Failed/skipped FLUSH ops: <b class='s3'>%d</b> / <b class='s4'>%d</b>" + "<br><br>"

		_worker_tooltip += fmt.Sprintf("SRFSk Since Last Resync: <b class='s1'>%d</b>&nbsp;/&nbsp;<b class='s2'>%d</b>&nbsp;/&nbsp;<b class='s3'>%d</b>&nbsp;/&nbsp;<b class='s4'>%d</b>",
			v.stat_sends_since_last_recync, v.stat_retries_since_last_resync, v.stat_errors_since_last_resync, v.stat_skips_since_last_resync)
		_worker_tooltip += "<br><br>"

		_worker_tooltip = fmt.Sprintf(_worker_tooltip,
			v.stat_sends, _sent, _sent_uncompressed,
			v.stat_key_set+v.stat_key_touch+v.stat_key_delete+v.stat_flush,
			v.stat_skips+v.stat_send_fail, _failtime, _skiptime,
			_failed_bytes, _skipped_bytes,
			v.stat_failed_key_set, v.stat_skipped_key_set,
			v.stat_failed_key_touch, v.stat_skipped_key_touch,
			v.stat_failed_key_delete, v.stat_skipped_key_delete,
			v.stat_failed_flush, v.stat_skipped_flush)

		_worker_tooltip += sync_data

		__conn := "<b class='cs1'>YES</b>"
		__last_send := "-"
		if v.conn == nil {
			__conn = "no"
		}
		if v.last_send_ts > 0 {
			__last_send = time.Unix(int64(v.last_send_ts), 0).Format(time.RFC850)
		}
		_worker_tooltip += fmt.Sprintf("<br><br>Connected to slave: %s, Number of connects: %d, Last Send: %s",
			__conn, v.stat_conn_number, __last_send)

		__is_running := ""
		if sync_status == "pending" {
			__is_running = " running"
		}
		_host = "<span class='tooltip" + __is_running + "'>(?)&nbsp;" + _host +
			"<div style='width: 700px;'><pre>" + _worker_tooltip + "</pre></div></span>"

		tab.AddRow(_host, _pending, _sending, _set, _touch, _delete, _flush, _sent, _failed_bytes, _retries)
		v.mu.Unlock()
	}
	mutex_replica.Unlock()

	ret += "<br>"
	ret += "Current replication status<br>"
	ret += tab.Render()
	return ret
}
