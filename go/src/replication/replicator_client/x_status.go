package replicator_client

import (
	"fmt"
	"handler_socket2/hscommon"
)

func GetStatus() string {

	ret := "<b>Replicator client</b><br>This plugin will process replication messages sent by master.<br>"

	tab := hscommon.NewTableGen("Packets", "Operations", "Processing Time", "Error Details")
	tab.SetClass("tab")

	_packets := fmt.Sprintf("Reveived: %d (%d errors)<br>Bytes: %s<br>Uncompressed: %s",
		stat_received_packets, stat_receive_error_decompr+stat_receive_error_len+stat_receive_error_packetsize,
		hscommon.FormatBytes(uint64(stat_bytes_received)), hscommon.FormatBytes(uint64(stat_bytes_uncompressed)))

	_operations := fmt.Sprintf("Set: %d<br>Touch: %d<br>Delete: %d<br>Flush: %d",
		stat_key_set, stat_key_touch, stat_key_delete, stat_flush)

	_times := []string{" - ", " - ", " - "}
	if stat_received_packets > 0 {
		_times = []string{fmt.Sprintf("%.2fms", (float64(stat_decompression_took)/float64(stat_received_packets))/1000.0),
			fmt.Sprintf("%.2fms", (float64(stat_processing_took)/float64(stat_received_packets))/1000.0),
			fmt.Sprintf("%.2fms", (float64(stat_decompression_took+stat_processing_took)/float64(stat_received_packets))/1000.0)}
	}

	_proctime := fmt.Sprintf("Average Packet Decompression: %s<br>Average Packet Processing Time: %s<br>Per Packet Total: %s",
		_times[0], _times[1], _times[2])

	_err := fmt.Sprintf("Decompression Errors: %d<br>Bad Decompressed Size: %d<br>Bad Packet Length: %d<br>Bad Received Data Size: %s",
		stat_receive_error_decompr, stat_receive_error_len, stat_receive_error_packetsize,
		hscommon.FormatBytes(uint64(stat_bytes_received_error)))

	tab.AddRow(_packets, _operations, _proctime, _err)

	return ret + tab.Render()

}
