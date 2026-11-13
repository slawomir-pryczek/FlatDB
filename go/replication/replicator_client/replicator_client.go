package replicator_client

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"time"
)

var rclient_mutex sync.Mutex
var rclient_items = make([]rclient_item, 0)

type rclient_item struct {
	Operation byte
	Expires   uint32
	CAS       uint32
	Key       string
	Data      []byte
}

const SYNC_KEY_DELETE = 100
const SYNC_KEY_SET = 101
const SYNC_KEY_TOUCH = 102
const SYNC_FLUSH = 200

var stat_received_packets = 0
var stat_receive_error_decompr = 0
var stat_receive_error_len = 0
var stat_receive_error_packetsize = 0
var stat_decompression_took = int64(0)
var stat_processing_took = int64(0)

var stat_bytes_received = 0
var stat_bytes_received_error = 0
var stat_bytes_uncompressed = 0
var stat_key_set = 0
var stat_key_touch = 0
var stat_key_delete = 0
var stat_flush = 0

func ProcessReplicationCall(data []byte) bool {

	_reclen := len(data)
	_started := time.Now().UnixNano() / 1000

	rclient_mutex.Lock()
	stat_received_packets++
	stat_bytes_received += _reclen
	rclient_mutex.Unlock()

	data_len := int(binary.LittleEndian.Uint32(data[0:4]))
	// if compressed data is over 512MB there's something wrong, as we're sending 64k packets MAX
	if data_len < 0 || data_len > 1024*1024*512 {
		_err("Packet too long #0001")

		rclient_mutex.Lock()
		stat_bytes_received_error += _reclen
		stat_receive_error_packetsize++
		rclient_mutex.Lock()
		return false
	}

	data = data[4:]

	buffer := bytes.NewBuffer(make([]byte, 0, data_len))
	inflate := flate.NewReader(bytes.NewReader(data))
	defer inflate.Close()

	if n, err := buffer.ReadFrom(inflate); int(n) != data_len || err != nil {
		if err != nil {
			_err("Cannot decompress replication packet #0002: " + err.Error())

			rclient_mutex.Lock()
			stat_bytes_received_error += _reclen
			stat_receive_error_decompr++
			rclient_mutex.Unlock()
			return false
		}

		err_str := fmt.Sprintf("hoping for %d bytes, got %d", data_len, n)
		_err("Error decompressing replication packet #0003 " + err_str)

		rclient_mutex.Lock()
		stat_bytes_received_error += _reclen
		stat_receive_error_len++
		rclient_mutex.Unlock()
		return false
	}

	_stat_key_set := 0
	_stat_key_touch := 0
	_stat_key_delete := 0
	_stat_flush := 0

	data = buffer.Bytes()
	pos := 0

	rclient_mutex.Lock()
	stat_decompression_took += (time.Now().UnixNano() / 1000) - _started
	for pos < len(data) {

		if pos+1+4+4+4+4 > len(data) {
			_err("Replication call error phase 1")
		}
		item_op := data[pos]
		pos += 1

		item_expires := binary.LittleEndian.Uint32(data[pos : pos+4])
		pos += 4

		item_cas := binary.LittleEndian.Uint32(data[pos : pos+4])
		pos += 4

		nlen := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		pos += 4
		dlen := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		pos += 4

		if pos+nlen+dlen > len(data) {
			_err("Replication call error phase 2")
			return false
		}

		item_name := string(data[pos : pos+nlen])
		pos += nlen

		item_data := data[pos : pos+dlen]
		pos += dlen

		switch {
		case item_op == SYNC_KEY_SET:
			_stat_key_set++
		case item_op == SYNC_KEY_TOUCH:
			_stat_key_touch++
		case item_op == SYNC_KEY_DELETE:
			_stat_key_delete++
		case item_op == SYNC_FLUSH:
			_stat_flush++
		}

		rclient_items = append(rclient_items, rclient_item{item_op, item_expires, item_cas, item_name, item_data})
	}

	stat_processing_took += (time.Now().UnixNano() / 1000) - _started
	stat_bytes_uncompressed += len(data)
	stat_key_set += _stat_key_set
	stat_key_touch += _stat_key_touch
	stat_key_delete += _stat_key_delete
	stat_flush += _stat_flush
	rclient_mutex.Unlock()

	return true
}

func GetPendingOps() []rclient_item {

	rclient_mutex.Lock()
	defer rclient_mutex.Unlock()

	if len(rclient_items) == 0 {
		return nil
	}
	_toprocess := rclient_items
	rclient_items = make([]rclient_item, 0, len(rclient_items)/2)

	fmt.Println("SLAVE <", len(_toprocess))
	return _toprocess
}

func _err(err string) {
	fmt.Fprintf(os.Stderr, "Replication-client error: "+err)
}
