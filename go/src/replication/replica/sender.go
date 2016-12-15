package replica

import (
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"time"
)

var send_count = 0

func (this *replica) _connect_clean() {

	now := int(time.Now().Unix())
	if this.conn != nil && now-this.last_send_ts > 20 {
		this._connect(false)
	}
}

func (this *replica) _connect(connect bool) bool {

	if connect {
		if this.conn != nil {
			return true
		}

		if conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", this.host, this.port), time.Second*2); err == nil {
			this.conn = conn
			this.stat_conn_number++
			return true
		}
	} else {
		if this.conn == nil {
			return true
		}

		go func(c net.Conn) {
			if c != nil {
				c.Close()
			}
			c = nil
		}(this.conn)
		this.conn = nil
		return true
	}

	return false
}

func sendData(conn net.Conn, data []byte) bool {

	if conn == nil {
		return false
	}

	send_count++
	guid := fmt.Sprintf("g-%d-g", send_count)
	action := []byte("r-client")
	_total_len := 1 + 4 + 2 + len(guid) + (2 + 4 + len("action") + len(action)) + (2 + 4 + 1 + len(data))

	// header - b, data size and guid
	header_to_send := make([]byte, 1024)
	pos := 0
	header_to_send[pos] = 'b'
	pos += 1
	binary.LittleEndian.PutUint32(header_to_send[pos:pos+4], uint32(_total_len))
	pos += 4
	binary.LittleEndian.PutUint16(header_to_send[pos:pos+2], uint16(len(guid)))
	pos += 2
	copy(header_to_send[pos:pos+len(guid)], guid)
	pos += len(guid)

	// action in action field

	binary.LittleEndian.PutUint16(header_to_send[pos:pos+2], uint16(len("action")))
	pos += 2
	binary.LittleEndian.PutUint32(header_to_send[pos:pos+4], uint32(len(action)))
	pos += 4
	copy(header_to_send[pos:pos+len("action")], []byte("action"))
	pos += len("action")
	copy(header_to_send[pos:pos+len(action)], action)
	pos += len(action)

	// payload in p field
	binary.LittleEndian.PutUint16(header_to_send[pos:pos+2], 1)
	pos += 2
	binary.LittleEndian.PutUint32(header_to_send[pos:pos+4], uint32(len(data)))
	pos += 4
	header_to_send[pos] = 'p'
	pos += 1

	header_to_send = header_to_send[0:pos]
	conn.SetWriteDeadline(time.Now().Add(time.Second * 6))
	conn.Write(header_to_send)
	n, err := conn.Write(data)
	if n != len(data) || err != nil {
		return false
	}

	header_to_send = header_to_send[0:1024]
	conn.SetReadDeadline(time.Now().Add(time.Second * 3))
	n, err = conn.Read(header_to_send)
	if n == 0 || err != nil {
		return false
	}
	header_to_send = header_to_send[0:n]

	return strings.Index(string(header_to_send), "GUID:"+guid) > -1
}
