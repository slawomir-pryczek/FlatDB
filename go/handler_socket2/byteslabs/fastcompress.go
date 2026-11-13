package byteslabs

import (
	"bytes"
	"compress/flate"
	"sync"
)

const max_compressors = 20

var mu sync.Mutex
var f_writers []*flate.Writer
var fw_stat_compressors_reuse = 0
var fw_stat_compressors_created = 0
var fw_stat_underflows = 0
var fw_stat_overflows = 0
var fw_stat_underflows_b = 0
var fw_stat_overflows_b = 0

func init() {

	f_writers = make([]*flate.Writer, max_compressors)
	for i := 0; i < max_compressors; i++ {
		tmp, _ := flate.NewWriter(nil, 2)
		f_writers[i] = tmp
	}
}

func Compress(data []byte, alloc *Allocator) []byte {

	mu.Lock()
	var writer *flate.Writer = nil
	if len(f_writers) > 0 {
		writer = f_writers[len(f_writers)-1]
		f_writers = f_writers[:len(f_writers)-1]
		fw_stat_compressors_reuse++
	} else {
		fw_stat_compressors_created++
	}
	mu.Unlock()
	if writer == nil {
		writer, _ = flate.NewWriter(nil, 2)
	}

	_d_len := len(data)
	_sa_max := (slab_size * slab_count) / 2

	switch {
	case _d_len > _sa_max:
		_d_len = int(float64(_d_len) / 3.0)
		if _d_len > _sa_max {
			_d_len = _sa_max
		}

	case _d_len > slab_size:
		_d_len = int(float64(_d_len) / 1.4)

	}

	b := bytes.NewBuffer(alloc.Allocate(_d_len))
	writer.Reset(b)
	writer.Write(data)
	writer.Close()

	diff := _d_len - b.Len()
	if diff >= 0 {
		fw_stat_underflows++
		fw_stat_underflows_b += diff
	} else {
		fw_stat_overflows++
		fw_stat_overflows_b += -diff
	}

	mu.Lock()
	f_writers = append(f_writers, writer)
	mu.Unlock()

	return b.Bytes()
}
