package slab

import (
	"FlatDB/gocached/common"
	"FlatDB/tools/ringlog"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// We should report expire = 0 for deleted chunk
func (this *slab) elxGCReportExpire(chunk_no uint16, expire uint32) {

	this.elx_expire[chunk_no] = expire
	if expire > 0 && (expire < this.elx_gc_next_run || this.elx_gc_next_run == 0) {
		this.elx_gc_next_run = expire
		this.elx_gc_pos = chunk_no
	}

}

func (this *slab) elxGCRun() int {

	elx_expire := this.elx_expire
	elx_expire_len := len(this.elx_expire)
	pos := this.elx_gc_pos
	now := uint32(common.TSNow())

	// reset GC status
	elx_gc_next_run := uint32(0)
	elx_gc_pos := uint16(0)
	// <<

	deleted := 0
	del_tries := 0
	for i := 0; i < elx_expire_len; i++ {

		pos++
		if pos >= uint16(elx_expire_len) {
			pos = 0
		}

		// there is no item
		curr_expire := elx_expire[pos]
		if curr_expire == 0 {
			continue
		}

		if curr_expire < now {
			// item probably expired - try removing it!
			if this.CollectChunkUnsafe(pos) {
				deleted++
			} else {
				del_tries++

				// item hasn't expired or we can delete no more, update GC vars
				if curr_expire < elx_gc_next_run || elx_gc_next_run == 0 {
					elx_gc_next_run = curr_expire
					elx_gc_pos = pos
				}

			}

		} else {

			// item hasn't expired or we can delete no more, update GC vars
			if curr_expire < elx_gc_next_run || elx_gc_next_run == 0 {
				elx_gc_next_run = curr_expire
				elx_gc_pos = pos
			}

		}
	}

	this.elx_gc_next_run = elx_gc_next_run
	this.elx_gc_pos = elx_gc_pos

	return deleted
}

func (this *slab_class) elxGCWalk() (int, string) {

	now := uint32(common.TSNow())
	start := time.Now()

	stat_slab_processed := 0
	stat_slab_skipped := 0
	stat_deleted := 0
	stat_age_run := 0
	stat_age_needed := 0

	this.mu.RLock()
	_tmp := make([]*slab, len(this.slabs))
	copy(_tmp, this.slabs)
	_slab_count := len(this.slabs)
	pos := this._lastfree_slab + 1
	this.mu.RUnlock()

	for i := 0; i < _slab_count; i++ {

		pos++
		slab_no := pos % _slab_count
		slab := _tmp[slab_no]

		if now <= slab.elx_gc_next_run || slab.elx_gc_next_run == 0 {
			slab.mu.Lock()
			if slab.elx_gc_next_run > 0 && slab.elx_gc_next_run > now {
				stat_age_needed += int(slab.elx_gc_next_run - now)
			}
			slab.stat_gc_skip_count++
			slab.mu.Unlock()

			stat_slab_skipped++
			continue
		}

		slab.mu.Lock()
		slab.stat_gc_run_count++
		stat_age_run += int(now - slab.elx_gc_next_run)
		stat_deleted += slab.elxGCRun()
		slab.mu.Unlock()

		stat_slab_processed++
	}

	// Generate GCWalk log item
	log := ""
	{
		took := float64(time.Since(start).Microseconds()) / 1000
		_lrt, _ttr := "-", "-"
		if stat_slab_processed > 0 {
			_lrt = fmt.Sprintf("%.02f", float64(stat_age_run)/float64(stat_slab_processed))
		}
		if stat_slab_skipped > 0 {
			_ttr = fmt.Sprintf("%.02f", float64(stat_age_needed)/float64(stat_slab_skipped))
		}
		log = fmt.Sprintf("GCWalk (Class #%d) Items Deleted:%d in %d/%d Slab(s) Took:%.02fms        ExpireAgeAvg(S):%s TimeToRunAvg(S): %s",
			this.slab_class_no, stat_deleted, stat_slab_processed, _slab_count, took, _lrt, _ttr)
	}

	return stat_deleted, log
}

// This code will walk through all slabs and clean them
func elxRunSimple() {
	for _, slab_class := range slab_classes {

		deleted, log := slab_class.elxGCWalk()
		if deleted > 0 {
			ringlog.GetStaticLog("gcwalk").Write(log)
		}
	}

	time.Sleep(time.Second)
}

func elxRun() {

	print_skiplist := func(prefix string, sl []bool) string {
		var sb strings.Builder
		sb.WriteString(prefix)
		items_total := 0
		for i, v := range sl {
			if v {
				sb.WriteString("#")
				sb.WriteString(strconv.Itoa(i))
				sb.WriteByte(' ')
				items_total++
			}
		}
		if items_total == 0 {
			sb.WriteString(" - ")
		}
		return fmt.Sprint(strings.TrimSuffix(sb.String(), " "))
	}

	skip_slab_class := make([]bool, len(slab_classes)+1)
	cleaned_up_anything := false

	logger := ringlog.GetStaticLog("gcwalk")
	for run := 0; run < 2; run++ {
		logger.Write(print_skiplist("Starting GCWalk, skiplist: ", skip_slab_class))

		cleaned_up_anything = false
		for k, slab_class := range slab_classes {
			if skip_slab_class[k] {
				skip_slab_class[k] = false
				continue
			}

			deleted, log := slab_class.elxGCWalk()
			if deleted > 0 {
				cleaned_up_anything = true
				logger.Write(log)
				time.Sleep(time.Second)
			} else {
				skip_slab_class[k] = true
			}
		}

		if !cleaned_up_anything {
			time.Sleep(time.Second)
		}
	}
}

func RunGCForSlab(slab_class_no, slab_no uint16) bool {

	sclass := slab_classes[slab_class_no]

	sclass.mu.RLock()
	if int(slab_no) >= len(sclass.slabs) {
		sclass.mu.RUnlock()
		return false
	}
	slab := sclass.slabs[slab_no]
	sclass.mu.RUnlock()

	slab.mu.Lock()
	// maybe we won't need to run the GC as all chunks are free
	if slab.chunks_free == slab.chunks_count {
		slab.mu.Unlock()
		return false
	}
	slab.elxGCRun()
	slab.mu.Unlock()

	return true
}
