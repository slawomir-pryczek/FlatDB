package slab

import (
	"fmt"
	"gocached/common"
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

func (this *slab_class) elxGCWalk() int {

	now := uint32(common.TSNow())
	deleted := int(0)

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

			//if this.slab_class_no == 10 {
			//	fmt.Println(now <= slab.elx_gc_next_run, "SLAB CLASS ", this.slab_class_no, "CHUNK ", i, "SKIPPED", ">>>", slab.elx_gc_next_run, " NOW", now)
			//}
			slab.mu.Lock()
			slab.stat_gc_skip_count++
			slab.mu.Unlock()
			continue
		}
		//if this.slab_class_no == 10 {
		//	fmt.Println(now <= slab.elx_gc_next_run, "XX SLAB CLASS RUNGC ", this.slab_class_no, "CHUNK ", i, "SKIPPED", ">>>", slab.elx_gc_next_run, " NOW", now)
		//	}

		//this.mu.Lock()
		//this._lastfree_slab = slab_no + 1
		//this.mu.Unlock()

		slab.mu.Lock()
		slab.stat_gc_run_count++
		deleted = slab.elxGCRun()
		slab.mu.Unlock()

		//if deleted > 0 {
		//}

	}

	return deleted
}

func elxRun() {

	for _, slab_class := range slab_classes {

		deleted := slab_class.elxGCWalk()
		if deleted > 0 {
			fmt.Println("EXLRUN", deleted)
		}

	}
}

func RunGCForSlab(slab_class_no, slab_no uint16) bool {

	sclass := slab_classes[slab_class_no]

	sclass.mu.RLock()
	if int(slab_no) >= len(sclass.slabs) {
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
