package slab

import (
	"encoding/binary"
	"fmt"
	"handler_socket2/hscommon"
	"sync/atomic"
)

func GetSlabInfo() string {

	tg := hscommon.NewTableGen("Class #", "Count", "Chunk Size", "Items", "Used Space", "Wasted", "Chunk Status",
		"Avg TTL", "EXP", "<30s", "<120s", ">10min", ">30min",
		"Avg Size",
		"<span class='tooltip'>By Size Lo / Hi<div>Number of items that are in lower and higher half of slab, by size.</div></span>")
	tg.SetClass("tab slabstats")

	percentiles := hscommon.NewPercentileStats(slab_classes[0].slab_chunk_count)
	stat_slabs_total := 0
	prev_chunk_size := 0
	for i := 0; i < len(slab_classes); i++ {

		slb := slab_classes[i]

		// get chunk statistics
		buckets := hscommon.NewBucketStats(20)
		stat_items := &itemstat{}
		percentiles.Clean()

		stat_slab := [8]int{0, 0, 0, 0, 0, 0, 0, 0}
		for i := 0; ; i++ {
			slb.mu.RLock()
			if i >= len(slb.slabs) {
				slb.mu.RUnlock()
				break
			}
			stat_items.beginChunk(slb.slab_chunk_count)

			stat_slab[0] += slb.slabs[i].stat_alloc_size
			stat_slab[1] += slb.slabs[i].stat_op_alloc
			stat_slab[2] += slb.slabs[i].stat_op_delete
			stat_slab[3] += slb.slabs[i].stat_op_delete_postponed
			stat_slab[4] += slb.slabs[i].stat_op_free
			stat_slab[5] += slb.slabs[i].stat_op_free_postponed
			stat_slab[6] += slb.slabs[i].stat_gc_run_count
			stat_slab[7] += slb.slabs[i].stat_gc_skip_count

			memory := slb.slabs[i].storage
			slb.mu.RUnlock()

			now := uint32(hscommon.TSNow())
			stat_used := 0
			for ii := 0; ii < slb.slab_chunk_count*slb.slab_chunk_size; ii += slb.slab_chunk_size {

				p := CHUNK_POS_EXPIRES + ii
				expires := binary.LittleEndian.Uint32(memory[p : p+4])

				if memory[ii+CHUNK_POS_STATE] != STATE_CAN_USE {
					continue
				}

				p = CHUNK_POS_DATASIZE + ii
				_tmp := binary.LittleEndian.Uint32(memory[p : p+4])
				percentiles.Push(CalculateChunkSizeI(int(_tmp)))
				stat_items.doItemStat(int(expires) - int(now))
				stat_used++
			}

			buckets.Push(slb.slab_chunk_count, stat_used)
		}

		// Chunk stats and info
		_tmp := _getSlabInfo(i, stat_slab, percentiles.Get(10), percentiles.Get(50), percentiles.Get(90))
		buckets_str := "<span class='tooltip'>" + buckets.Gen() + " <div>" + _tmp + "</div></span>"
		// <<

		s_avg := "-"
		s_lo_hi := "-"
		if pp := percentiles.Avg(); pp > 0 {
			s_avg = hscommon.FormatBytes(uint64(pp))
		}
		if lo, hi := percentiles.CountLoHi((prev_chunk_size + slb.slab_chunk_size) / 2); hi > 0 || lo > 0 {
			s_lo_hi = fmt.Sprintf("%d / %d", lo, hi)
		}
		prev_chunk_size = slb.slab_chunk_size

		slb.mu.RLock()
		tg.AddRow(fmt.Sprintf("%d", slb.slab_class_no), fmt.Sprintf(" x%d", len(slb.slabs)),
			hscommon.FormatBytes(uint64(slb.slab_chunk_size)), fmt.Sprintf("%d", slb.slab_chunk_count),
			stat_items.getUsed(), stat_items.getWasted(), buckets_str, stat_items.getAvgTTL(),
			fmt.Sprintf("%d", stat_items.expired), fmt.Sprintf("%d", stat_items.exp_stats[0]), fmt.Sprintf("%d", stat_items.exp_stats[1]), fmt.Sprintf("%d", stat_items.exp_stats[2]), fmt.Sprintf("%d", stat_items.exp_stats[3]),
			s_avg, s_lo_hi)
		stat_slabs_total += len(slb.slabs)
		slb.mu.RUnlock()

	}

	info := "<pre>"
	info += fmt.Sprintf("<b>Slab statistics</b>:<br>Maximum number of slabs is: %d; We have room for <b style='color: green'>%d more</b>\n", slabs_max, slabs_max-stat_slabs_total)

	info += fmt.Sprintf("Factor is %.2f; Chunk sizes range [ %s - %s ]", factor, hscommon.FormatBytes(min_chunk_size), hscommon.FormatBytes(max_chunk_size))
	info += "To see more details about chunk, hover the mouse over `chunk status` rows"
	info += "</pre>"

	ret := info + tg.Render()

	tg = hscommon.NewTableGen("Class #", "Item Size", "Allocated", "Rebalances", "Store Failed")
	tg.SetClass("tab")

	sum_storage, sum_sfail, sum_rebalances := 0, 0, 0
	for i := 0; i < len(slab_classes); i++ {

		slb := slab_classes[i]
		slb.mu.RLock()

		_size := hscommon.FormatBytes(uint64((slb.slab_chunk_count * slb.slab_chunk_size) + slb.slab_margin))
		_allocated := "-"
		if len(slb.slabs) > 0 {
			_tmp := len(slb.slabs[0].storage) * len(slb.slabs)
			_allocated = hscommon.FormatBytesI(uint64(_tmp))
			sum_storage += _tmp
		}
		_rbl := fmt.Sprint(slb.stat_rebal_slabs_rebalanced)
		_sfail := fmt.Sprint(slb.stat_store_failed)
		sum_rebalances += slb.stat_rebal_slabs_rebalanced
		sum_sfail += slb.stat_store_failed

		_info := _getRebalanceInfo(slb)
		_size = "<span class='tooltip'>" + _size + "<br>(?) Info<div>" + _info + "</div></span>"
		slb.mu.RUnlock()

		tg.AddRow(fmt.Sprint(i), _size, _allocated, _rbl, _sfail)
	}

	_storage := hscommon.FormatBytesI(uint64(sum_storage))
	_rbl := fmt.Sprint(sum_rebalances)
	_f := fmt.Sprint(sum_sfail)
	tg.AddRow("&#931; Sum", "-", _storage, _rbl, _f)

	ret += "<br><b>SLABS summary</b>:<br>"
	_sa := atomic.LoadInt32(&slabs_allocated)
	_sf := atomic.LoadInt32(&slabs_freed)
	ret += fmt.Sprintf("Slab allocations: %d, Slab frees: %d ... Allocated: %d/%d<br>", _sf+_sa, _sf, _sa, slabs_max)
	ret += fmt.Sprintf("Total memory that we can allocate is %d x %s = %s ... allocated currently: %s<br>",
		slabs_max, hscommon.FormatBytes(max_slab_size), hscommon.FormatBytes(slabs_max*max_slab_size), hscommon.FormatBytes(uint64(sum_storage)))

	ret += tg.RenderHoriz(30)

	return ret

	/*tg = hscommon.NewTableGen("Class #", "Item Size", "Allocated", "Rebalances", "Slab Freed", "<b style='color: #EE7777'>Store Failed</b>",
		"Slabs Processed", "AVG Proc. Time")
	tg.SetClass("tab")
	sum_all, sum_sfail, sum_rebal, sum_sf := 0, 0, 0, 0
	sum_rb_c, sum_rb_t := uint64(0), uint64(0)
	for i := 0; i < len(slab_classes); i++ {

		slb := slab_classes[i]
		slb.mu.RLock()
		_size := hscommon.FormatBytes(uint64((slb.slab_chunk_count * slb.slab_chunk_size) + slb.slab_margin))
		_allocated := "-"
		if len(slb.slabs) > 0 {
			_tmp := len(slb.slabs[0].storage) * len(slb.slabs)
			_allocated = hscommon.FormatBytesI(uint64(_tmp))
			sum_all += _tmp
		}

		sum_sf += slb.stat_slab_freed
		sum_sfail += slb.stat_store_failed
		sum_rebal += slb.stat_rebal_slabs_rebalanced

		sum_rb_c += slb.stat_rebal_slabs_scanned
		sum_rb_t += slb.stat_rebal_took_us
		_avg_t := "-"
		if slb.stat_rebal_slabs_scanned > 0 {
			_avg_t = fmt.Sprintf("%.2fms", (float64(slb.stat_rebal_took_us)/float64(slb.stat_rebal_slabs_scanned))/1000.0)
		}

		tg.AddRow(fmt.Sprint(i), _size, _allocated, fmt.Sprint(slb.stat_rebal_slabs_rebalanced),
			fmt.Sprint(slb.stat_slab_freed), fmt.Sprintf("<b style='color: #EE7777'>%d</b>", slb.stat_store_failed),
			fmt.Sprint(slb.stat_rebal_slabs_scanned), _avg_t)
		slb.mu.RUnlock()
	}

	_avg_t := "-"
	if sum_rb_c > 0 {
		_avg_t = fmt.Sprintf("%.2fms", (float64(sum_rb_t)/float64(sum_rb_c))/1000.0)
	}
	tg.AddRow("&#931; Sum", "-", hscommon.FormatBytesI(uint64(sum_all)), fmt.Sprint(sum_rebal),
		fmt.Sprint(sum_sf), fmt.Sprintf("<b style='color: #EE7777'>%d</b>", sum_sfail),
		fmt.Sprint(sum_rb_c), _avg_t)

	ret += "<br><b>SLABS summary</b>:<br>"
	_sa := atomic.LoadInt32(&slabs_allocated)
	_sf := atomic.LoadInt32(&slabs_freed)
	ret += fmt.Sprintf("Slab allocations: %d, Slab frees: %d ... Allocated: %d/%d<br>", _sf+_sa, _sf, _sa, slabs_max)
	ret += fmt.Sprintf("Total memory that we can allocate is %d x %s = %s ... allocated currently: %s<br>",
		slabs_max, hscommon.FormatBytes(max_slab_size), hscommon.FormatBytes(slabs_max*max_slab_size), hscommon.FormatBytes(uint64(sum_all)))

	ret += tg.RenderHoriz(30)*/

	return ret
}

func _getRebalanceInfo(slb *slab_class) string {

	_avg_t := "-"
	if slb.stat_rebal_slabs_scanned > 0 {
		_avg_t = fmt.Sprintf("%.2fms", (float64(slb.stat_rebal_took_us)/float64(slb.stat_rebal_slabs_scanned))/1000.0)
	}

	_items_moved_perslab := "-"
	if slb.stat_rebal_slabs_scanned > 0 {
		_items_moved_perslab = fmt.Sprintf("%.1f", float64(slb.stat_rebal_item_moved)/float64(slb.stat_rebal_slabs_scanned))
	}

	_movedvs := "-"
	if slb.stat_rebal_slabs_scanned > 0 {
		_total := slb.stat_rebal_slabs_scanned * uint64(slb.slab_chunk_count)
		_moved := slb.stat_rebal_item_moved
		_movedvs = fmt.Sprintf("%.1f%%", (float64(_moved)/float64(_total))*100.0)
	}

	ret := fmt.Sprintf("<pre>Slab Class %d Summary\n", slb.slab_class_no)
	if auto_rebalancer_settings != nil && int(slb.slab_class_no) < len(auto_rebalancer_settings) {
		rbl_settings := auto_rebalancer_settings[slb.slab_class_no]
		ret += fmt.Sprintf("<i style='color: #8844aa'>AUTO Rebalance if Below <b>%d%%</b> => Over %d%%</i>\n", rbl_settings[0], rbl_settings[1])
	} else {
		ret += fmt.Sprintf("<i style='color: #8844aa'>AUTO Rebalance DISABLED\n</i>")
	}

	ret += "--------------------------\n\n"
	ret += fmt.Sprintf("Rebalances: %d\n", slb.stat_rebal_slabs_rebalanced)
	ret += fmt.Sprintf("Slabs Processed / Freed: %d / <b style='color: green'>%d</b>\n", slb.stat_rebal_slabs_scanned, slb.stat_slab_freed)
	ret += fmt.Sprintf("AVG Slab Processing Time:  %s\n", _avg_t)

	ret += fmt.Sprintf("\n--Item Stats:\nItems Moved: %d\n", slb.stat_rebal_item_moved)
	ret += fmt.Sprintf("Items Moved Per Slab Processed AVG: %s\n", _items_moved_perslab)
	ret += fmt.Sprintf("Items Moved vs. Expired/Skipped: %s\n", _movedvs)

	ret += fmt.Sprintf("<span style='color: red'>Failed Moves:* %d</span>\n", slb.stat_rebal_item_failed)
	ret += fmt.Sprintf("<i style='color: gray'> * Moving items failed because space couldn't be allocated</i>\n")
	ret += fmt.Sprintf("<span style='color: red'>Failed Store:* %d</span>\n", slb.stat_store_failed)
	ret += fmt.Sprintf("<i style='color: gray'> * Item failed to be stored because out of memory</i>\n")

	ret += fmt.Sprintf("\n--Other:\n<i style='color: #8844aa'>Forced GCs:* %d</i>\n", slb.stat_rebal_gc_needed)
	ret += fmt.Sprintf("<i style='color: gray'>Garbage collections needed just right after rebalance to clean memory of old items</i>\n")
	ret += fmt.Sprintf("<span style='color: #999922'>Rebalances interrupted:* %d</span>\n", slb.stat_rebal_slab_interrupted)
	ret += fmt.Sprintf("<i style='color: gray'> * If we created a new slab during rebalance, it'll break the process, see DESC192811 in code for more info</i>")

	ret += "</pre>"
	return ret
}

func _getSlabInfo(class_no int, stat_slab [8]int, p10, p50, p90 int) string {
	ret := fmt.Sprintf("<pre>Slab Class %d Summary\n", class_no)
	ret += "--------------------------\n\n"
	ret += fmt.Sprintf("Allocations: %d; (Size: %s)\n", stat_slab[1], hscommon.FormatBytes(uint64(stat_slab[0])))
	ret += fmt.Sprintf("Deletes: %d\n", stat_slab[2])
	ret += fmt.Sprintf("  Postponed: %d\n", stat_slab[3])
	ret += fmt.Sprintf("Garbage Collected: %d\n", stat_slab[4])
	ret += fmt.Sprintf("  Postponed: %d\n", stat_slab[5])

	ret += "<i style='color: #555555'>* Delete occurs eg. when we're replacing an item, GC occurs when the item is expiring.\n"
	ret += "If deleted/replaced item is read from, it'll show up as postponed delete/gc,\n"
	ret += "the request will serve the old item normally,while new requests will serve updated data.</i>\n\n"

	_p10, _p90, _p50 := "-", "-", "-"
	if p10 > 0 {
		_p10 = hscommon.FormatBytes(uint64(p10))
	}
	if p50 > 0 {
		_p50 = hscommon.FormatBytes(uint64(p50))
	}
	if p90 > 0 {
		_p90 = hscommon.FormatBytes(uint64(p90))
	}

	ret += fmt.Sprintf("Garbage Collections Run: %d\n", stat_slab[6])
	ret += fmt.Sprintf("Garbage Collections Skipped: %d\n", stat_slab[7])
	ret += "<i style='color: #555555'>Number of times that GC scanned the items or was skipped because of optimizations</i>\n\n"

	ret += fmt.Sprintf("10'th Percentile Size: %s\n", _p10)
	ret += fmt.Sprintf("50'th Percentile Size: %s\n", _p50)
	ret += fmt.Sprintf("90'th Percentile Size: %s\n", _p90)
	ret += "<i style='color: #555555'>* 10 / 50 / 90% of the items stored are smaller that SIZE</i>"
	return ret + "</pre>"
}

type itemstat struct {
	exp_stats       [4]int
	sum_items_used  int
	sum_items_avail int
	expired         int
	ttl_sum         uint64
}

func (this *itemstat) doItemStat(TTL int) {

	this.sum_items_used++
	if TTL <= 0 {
		this.expired++
		return
	}

	this.ttl_sum += uint64(TTL)
	switch {
	case TTL < 30:
		this.exp_stats[0]++
	case TTL < 120:
		this.exp_stats[1]++

	case TTL > 3000:
		this.exp_stats[2]++
	case TTL > 600:
		this.exp_stats[3]++
	}
}

func (this *itemstat) beginChunk(item_count int) {
	this.sum_items_avail += item_count
}

func (this *itemstat) getUsed() string {

	stat_perc_used := fmt.Sprintf("%.2f", float64(this.sum_items_used*100)/float64(this.sum_items_avail))
	if this.sum_items_used == 0 {
		stat_perc_used = " - "
	}

	return fmt.Sprintf("<b style='color: green'>%d</b> / %d (%s%%)", this.sum_items_used, this.sum_items_avail, stat_perc_used)

}

func (this *itemstat) getWasted() string {
	ret := "- %"
	if this.sum_items_avail > 0 {
		ret = fmt.Sprintf("%.2f%%", float64(this.expired*100)/float64(this.sum_items_avail))
	}

	return ret
}

func (this *itemstat) getAvgTTL() string {
	exp_avg := "-"
	if this.sum_items_used > 0 {
		exp_avg = fmt.Sprintf("%.1fs", float64(this.ttl_sum)/float64(this.sum_items_used))
	}

	return exp_avg
}
