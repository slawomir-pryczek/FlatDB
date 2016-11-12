package slab

import (
	"fmt"
	"handler_socket2"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
)

func MoveSlabsStat(slab_class_no uint16, slabs_scanned int, took_us uint64, items_moved, items_move_failed int) {

	slab_class := slab_classes[slab_class_no]
	slab_class.mu.Lock()
	slab_class.stat_rebal_took_us += took_us
	slab_class.stat_rebal_slabs_scanned += uint64(slabs_scanned)
	slab_class.stat_rebal_item_moved += uint64(items_moved)
	slab_class.stat_rebal_item_failed += uint64(items_move_failed)
	slab_class.mu.Unlock()

}

var auto_rebalancer_settings [][2]int = nil

func MoveSlabsStart(rbl_settings string) [][]int {

	rebalancer_settings := [][2]int{}

	if rbl_settings == "" {

		if auto_rebalancer_settings == nil {

			fmt.Println("Building default rebalancer config...")
			comment := ""
			auto_rebalancer_settings, comment = GetRebalanceSettings(rbl_settings)
			fmt.Println(comment)

		}
		rebalancer_settings = auto_rebalancer_settings

	} else {
		rebalancer_settings, _ = GetRebalanceSettings(rbl_settings)
	}

	type globalStats struct {
		slabs           int
		chunks_per_slab int
		chunks_free     int
	}
	stat := make([]globalStats, len(slab_classes))

	for slab_class_no, slab_class := range slab_classes {
		slab_class.mu.RLock()
		_tmp := make([]*slab, len(slab_class.slabs))
		copy(_tmp, slab_class.slabs)
		_slab_count := len(slab_class.slabs)
		slab_class.mu.RUnlock()

		stat[slab_class_no].chunks_per_slab = slab_class.slab_chunk_count
		for i := 0; i < _slab_count; i++ {

			chunk := _tmp[i]
			chunk.mu.Lock()
			stat[slab_class_no].slabs++
			stat[slab_class_no].chunks_free += chunk.chunks_free
			chunk.mu.Unlock()
		}
	}

	ret := make([]int, len(slab_classes))
	for slab_class_no, s := range stat {

		conf_min_used_percent, conf_desired_usage := 20, 70
		if len(rebalancer_settings) > slab_class_no {
			conf_min_used_percent = rebalancer_settings[slab_class_no][0]
			conf_desired_usage = rebalancer_settings[slab_class_no][1]
		}

		space := s.slabs * s.chunks_per_slab
		used := space - s.chunks_free // + (s.chunks_per_slab) // just to make sure that after removing one SLAB we still will be below the limit!

		perc_used := ((float64(used) * 100.0) / float64(space))
		if used == 0 {
			perc_used = 0
		}
		if s.slabs <= 3 || space < 5 || perc_used > float64(conf_min_used_percent) {
			//fmt.Println("Skipped ", slab_class_no, ":", perc_used, " < ", conf_min_used_percent)
			continue
		}

		fmt.Print("Probing ", slab_class_no, ": ")
		for space > 0 && s.slabs-ret[slab_class_no] > 2 /* minimum slabs that should be left */ {

			space -= s.chunks_per_slab
			ret[slab_class_no]++

			perc_used := ((float64(used) * 100.0) / float64(space))
			fmt.Printf("%.1f ", perc_used)

			if (float64(used)*100.0)/float64(space) > float64(99.8) {
				ret[slab_class_no]--
				break
			}

			if perc_used >= float64(conf_desired_usage) {
				break
			}

			// allow to move ONLY 25% of the slab class / 10 slabs at most (whichever is greater)
			// this is very important to prevent chain reaction that
			// could cause memory starvation
			// as many rebalances could lock slabs for writes and then copied values
			// would take all memory avail.
			//
			// because we can remove SLAB only if it's the last one in the slab
			// array!
			if ret[slab_class_no] > 10 && float64(ret[slab_class_no]) > float64(s.slabs)*0.25 {
				break
			}

		}
		fmt.Println("To remove SLABS: ", ret[slab_class_no])

	}

	ret2 := make([][]int, len(slab_classes))
	for slab_class_no, move_cnt := range ret {

		ret2[slab_class_no] = make([]int, 0, move_cnt)
		for i := 0; i < move_cnt; i++ {

			slab_no := stat[slab_class_no].slabs - i - 1

			ret2[slab_class_no] = append(ret2[slab_class_no], slab_no)
			__lock_for_read(uint16(slab_class_no), uint16(slab_no), true)
		}

	}

	return ret2
}

func MoveSlabsFinish(_cfg [][]int) {

	for slab_class_no, toremove_list := range _cfg {

		if len(toremove_list) == 0 {
			continue
		}

		rebal_gc_needed := 0
		for _, slab_no := range toremove_list {
			if RunGCForSlab(uint16(slab_class_no), uint16(slab_no)) {
				rebal_gc_needed++
			}
		}

		slab_class := slab_classes[slab_class_no]
		slab_class.mu.Lock()

		for len(slab_class.slabs) > 0 {

			_l := len(slab_class.slabs)
			slab := slab_class.slabs[_l-1]
			slab.mu.Lock()

			if slab.allow_inserts {
				slab.mu.Unlock()
				break
			}

			if slab.chunks_free != slab.chunks_count {
				fmt.Println("Cannot remove SLAB", _l, "items left:", slab.chunks_count-slab.chunks_free, "unlocked", slab.allow_inserts)
				slab.mu.Unlock()
				break
			}
			slab_class.slabs[_l-1] = nil
			slab_class.slabs = slab_class.slabs[:_l-1]
			slab_class.stat_slab_freed++
			slab.mu.Unlock()

			atomic.AddInt32(&slabs_allocated, -1)
			atomic.AddInt32(&slabs_freed, 1)
		}

		// DESC192811
		// if we failed to delete the slabs - make them WRITABLE again
		// it's very special situation, as SLABS can be deleted only from the end of the list
		// so if we started the rebalance (removing) process, and then there is not enough of memory to
		// hold NEWLY ARRIVING and MOVED items, new slab will be created like this
		// D - DATA
		// R - REBALANCED DATA (slabs in readonly mode)
		// N - NEW SLAB
		// DDDDDDRRRRRRR =>
		// DDDDDDRRRRRRRN
		// so at the end of the process we'd have empty R blocks that need to be
		// made writable again, because we cannot remove them
		//
		// Then the rebalance process needs to be repeated
		// This situation can be monitored using "Interrupted Slabs counter"
		for _, slab_no := range toremove_list {
			if slab_no < len(slab_class.slabs) {
				slab_class.slabs[slab_no].mu.Lock()
				slab_class.slabs[slab_no].allow_inserts = true
				slab_class.slabs[slab_no].mu.Unlock()

				slab_class.stat_rebal_slab_interrupted++
			}

		}

		slab_class.stat_rebal_slabs_rebalanced++
		slab_class.stat_rebal_gc_needed += rebal_gc_needed
		slab_class.mu.Unlock()
	}

}

func __lock_for_read(slab_class_no, slab_no uint16, lock bool) {

	sclass := slab_classes[slab_class_no]

	sclass.mu.RLock()
	if int(slab_no) >= len(sclass.slabs) {
		sclass.mu.RUnlock()
		return
	}
	slab := sclass.slabs[slab_no]
	sclass.mu.RUnlock()

	slab.mu.Lock()
	slab.allow_inserts = !lock
	slab.mu.Unlock()
}

func GetRebalanceSettings(rebalancer_settings_str string) ([][2]int, string) {

	comment := ""

	if rebalancer_settings_str == "" {
		rebalancer_settings_str = handler_socket2.Config.Get("REBALANCE", "20-70")
	}

	rebalancer_settings := strings.Split(rebalancer_settings_str, ",")
	slab_classes_count := len(slab_classes)

	_tmp := make([][2]int, 0, len(rebalancer_settings))
	for _, v := range rebalancer_settings {
		vv := strings.Split(v, "-")
		if len(vv) < 2 {
			continue
		}
		vvv, err := strconv.ParseInt(vv[0], 10, 64)
		if err != nil {
			continue
		}
		vvv2, err2 := strconv.ParseInt(vv[1], 10, 64)
		if err2 != nil {
			continue
		}

		_tmp = append(_tmp, [2]int{int(vvv), int(vvv2)})
	}
	if len(_tmp) == 0 {
		_tmp = append(_tmp, [2]int{20, 70})
	}

	settings := make([][2]int, slab_classes_count)
	step_size := float64(len(_tmp)) / float64(slab_classes_count)

	for i := 0; i < slab_classes_count && i < ((slab_classes_count+1)/2); i++ {

		pos1 := int(math.Floor(float64(i) * step_size))
		pos2 := len(_tmp) - 1 - int(math.Floor(float64(i)*step_size))

		if pos1 > len(_tmp)-1 {
			pos1 = len(_tmp) - 1
		}
		if pos2 < 0 {
			pos2 = 0
		}
		comment += fmt.Sprintf("FORSLAB%.2d %d(%d->%d) $$$$$$$ FORSLAB%.2d %d(%d->%d)\n", i, pos1, _tmp[pos1][0], _tmp[pos1][1],
			slab_classes_count-i-1, pos2, _tmp[pos2][0], _tmp[pos2][1])
		settings[i] = _tmp[pos1]
		settings[slab_classes_count-i-1] = _tmp[pos2]
	}

	comment += fmt.Sprintln("Rebalancer settings:", _tmp)
	comment += fmt.Sprintln("Rebalancer perslab settings:", settings)
	return settings, comment
}
