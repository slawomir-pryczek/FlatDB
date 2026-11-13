package ops

import (
	"fmt"
	"gocached/slab"
	"handler_socket2"
	"handler_socket2/hscommon"
	"math"
	"strings"
)

func init() {

	handler_socket2.StatusPluginRegister(func() (string, string) {

		tg := hscommon.NewTableGen("Hash Table No.", "Old Items", "Curr Items", "Passes / Eff.",
			"GC Item Touch", "GC Item Recl.",
			"Operations", "Hits", "Miss", "Set", "Add", "Repl", "Inc", "Dec")
		tg.SetClass("tab httab")

		sum_old := 0
		sum_items := 0
		for k, kvs := range kvstores {

			kvs.mu.RLock()

			items_old := len(kvs.key_map_old)
			items := len(kvs.key_map)
			sum_old += items_old
			sum_items += items

			perc_coll := "0.00"
			if kvs.stat_gc_collected+kvs.stat_gc_kept > 0 {
				perc_coll = fmt.Sprintf("%.1f", float64(kvs.stat_gc_collected*100)/float64(kvs.stat_gc_collected+kvs.stat_gc_kept))
			}
			gc_stats := fmt.Sprintf("%dp / %s%%", kvs.stat_gc_passes, perc_coll)
			gc_passes := fmt.Sprintf("%d", kvs.stat_gc_collected+kvs.stat_gc_kept)
			gc_collected := fmt.Sprintf("%d", kvs.stat_gc_collected)

			operations_status := _genStatusBar(int(kvs.stat_atomic_get_hit+kvs.stat_atomic_get_nohit),
				kvs.stat_set, kvs.stat_add, kvs.stat_replace, kvs.stat_increment, kvs.stat_decrement)

			_hit_stats := fmt.Sprintf("%d (%.2f%%)", kvs.stat_atomic_get_hit, float64(kvs.stat_atomic_get_hit*100)/float64(kvs.stat_atomic_get_hit+kvs.stat_atomic_get_nohit))

			tg.AddRow(fmt.Sprintf("%d", k), fmt.Sprintf("%d", items_old), fmt.Sprintf("%d", items),
				gc_stats, gc_passes, gc_collected, operations_status,
				_hit_stats, fmt.Sprint(kvs.stat_atomic_get_nohit),
				fmt.Sprint(kvs.stat_set), fmt.Sprint(kvs.stat_add), fmt.Sprint(kvs.stat_replace), fmt.Sprint(kvs.stat_increment), fmt.Sprint(kvs.stat_decrement))
			kvs.mu.RUnlock()

		}

		all := fmt.Sprintf("%d", sum_old+sum_items)
		tg.AddRow("Sum	&#931;", fmt.Sprintf("%d", sum_old), fmt.Sprintf("%d", sum_items), " ", "All Items: "+all)

		return "Hash Tables Status", tg.Render()

	})

	handler_socket2.StatusPluginRegister(func() (string, string) {
		return "Slab Status", slab.GetSlabInfo()
	})

}

func _genStatusBar(get, set, add, repl, inc, dec int) string {

	chars := 30
	sum := int(get+set+add+repl+inc+dec) + 1

	stats := [6]int{0, 0, 0, 0, 0, 0}

	stats[0] = int(math.Ceil((float64(get) / float64(sum)) * float64(chars)))
	stats[1] = int(math.Ceil((float64(set) / float64(sum)) * float64(chars)))
	stats[2] = int(math.Ceil((float64(add) / float64(sum)) * float64(chars)))
	stats[3] = int(math.Ceil((float64(repl) / float64(sum)) * float64(chars)))
	stats[4] = int(math.Ceil((float64(inc) / float64(sum)) * float64(chars)))
	stats[5] = int(math.Ceil((float64(dec) / float64(sum)) * float64(chars)))
	for k, v := range stats {
		if v < 0 {
			stats[k] = 0
		}
	}

	for stats[0]+stats[1]+stats[2]+stats[3]+stats[4]+stats[5] > chars {
		max := 0
		pos := 0
		for p, v := range stats {
			if v > max {
				max = v
				pos = p
			}
		}

		stats[pos]--
	}

	ret := "<span class='htstaus'>"
	ret += "<span class='ht1'>" + strings.Repeat("<", stats[0]) + "</span>"
	ret += "<span class='ht2'>" + strings.Repeat("=", stats[1]) + "</span>"
	ret += "<span class='ht3'>" + strings.Repeat("!", stats[2]) + "</span>"
	ret += "<span class='ht4'>" + strings.Repeat("&#9654;", stats[3]) + "</span>"
	ret += "<span class='ht5'>" + strings.Repeat("+", stats[4]) + "</span>"
	ret += "<span class='ht6'>" + strings.Repeat("-", stats[5]) + "</span>"
	ret += "</span>"

	return ret
}
