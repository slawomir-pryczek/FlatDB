package ringlog

import (
	"fmt"
	"html"
	"strings"
	"sync"
	"time"
)

var globalLogs map[string]*RingLog = make(map[string]*RingLog)
var mu sync.RWMutex
var onWrite = func(e entry, logName string) {
	fmt.Printf("%s - %s\n", logName, e)
}

func GetStaticLogBegin(logName string) *writeMany {
	return GetStaticLog(logName).WriteMany()
}

func GetStaticLog(logName string) *RingLog {
	mu.RLock()
	if ret, ok := globalLogs[logName]; ok {
		mu.RUnlock()
		return ret
	}
	mu.RUnlock()

	mu.Lock()
	defer mu.Unlock()
	// Re-check after acquiring write lock (another goroutine may have inserted)
	if ret, ok := globalLogs[logName]; ok {
		return ret
	}
	tmp := NewRingLog(logName)
	globalLogs[logName] = tmp
	return tmp
}

type entry struct {
	time  time.Time
	event string
	class string
}

func (e entry) String() string {
	out := e.event
	if strings.Index(out, "#DATE#") != -1 {
		d := fmt.Sprintf("<span class='t'>%s</span>", e.time.Format(time.DateTime))
		out = strings.Replace(out, "#DATE#", d, -1)
	}
	return out
}

// RingLog is a fixed-capacity, thread-safe ring buffer of log events.
// The backing arrays are allocated once in NewRingLog and never grown.
type RingLog struct {
	logName string

	mu    sync.Mutex
	entry []entry
	head  int // index of next write slot, always in [0, len(times))
	len   int
}

// NewRingLog allocates the backing arrays once and returns a ready-to-use log.
// capacity must be >= 1.
func NewRingLog(logName string) *RingLog {
	capacity := getConfigInt(logName, "cap", 50)

	if capacity < 1 {
		capacity = 1
	}
	return &RingLog{entry: make([]entry, capacity), logName: logName}
}

// Write records a new event, overwriting the oldest slot when the ring is full.
func (r *RingLog) Write(event string, args ...any) {
	if len(args) > 0 {
		event = fmt.Sprintf(event, args...)
	}
	r.WriteClass(event, "")
}

// Write records a new event, overwriting the oldest slot when the ring is full.
func (r *RingLog) WriteClass(event string, class string) {
	_tnow := time.Now()

	r.mu.Lock()
	r.entry[r.head] = entry{_tnow, event, class}
	r.head = (r.head + 1) % len(r.entry)
	if r.len < len(r.entry) {
		r.len++
	}
	r.mu.Unlock()

	onWrite(entry{_tnow, event, class}, r.logName)
}

// Cap returns the maximum number of events the ring can hold.
func (r *RingLog) Cap() int { return len(r.entry) }

// Len returns the number of events written (up to Cap).
func (r *RingLog) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.len
}

// Do calls fn for every written event in reverse chronological order (newest to oldest).
// fn receives a preformatted "TIME: EVENT" string.
func (r *RingLog) DoRaw(fn func(time time.Time, event, className string)) {

	// create snapshot
	r.mu.Lock()
	n, cap := r.len, len(r.entry)
	snap := make([]entry, n)

	//for i := 0; i < n; i++ {
	//	snap[i] = r.entry[(r.head-1-i+cap)%cap]
	//}

	start := r.head - n + cap
	for i := 0; i < n; i++ {
		snap[i] = r.entry[(start+i)%cap]
	}
	r.mu.Unlock()

	for _, e := range snap {
		event := e.event
		if strings.Index(e.event, "#DATE#") != -1 {
			repl := fmt.Sprintf("<span class='t'>%s</span>", e.time.Format(time.DateTime))
			event = strings.Replace(event, "#DATE#", repl, -1)
		}

		fn(e.time, e.event, e.class)
	}
}

func (r *RingLog) Do(fn func(s string)) {
	r.DoRaw(func(t time.Time, event, className string) {
		if len(className) == 0 {
			className = "default"
		}
		className = html.EscapeString(className)
		fn(fmt.Sprintf("<span class='%s'>%s</span>\n", className, html.EscapeString(event)))
	})
}

// Reset clears the log without releasing memory.
func (r *RingLog) Reset() {
	r.mu.Lock()
	for i := range r.entry {
		r.entry[i] = entry{time: time.Time{}, event: "", class: ""}
	}
	r.head = 0
	r.len = 0
	r.mu.Unlock()
}

type writeMany struct {
	dest         *RingLog
	classDefault string

	entry []entry
}

func (r *RingLog) NullLog() *writeMany {
	return nil
}

func (r *RingLog) WriteMany() *writeMany {
	return &writeMany{dest: r, entry: make([]entry, 0, 20)}
}

func (w *writeMany) SetClass(class string) *writeMany {
	if w == nil {
		return w
	}
	w.classDefault = class
	return w
}

func (w *writeMany) Write(format string, args ...any) {
	if w == nil {
		return
	}

	if len(args) > 0 {
		format = fmt.Sprintf(format, args...)
	}
	if len(format) > 0 {
		w.entry = append(w.entry, entry{time: time.Now(), event: format, class: w.classDefault})
	}
}

func (w *writeMany) Commit() {
	if w == nil || len(w.entry) == 0 {
		return
	}

	ring := w.dest
	ring.mu.Lock()
	for _, v := range w.entry {
		ring.entry[ring.head] = v
		ring.head = (ring.head + 1) % len(ring.entry)
		if ring.len < len(ring.entry) {
			ring.len++
		}
	}
	ring.mu.Unlock()

	for _, v := range w.entry {
		onWrite(v, w.dest.logName)
	}
	w.entry = w.entry[:0]
}

func (w *writeMany) CommitSingle() {
	if w == nil || len(w.entry) == 0 {
		return
	}

	event := make([]string, 0, len(w.entry))
	for _, entry := range w.entry {
		event = append(event, entry.event)
	}

	w.entry = w.entry[0:1]
	w.entry[0].event = strings.Join(event, " ")
	w.Commit()
}

func (w *writeMany) GetLineCount() int {
	if w == nil {
		return 0
	}
	return len(w.entry)
}
