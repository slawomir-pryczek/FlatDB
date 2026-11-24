package ringlog

import (
	"strconv"
	"sync"
	"sync/atomic"
)

var (
	config   atomic.Pointer[map[string]any]
	configMu sync.Mutex // serializes writers only
)

func init() {
	m := map[string]any{}
	config.Store(&m)
}

var OnGetConfig = func(logName string, attrib string, def any) any {
	if v, ok := (*config.Load())[logName+"-"+attrib]; ok {
		return v
	}
	return def
}

func SetConfig(logName string, attrib string, val any) {
	cfg_key := logName + "-" + attrib

	configMu.Lock()
	defer configMu.Unlock()

	old := *config.Load()
	m := make(map[string]any, len(old)+1)
	for k, v := range old {
		m[k] = v
	}
	m[cfg_key] = val
	config.Store(&m)
}

func getConfigString(logName string, attrib string, def string) string {
	switch v := OnGetConfig(logName, attrib, def).(type) {
	case string:
		return v
	case bool:
		if v {
			return "1"
		}
		return "0"
	case int:
		return strconv.Itoa(v)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	}
	return def
}

func getConfigInt(logName string, attrib string, def int) int {
	switch v := OnGetConfig(logName, attrib, def).(type) {
	case int:
		return v
	case bool:
		if v {
			return 1
		}
		return 0
	case int32:
		return int(v)
	case int64:
		return int(v)
	case float64:
		return int(v)
	case string:
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
