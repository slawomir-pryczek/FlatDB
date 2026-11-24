package main

import (
	"FlatDB/gocached/common"
	"FlatDB/gocached/ops"
	"FlatDB/tools/ringlog"
	"fmt"
	"github.com/slawomir-pryczek/HSServer/handler_socket2"
	"github.com/slawomir-pryczek/HSServer/handler_socket2/config"
	"github.com/slawomir-pryczek/HSServer/handler_socket2/handle_echo"
	"github.com/slawomir-pryczek/HSServer/handler_socket2/handle_profiler"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strings"
	"time"

	"FlatDB/gocached"
)

const version = "FlatDB Server v4.0"

var buildInfo = "Can't read build info"

func init() {
	ringlog.SetConfig("mover", "cap", 10)
	ringlog.SetConfig("gcwalk", "cap", 20)

	{
		bi, ok := debug.ReadBuildInfo()
		if !ok {
			return
		}
		rev, ts, dirty := "unknown", "", false
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				rev = s.Value
			case "vcs.time":
				ts = s.Value
			case "vcs.modified":
				dirty = s.Value == "true"
			}
		}
		if len(rev) > 7 {
			rev = rev[:7]
		}
		if dirty {
			rev += "-dirty"
		}
		if t, err := time.Parse(time.RFC3339, ts); err == nil {
			ts = t.Local().Format("2006-01-02 15:04")
		}

		feature := "n/a (not amd64)"
		for _, s := range bi.Settings {
			if s.Key != "GOAMD64" {
				continue
			}
			switch s.Value {
			case "v1":
				feature = "v1 (baseline x86-64, SSE2)"
			case "v2":
				feature = "v2 (SSE4.2, POPCNT; ~2009+ Nehalem)"
			case "v3":
				feature = "v3 (AVX2, BMI2, FMA; ~2013+ Haswell/Zen)"
			case "v4":
				feature = "v4 (AVX-512; Zen 4+, Intel server)"
			default:
				feature = s.Value
			}
		}

		buildInfo = rev + " " + ts + " " + bi.GoVersion + " " + feature
	}
}

func main() {

	num_cpu := runtime.NumCPU() * 2
	runtime.GOMAXPROCS(num_cpu)

	handler_socket2.StatusPluginRegister(func() (string, string) {
		var sb strings.Builder
		sb.WriteString("<pre>")

		addLog := func(rl *ringlog.RingLog) {
			if rl.Len() == 0 {
				sb.WriteString(" - Nothing to display - \n")
			}
			rl.DoRaw(func(t time.Time, s, className string) {
				sb.WriteString(t.Format(time.DateTime) + " " + s + "\n")
			})
		}

		// Rebalancer logs
		sb.WriteString(" :: Slab Mover/Rebalancer Logs \n")
		addLog(ringlog.GetStaticLog("mover"))

		// GC Walk Logs
		sb.WriteString("\n :: GCWalk Logs \n")
		addLog(ringlog.GetStaticLog("gcwalk"))

		sb.WriteString("</pre>")
		return "Logs", sb.String()
	})

	/*l, err := net.ListenUnix("unix", &net.UnixAddr{"/tmp/uds", "unix"})
	if err != nil {
		panic(err)
	}
	defer os.Remove("/tmp/uds")

	for {
		conn, err := l.AcceptUnix()
		if err != nil {
			panic(err)
		}

		i := 0
		var buf [1024]byte

		for {

			n, err := conn.Read(buf[:])
			if err == io.EOF {
				conn.Close()
				break
			}

			if err != nil {
				fmt.Println(err.Error())
				panic(err)
			}

			i++

			if false {

				conn.Write(buf[:n])

			}

			if i%10000 == 0 {
				fmt.Println(i, ">>", len(buf))
			}
		}

		conn.Close()
	}

	return*/

	if false {
		f, _ := os.Create("profiler5")
		pprof.StartCPUProfile(f)

		go func() {
			time.Sleep(10 * time.Second)
			pprof.StopCPUProfile()
		}()
	}

	fmt.Println("---")
	fmt.Println("Welcome to", version, "2015-2026.", buildInfo)
	fmt.Println("Visit https://github.com/slawomir-pryczek/FlatDB for more info and documentation")
	fmt.Println("Configuration is found in conf.json")
	fmt.Println("---")

	// register handlers
	handlers := []handler_socket2.ActionHandler{&HandleStore{}, &handle_profiler.HandleProfiler{},
		&handle_echo.HandleEcho{}}
	if len(config.Config().Get("RUN_SERVICES", "")) > 0 && config.Config().Get("RUN_SERVICES", "") != "*" {
		_h_modified := []handler_socket2.ActionHandler{}
		_tmp := strings.Split(config.Config().Get("RUN_SERVICES", ""), ",")
		supported := make(map[string]bool)
		for _, v := range _tmp {
			supported[strings.Trim(v, "\r\n \t")] = true
		}

		for _, v := range handlers {

			should_enable := false
			for _, action := range handler_socket2.ActionHandler(v).GetActions() {
				if supported[action] {
					should_enable = true
					break
				}
			}

			if should_enable {
				_h_modified = append(_h_modified, v)
			}
		}

		handlers = _h_modified
	}

	if false {
		gocached.Run_kvs_testing()
	}

	if false {
		ops.OpRebalance("20-70")

		data := []byte(common.RandString(2 * 60))
		data2 := []byte(common.RandString(2 * 20))
		data3 := []byte(common.RandString(2 * 2))

		now := common.TSNow()
		for i := 0; i < 1000000; i++ {
			k := common.RandString(15)
			ops.OpSetKey("1"+k, data, uint32(now+i%20))
			ops.OpSetKey("2"+k, data2, uint32(now+i%20))
			ops.OpSetKey("3"+k, data3, uint32(now+i%20))

			if i%100 == 0 {
				ops.OpSetKey("11"+k, data, uint32(now+3600))
				ops.OpSetKey("12"+k, data2, uint32(now+3600))
				ops.OpSetKey("13"+k, data3, uint32(now+3600))
			}
		}

		go func() {
			time.Sleep(2 * time.Second)
			for {
				//		ops.OpRebalance("50-70")
				time.Sleep(1 * time.Second)
			}
		}()
	}

	// start the server
	handler_socket2.RegisterHandler(handlers...)
	handler_socket2.StartServer(strings.Split(config.Config().Get("BIND_TO", ""), ","))

}
