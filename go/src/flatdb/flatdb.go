package main

import (
	"handler_socket2"

	"fmt"
	"handler_socket2/handle_echo"
	"handler_socket2/handle_profiler"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"
)

const version = "FlatDB Server v3.001"

func main() {

	num_cpu := runtime.NumCPU() * 2
	runtime.GOMAXPROCS(num_cpu)

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
	fmt.Println("Welcome to", version, "2015-2016")
	fmt.Println("Visit https://github.com/slawomir-pryczek/FlatDB for more info and documentation")
	fmt.Println("Configuration is found in conf.json")
	fmt.Println("---")

	// register handlers
	handlers := []handler_socket2.ActionHandler{&HandleStore{}, &handle_profiler.HandleProfiler{},
		&handle_echo.HandleEcho{}}
	if len(handler_socket2.Config.Get("RUN_SERVICES", "")) > 0 && handler_socket2.Config.Get("RUN_SERVICES", "") != "*" {
		_h_modified := []handler_socket2.ActionHandler{}
		_tmp := strings.Split(handler_socket2.Config.Get("RUN_SERVICES", ""), ",")
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

	// start the server
	handler_socket2.RegisterHandler(handlers...)
	handler_socket2.StartServer(strings.Split(handler_socket2.Config.Get("BIND_TO", ""), ","))

}
