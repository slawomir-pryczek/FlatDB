package handler_socket2

import "sync"
import "fmt"
import "os"
import "io/ioutil"
import "path/filepath"
import "encoding/json"
import "strconv"
import "net"
import "strings"

var Config = new(cfg)

type cfg struct {
	is_ready bool
	config   map[string]string
	mu       sync.Mutex

	debug, verbose bool
}

func (this *cfg) parse() {

	this.mu.Lock()

	if this.is_ready {
		this.mu.Unlock()
		return
	}

	//----

	conf_path := "conf.json"

	if path, err := os.Readlink("/proc/self/exe"); err == nil {
		path = filepath.Dir(path)
		conf_path = path + "/conf.json"
	} else {
		fmt.Println("Can't find executable directory, using current dir for config!")
	}

	data, err := ioutil.ReadFile(conf_path)
	if err != nil {
		fmt.Println("FATAL Error opening configuration file conf.json:", err)
		os.Exit(1)
	}

	var cfg_tmp map[string]interface{}
	json.Unmarshal(data, &cfg_tmp)

	this.config = make(map[string]string)

	for k, v := range cfg_tmp {

		switch v.(type) {
		case string:
			this.config[k] = v.(string)
		case int:
			this.config[k] = strconv.Itoa(v.(int))
		case float64:
			this.config[k] = strconv.FormatFloat(v.(float64), 'f', 3, 64)
		case bool:
			if v.(bool) {
				this.config[k] = "1"
			} else {
				this.config[k] = "0"
			}
		}

	}

	fmt.Println("Config: ", this.config)

	this.debug = this.config["DEBUG"] == "1"
	this.verbose = this.config["VERBOSE"] == "1"

	//----

	// add more interfaces based on what is currently available
	ip_ifaces_uniq := make(map[string]bool)
	bind_to_ip := strings.Split(this.config["BIND_TO"], ",")
	for k, v := range bind_to_ip {
		v = strings.Trim(v, "\r\n\t ")
		if ip_ifaces_uniq[v] {
			continue
		}
		bind_to_ip[k] = v
		ip_ifaces_uniq[v] = true
	}

	for _, iface_match := range getMatchInterfaces() {

		_key := "BIND_TO_" + iface_match
		if v, exists := this.config[_key]; exists {

			vv := strings.Split(v, ",")
			fmt.Println("Conditional bind", _key, "adding -", vv)

			for _, vvv := range vv {
				vvv = strings.Trim(vvv, "\r\n\t ")
				if ip_ifaces_uniq[vvv] {
					continue
				}
				bind_to_ip = append(bind_to_ip, vvv)
			}
		}
	}

	this.is_ready = true
	this.mu.Unlock()
	return
}

func (this *cfg) Get(attr, def string) string {

	if !this.is_ready {
		this.parse()
	}

	if val, ok := this.config[attr]; ok {
		return val
	}

	return def
}

func (this *cfg) GetB(attr string) bool {

	if !this.is_ready {
		this.parse()
	}

	if val, ok := this.config[attr]; ok && val == "1" {
		return true
	}

	return false
}

func (this *cfg) GetI(attr string, def int) int {

	if !this.is_ready {
		this.parse()
	}

	if _, ok := this.config[attr]; !ok {
		return def
	}

	if ret, err := strconv.ParseInt(this.config[attr], 10, 64); err == nil {
		return int(ret)
	}

	return def
}

func CfgIsDebug() bool {
	return Config.debug
}

func CfgIsVerbose() bool {
	return Config.verbose
}

func getMatchInterfaces() []string {

	verbose := CfgIsVerbose()

	match_ifaces := make([]string, 0)
	ifaces, err := net.Interfaces()
	if err != nil {
		fmt.Println("Cannot read interfaces (0x2) ", err.Error())
		os.Exit(2)
	}

	for _, iface := range ifaces {

		addrs, err := iface.Addrs()
		if err != nil {
			fmt.Println("Cannot read interfaces (0x3) ", err.Error())
			os.Exit(3)
		}

		for _, addr := range addrs {

			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			default:
				continue
			}

			if ip.To4() != nil {
				// ipv4 processing
				pieces := strings.Split(ip.String(), ".")
				if verbose {
					fmt.Print("Interface V4 ", iface.Name, " ... ")
				}

				for i := 0; i < len(pieces); i++ {
					_m := strings.Join(pieces[i:], ".")
					match_ifaces = append(match_ifaces, _m)
					if verbose {
						fmt.Print(_m, " ")
					}
				}
			} else {
				if verbose {
					fmt.Print("Interface V6 ", iface.Name, " ... ", ip.String())
				}
				match_ifaces = append(match_ifaces, ip.String())
			}

			if verbose {
				fmt.Println()
			}
		}
	}

	return match_ifaces
}
