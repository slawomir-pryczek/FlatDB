package handle_echo

import (
	"encoding/json"
	"handler_socket2"
)

type HandleEcho struct {
}

func (this *HandleEcho) Initialize() {

	handler_socket2.StatusPluginRegister(func() (string, string) {
		return "Echo", "Echo plugin is enabled"
	})

}

func (this *HandleEcho) Info() string {
	return "This plugin will send back received data"
}

func (this *HandleEcho) GetActions() []string {
	return []string{"echo"}
}

func (this *HandleEcho) HandleAction(action string, data *handler_socket2.HSParams) string {

	ret := map[string]string{}
	ret["data"] = data.GetParam("data", "")
	_tmp, _ := json.Marshal(ret)

	return string(_tmp)
}
