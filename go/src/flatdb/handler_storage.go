package main

import (
	"encoding/binary"
	"encoding/json"
	"gocached/common"
	"gocached/ops"
	"handler_socket2"
	"strconv"
)

type HandleStore struct {
}

func (this *HandleStore) Initialize() {

}

func (this *HandleStore) Info() string {
	return "Handle Storage"
}

func (this *HandleStore) GetActions() []string {
	return []string{"mc-get", "mc-set", "mc-add", "mc-del", "mc-rep", "mc-touch",
		"mc-inc", "mc-dec", "mc-eget",
		"mca-get", "mca-insert", "mca-mget", "mca-fget", "mca-fpeek",
		"mc-maint"}
}

func (this *HandleStore) HandleAction(action string, data *handler_socket2.HSParams) string {

	if action == "mc-del" {
		k := data.GetParam("k", "")

		if !ops.OpDelete(k) {
			data.SetRespHeader("mc-error", "1")
			return "error:item not found"
		}

		return ""
	}

	if action == "mc-set" {

		k := data.GetParam("k", "")
		v := data.GetParamBUnsafe("v", nil)
		if v == nil {
			return "error:value needed"
		}

		e_ := data.GetParam("e", "")
		e, _ := strconv.ParseUint(e_, 10, 32)

		if ops.OpSetKey(k, v, uint32(e)+uint32(common.TSNow())) {
			return "ok"
		} else {
			data.SetRespHeader("mc-error", "1")
			return "error:full"
		}
	}

	if action == "mc-add" {

		k := data.GetParam("k", "")
		v := data.GetParamBUnsafe("v", nil)
		if v == nil {
			return "error:value needed"
		}
		e_ := data.GetParam("e", "")
		e, _ := strconv.ParseUint(e_, 10, 32)

		if ops.OpAddKey(k, v, uint32(e)+uint32(common.TSNow())) {
			return "ok"
		} else {
			data.SetRespHeader("mc-error", "1")
			return "error:exists"
		}
	}

	if action == "mc-rep" {

		k := data.GetParam("k", "")
		v := data.GetParamBUnsafe("v", nil)
		if v == nil {
			return "error:value needed"
		}
		e_ := data.GetParam("e", "")
		e, _ := strconv.ParseUint(e_, 10, 32)

		if ops.OpReplaceKey(k, v, uint32(e)+uint32(common.TSNow())) {
			return "ok"
		} else {
			data.SetRespHeader("mc-error", "1")
			return "error:key not found"
		}
	}

	if action == "mc-get" {

		k := data.GetParam("k", "")
		ret := ops.OpGetKey(k, func(size int) []byte {
			if size < 256 {
				return make([]byte, 0, size)
			}
			return data.Allocate(size)
		})

		if ret == nil {
			data.SetRespHeader("mc-error", "1")
			return ""
		}
		data.FastReturnBNocopy(ret)
		return ""
	}

	if action == "mc-touch" {
		k := data.GetParam("k", "")
		e_ := data.GetParam("e", "")
		e, _ := strconv.ParseUint(e_, 10, 32)

		var cas *uint32 = nil
		if _c := data.GetParam("cas", ""); len(_c) > 0 {
			if __c, err := strconv.ParseUint(_c, 10, 32); err == nil {
				___c := uint32(__c)
				cas = &___c
			}
		}

		if ops.OpTouch(k, uint32(e)+uint32(common.TSNow()), cas) {
			return ""
		}

		data.SetRespHeader("mc-error", "1")
		return "error:notfound"
	}

	if action == "mca-get" {

		k := data.GetParam("k", "")
		ret, cas, expire := ops.OpAdvancedGetKey(k, func(size int) []byte {
			if size < 256 {
				return make([]byte, 0, size)
			}
			return data.Allocate(size)
		})

		if ret == nil {
			data.SetRespHeader("mc-error", "1")
			return ""
		}

		data.SetRespHeader("cas", strconv.Itoa(int(cas)))
		data.SetRespHeader("e", strconv.Itoa(int(expire)-common.TSNow()))
		data.FastReturnBNocopy(ret)
		return ""
	}

	if action == "mca-fpeek" {

		k_ := data.GetParamBUnsafe("k", nil)
		if k_ == nil {
			return "Please specify k as json array, one key per piece"
		}
		k := make([]string, 0)
		err := json.Unmarshal(k_, &k)
		if err != nil {
			return "Error in k: " + err.Error()
		}

		for _, key := range k {
			if ops.OpExists(key) {
				return key
			}
		}

		data.SetRespHeader("mc-error", "1")
		return "key not found"
	}

	/* get first key found or return error */
	if action == "mca-fget" {

		k_ := data.GetParamBUnsafe("k", nil)
		if k_ == nil {
			return "Please specify k as json array, one key per piece"
		}
		k := make([]string, 0)
		err := json.Unmarshal(k_, &k)
		if err != nil {
			return "Error in k: " + err.Error()
		}

		for _, key := range k {
			ret, cas, expire := ops.OpAdvancedGetKey(key, func(size int) []byte {
				if size < 256 {
					return make([]byte, 0, size)
				}
				return data.Allocate(size)
			})

			if ret != nil {
				data.SetRespHeader("k", key)
				data.SetRespHeader("cas", strconv.Itoa(int(cas)))
				data.SetRespHeader("e", strconv.Itoa(int(expire)-common.TSNow()))
				data.FastReturnBNocopy(ret)
				return ""
			}
		}

		data.SetRespHeader("mc-error", "1")
		return "no key found"
	}

	if action == "mca-mget" {

		k_ := data.GetParamBUnsafe("k", nil)
		if k_ == nil {
			return "Please specify k as json array, one key per piece"
		}
		k := make([]string, 0)
		err := json.Unmarshal(k_, &k)
		if err != nil {
			return "Error in k: " + err.Error()
		}

		type gret struct {
			cas      uint32
			e        uint32
			datasize uint32
			data     []byte
		}

		output := make([]gret, 0, len(k))
		size_total := 0
		for _, key := range k {
			ret, cas, expire := ops.OpAdvancedGetKey(key, func(size int) []byte {
				if size < 256 {
					return make([]byte, 0, size)
				}
				return data.Allocate(size)
			})

			tmp := gret{}
			tmp.data = ret
			if data == nil {
				tmp.datasize = 0
			} else {
				tmp.datasize = uint32(len(ret))
				tmp.cas = cas
				tmp.e = expire
			}

			output = append(output, tmp)
			size_total += (12 + len(ret))
		}

		ret := data.Allocate(size_total)
		pos := 0
		now := uint32(common.TSNow())
		for _, piece := range output {

			if piece.e > 0 {
				piece.e = piece.e - now
			}
			binary.LittleEndian.PutUint32(ret[pos:pos+4], piece.cas)
			pos += 4
			binary.LittleEndian.PutUint32(ret[pos:pos+4], piece.e)
			pos += 4
			binary.LittleEndian.PutUint32(ret[pos:pos+4], piece.datasize)
			pos += 4
			copy(ret[pos:pos+int(piece.datasize)], piece.data)
			pos += int(piece.datasize)
		}

		data.FastReturnBNocopy(ret[0:size_total])
		return ""
	}

	if action == "mca-insert" {

		k := data.GetParam("k", "")
		v := data.GetParamBUnsafe("v", nil)

		e_ := data.GetParam("e", "")
		e, _ := strconv.ParseUint(e_, 10, 32)

		var cas *uint32 = nil
		if _c := data.GetParam("cas", ""); len(_c) > 0 {
			if __c, err := strconv.ParseUint(_c, 10, 32); err == nil {
				___c := uint32(__c)
				cas = &___c
			}
		}

		result, cas, expires, value := ops.OpAdvancedInsert(k, v, uint32(e)+uint32(common.TSNow()), cas, func(size int) []byte {
			if size < 256 {
				return make([]byte, 0, size)
			}
			return data.Allocate(size)
		})

		if cas != nil {
			data.SetRespHeader("cas", strconv.Itoa(int(*cas)))
			data.SetRespHeader("e", strconv.Itoa(int(expires)-common.TSNow()))
		}

		if result == ops.ADVI_INSERT_OK || result == ops.ADVI_DELETE_OK {
			return "ok"
		}

		data.SetRespHeader("mc-error", strconv.Itoa(int(result)))
		if value != nil {
			data.SetRespHeader("mc-curr-val", "1")
			data.FastReturnBNocopy(value)
		}

		return ""
	}

	if action == "mc-maint" {

		operation := data.GetParam("op", "")

		if operation == "flush" {
			return ops.OpFlush()
		}

		if operation == "store" {
			return ops.StoreToFiles()
		}

		if operation == "rebalance" {
			sett := data.GetParam("config", "")
			_sett := sett
			if _sett == "" {
				_sett = "default settings from conf.json"
			}

			return "Rebalance Config (config param): " + _sett + "\n" + ops.OpRebalance(sett)
		}

		data.SetRespHeader("mc-error", "1")
		if operation == "" {
			return "Please specify operation (op)"
		}
		return "Invalid operation: " + operation
	}

	op_decrement := action == "mc-dec"
	op_increment := action == "mc-inc"
	if op_decrement || op_increment {
		k := data.GetParam("k", "")
		v := data.GetParam("v", "")

		var exp *uint32 = nil
		e_ := data.GetParam("e", "")
		if len(e_) > 0 {
			if e_, err := strconv.ParseUint(e_, 10, 32); err == nil {
				e__ := uint32(e_) + uint32(common.TSNow())
				exp = &e__
			}
		}

		ret := ops.OpArithmetics(k, []byte(v), op_increment, exp)
		if ret == nil {
			data.SetRespHeader("mc-error", "1")
			return "error:math op fail"
		}
		return string(ret)
	}

	if action == "mc-eget" {
		k := data.GetParam("k", "")
		if k == "" {
			return "Please specify k, item key "
		}

		exp := ops.OpGetExpires(k)
		if exp == 0 {
			data.SetRespHeader("mc-error", "1")
			return "error:key not found"
		}

		return strconv.Itoa(int(exp - uint32(common.TSNow())))
	}

	return "G"
}
