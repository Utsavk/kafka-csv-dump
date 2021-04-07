package utils

import (
	"fmt"
	"strconv"
	"strings"
)

func GetFileBaseDir(fullPath string, dirSep string) (string, string) {
	if dirSep == "" {
		dirSep = "/"
	}
	lastSlash := strings.LastIndex(fullPath, dirSep)
	if lastSlash == -1 {
		return "", fullPath
	}
	baseDir := fullPath[:lastSlash]
	if lastSlash == len(fullPath)-1 {
		return baseDir, ""
	}
	return baseDir, fullPath[lastSlash+1:]
}

func ToString(v interface{}) (val string, err error) {
	switch t := v.(type) {
	case int:
		val = strconv.Itoa(v.(int))
	case float32:
		val = strconv.FormatFloat(float64(v.(float32)), 'E', -1, 64)
	case float64:
		fval := v.(float64)
		if fval == float64(int64(fval)) {
			val = strconv.FormatInt(int64(fval), 10)
			break
		}
		val = strconv.FormatFloat(v.(float64), 'E', -1, 64)
	case int64:
		val = strconv.FormatInt(v.(int64), 10)
	case bool:
		val = strconv.FormatBool(v.(bool))
	case string:
		val = v.(string)
	default:
		val = ""
		err = fmt.Errorf("uknown datatype %v", t)
	}
	return val, err
}
