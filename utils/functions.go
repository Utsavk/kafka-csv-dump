package utils

import "strings"

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
