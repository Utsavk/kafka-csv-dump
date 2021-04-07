package parser

import "github.com/kataras/golog"

func Json(data []byte) []string {
	golog.Info(string(data), "===")
	return nil
}
