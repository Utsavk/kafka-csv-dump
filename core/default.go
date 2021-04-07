package core

import (
	"fmt"

	"github.com/kafka-csv-dump/config"
	"github.com/kafka-csv-dump/parser"
	"github.com/kataras/golog"
)

type SchemaType int

var (
	json SchemaType = 1
	avro SchemaType = 2
)

//file_name
type Handler struct {
	Schema         SchemaType
	FilteredFields []string
	Fn             parserFn
}

type parserFn func([]byte, []string) []string

func ProcessData(partition int, topicConf config.Topic, data []byte) {
	//do not create h object everytime
	h, err := setHandler(topicConf.Schema, topicConf.FilteredFields)
	if err != nil {
		golog.Error(err)
		return
	}
	row := h.Fn(data, h.FilteredFields)
	if row == nil {
		return
	}
	golog.Info(row)
	//push to csv
}

func setHandler(schema string, filter []string) (*Handler, error) {
	var err error
	var h = &Handler{FilteredFields: filter}
	if schema == "json" {
		h.Schema = json
		h.Fn = parser.Json
	} else if schema == "avro" {
		h.Schema = avro
		err = fmt.Errorf("avro schema not yet implemented")
	} else {
		err = fmt.Errorf("invalid schema %s", schema)
	}
	if err != nil {
		return nil, err
	}
	return h, nil
}
