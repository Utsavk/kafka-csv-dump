package parser

import (
	"encoding/json"

	"github.com/kafka-csv-dump/utils"
)

func Json(data []byte, fields []string) []string {
	var dump = make(map[string]interface{})
	if err := json.Unmarshal(data, &dump); err != nil {
		return nil
	}
	var row = make([]string, len(fields))
	var i = 0
	for _, f := range fields {
		if val, ok := dump[f]; ok {
			row[i], _ = utils.ToString(val)
		} else {
			row[i] = ""
		}
		i++
	}
	return row
}
