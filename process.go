package main

import (
	"github.com/ziutek/mymysql/mysql"
	_ "github.com/ziutek/mymysql/native"
)

var ProcessListToSend = []string{
	"ID",
	"USER",
	"HOST",
	"DB",
	"COMMAND",
	"TIME",
	"STATE",
	"INFO",
}

type ProcessList struct {
	Endpoint  string      `json:"server"` //hostname
	Value     interface{} `json:"processlist"`
	Timestamp int64       `json:"timeAt"`
}

func processList(m *MysqlIns, db mysql.Conn) ([]*MetaData, error) {
	process := NewMetric("CustomData_Process_List", m)
	rows, res, err := db.Query("SELECT * FROM information_schema.`PROCESSLIST`")
	if err != nil {
		return nil, err
	}
	plists := []map[string]string{}
	data := make([]*MetaData, 0, 1)

	for _, row := range rows {
		plist := map[string]string{}
		for _, s := range ProcessListToSend {
			plist[s] = row.Str(res.Map(s))
		}
		plists = append(plists[:], plist)
	}
	process.SetValue(plists)
	return append(data[:], process), nil
}
