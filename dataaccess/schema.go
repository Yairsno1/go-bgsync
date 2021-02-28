// Package dataaccess is the data access layer for all of the
// data files involved in the sync process
package dataaccess

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

// SchemaLoader defines schema loading interface
type SchemaLoader interface {
	Load() (FileDefs, error)
}

type deezTarg struct {
	Src  bool   `json:"Src"`
	Dest string `json:"Dest"`
}

type deezFmt struct {
	Delimiter     string `json:"Delimiter"`
	Header        int    `json:"Header"`
	Columns       int    `json:"Columns"`
	Date          string `json:"Date"`
	Time          string `json:"Time"`
	DecimalDigits int    `json:"DecimalCount"`
}

type deezFileInfo struct {
	ID        string   `json:"Id"`
	Kind      string   `json:"Kind"`
	TimeFrame string   `json:"TimeFrame"`
	Symbol    string   `json:"Symbol"`
	Name      string   `json:"Name"`
	Desc      string   `json:"Description"`
	Path      string   `json:"Path"`
	Target    deezTarg `json:"Target"`
	Fmt       deezFmt  `json:"Format"`
}

func (fi deezFileInfo) toFileDef() FileDef {
	rv := FileDef{
		id:        fi.ID,
		kind:      fi.Kind,
		timeFrame: fi.TimeFrame,
		symbol:    fi.Symbol,
		name:      fi.Name,
		desc:      fi.Desc,
		path:      fi.Path,
		target: TargetDef{
			isSrc: fi.Target.Src,
			dest:  fi.Target.Dest,
		},
		format: FmtDef{
			delimiter:     fi.Fmt.Delimiter,
			header:        fi.Fmt.Header,
			columns:       fi.Fmt.Columns,
			date:          fi.Fmt.Date,
			time:          fi.Fmt.Time,
			decimalDigits: fi.Fmt.DecimalDigits,
		},
	}

	return rv
}

// Schema represents metadata for the data store
type Schema struct {
	connection FConnR
}

// CreateSchema initalizes new schema instance
func CreateSchema(conn FConnR) Schema {
	return Schema{
		connection: conn,
	}
}

// Load reads the data store's metadata
func (schm Schema) Load() (FileDefs, error) {
	rv := createFileDefs()

	r, err := schm.connection.Open()
	if err != nil {
		return rv, fmt.Errorf("Schema load, open connection error:\n%v", err)
	}
	defer schm.connection.Close()

	data, err := ioutil.ReadAll(r)
	if err != nil {
		return rv, fmt.Errorf("Schema load, read error:\n%v", err)
	}

	var fis []deezFileInfo
	jsonErr := json.Unmarshal(data, &fis)
	if jsonErr != nil {
		return rv, fmt.Errorf("Schema load, data format error:\n%v", jsonErr)
	}

	for _, fi := range fis {
		if rv.Contains(fi.ID) {
			rv = createFileDefs()
			return rv, fmt.Errorf("Schema load, duplicate file definition id [%s]", fi.ID)
		}

		rv.add(fi.toFileDef())
	}

	return rv, nil
}
