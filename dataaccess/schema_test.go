package dataaccess

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_toFileDef(t *testing.T) {
	// Arrange
	subj := setupDeezFileInfo("1")

	// Act
	fd := subj.toFileDef()

	// Assert
	assert := require.New(t)
	assert.Equal("1", fd.ID())
	assert.Equal("Index", fd.Kind())
	assert.Equal("Daily", fd.TimeFrame())
	assert.Equal("$SPX", fd.Symbol())
	assert.Equal("S&P 500", fd.Name())
	assert.Equal("S&P 500 Large Caps", fd.Desc())
	assert.Equal("/home/wl/sp500/sp500.xls", fd.Path())
	assert.True(fd.target.isSrc)
	assert.Equal("d-1", fd.Target().Dest())
	assert.Equal("tab", fd.Fmt().Delimiter())
	assert.Equal(2, fd.Fmt().Hdr())
	assert.Equal(6, fd.Fmt().Columns())
	assert.Equal("dd/mm/yyyy", fd.Fmt().Date())
	assert.Equal("hh:mm", fd.Fmt().Time())
	assert.Equal(2, fd.Fmt().DecimalDigits())
}

func Test_CreateSchema(t *testing.T) {
	// Arrange
	conn := new(FConnRDouble)

	// Act
	subj := CreateSchema(conn)

	// Assert
	assert := require.New(t)
	assert.Same(conn, subj.connection)
}

func Test_Load_Connection_Error(t *testing.T) {
	// Arrange
	conn := new(FConnRDouble)
	conn.connectionErr = true

	subj := CreateSchema(conn)

	// Act
	fds, err := subj.Load()

	// Assert
	assert := require.New(t)
	assert.NotNil(err)
	assert.Equal(0, len(fds.defs))
}

func Test_Load_Read_Error(t *testing.T) {
	// Arrange
	conn := new(FConnRDouble)
	conn.connectionErr = false
	conn.readErr = true

	subj := CreateSchema(conn)

	// Act
	fds, err := subj.Load()

	// Assert
	assert := require.New(t)
	assert.NotNil(err)
	assert.Contains(err.Error(), "!! no read 4 u")
	assert.Equal(0, len(fds.defs))
}

func Test_Load_JSON_Error(t *testing.T) {
	// Arrange
	badJSON := strings.NewReader(`[{"Id": "3", "x": 3 "y": 3}]`)
	conn := &FConnRDouble{
		connectionErr: false,
		readErr:       false,
		reader:        badJSON,
	}

	subj := CreateSchema(conn)

	// Act
	fds, err := subj.Load()

	// Assert
	assert := require.New(t)
	assert.NotNil(err)
	assert.Contains(strings.ToLower(err.Error()), "invalid char")
	assert.Equal(0, len(fds.defs))
}

func Test_Load(t *testing.T) {
	// Arrange
	r := strings.NewReader(`[
		{
			"Id": "1",
			"Kind": "Index",
			"TimeFrame": "30min",
			"Symbol": "TA35",
			"Name": "TA35-30min",
			"Description": "TA35 Index, intraday: 30nin",
			"Path": "e:/wl/tase/ta35_30.xls",
			"Target":
			{
				"Src": true,
				"Dest": "100"
			},
			"Format":
			{
				"Delimiter": "comma",
				"Header": 2,
				"Columns": 7,
				"Date": "dd/mm/yyyy",
				"Time": "hh:mm",
				"DecimalCount": 2
			}
		}
	]`)
	conn := &FConnRDouble{
		connectionErr: false,
		readErr:       false,
		reader:        r,
	}

	subj := CreateSchema(conn)

	// Act
	fds, err := subj.Load()

	// Assert
	assert := require.New(t)
	assert.Nil(err)
	assert.Equal(1, len(fds.defs))
	assert.True(fds.Contains("1"))
	fd := fds.defs["1"]
	assert.Equal("1", fd.ID())
	assert.Equal("Index", fd.Kind())
	assert.Equal("30min", fd.TimeFrame())
	assert.Equal("TA35", fd.Symbol())
	assert.Equal("TA35-30min", fd.Name())
	assert.Equal("TA35 Index, intraday: 30nin", fd.Desc())
	assert.Equal("e:/wl/tase/ta35_30.xls", fd.Path())
	assert.True(fd.target.IsSrc())
	assert.Equal("100", fd.Target().Dest())
	assert.Equal("comma", fd.Fmt().Delimiter())
	assert.Equal(2, fd.Fmt().Hdr())
	assert.Equal(7, fd.Fmt().Columns())
	assert.Equal("dd/mm/yyyy", fd.Fmt().Date())
	assert.Equal("hh:mm", fd.Fmt().Time())
	assert.Equal(2, fd.Fmt().DecimalDigits())
}

func Test_Load_ID_Duplicated(t *testing.T) {
	// Arrange
	r := strings.NewReader(`[
		{
			"Id": "1",
			"Kind": "Index",
			"TimeFrame": "30min",
			"Symbol": "TA35",
			"Name": "TA35-30min",
			"Description": "TA35 Index, intraday: 30nin",
			"Path": "e:/wl/tase/ta35_30.xls",
			"Target":
			{
				"Src": true,
				"Dest": "100"
			},
			"Format":
			{
				"Delimiter": "comma",
				"Header": 2,
				"Columns": 7,
				"Date": "dd/mm/yyyy",
				"Time": "hh:mm",
				"DecimalCount": 2
			}
		},
		{
			"Id": "1",
			"Kind": "Index",
			"TimeFrame": "30min",
			"Symbol": "TA35",
			"Name": "TA35-30min",
			"Description": "TA35 Index, intraday: 30nin",
			"Path": "e:/wl/tase/ta35_30.xls",
			"Target":
			{
				"Src": true,
				"Dest": "100"
			},
			"Format":
			{
				"Delimiter": "comma",
				"Header": 2,
				"Columns": 7,
				"Date": "dd/mm/yyyy",
				"Time": "hh:mm",
				"DecimalCount": 2
			}
		}
	]`)
	conn := &FConnRDouble{
		connectionErr: false,
		readErr:       false,
		reader:        r,
	}

	subj := CreateSchema(conn)

	// Act
	fds, err := subj.Load()

	// Assert
	assert := require.New(t)
	assert.NotNil(err)
	assert.Contains(err.Error(), "duplicate file definition")
	assert.Equal(0, len(fds.defs))
}

func Test_Load_Multiple_Defs(t *testing.T) {
	// Arrange
	r := strings.NewReader(`[
		{
			"Id": "1",
			"Kind": "Index",
			"TimeFrame": "30min",
			"Symbol": "TA35",
			"Name": "TA35-30min",
			"Description": "TA35 Index, intraday: 30nin",
			"Path": "e:/wl/tase/ta35_30.xls",
			"Target":
			{
				"Src": true,
				"Dest": "100"
			},
			"Format":
			{
				"Delimiter": "comma",
				"Header": 2,
				"Columns": 7,
				"Date": "dd/mm/yyyy",
				"Time": "hh:mm",
				"DecimalCount": 2
			}
		},
		{
			"Id": "100",
			"Kind": "Index",
			"TimeFrame": "30min",
			"Symbol": "TA35",
			"Name": "TA35-30min",
			"Description": "TA35 Index, intraday: 30nin",
			"Path": "e:/wl/tase/ta35_30_ingr.xls",
			"Target":
			{
				"Src": false,
				"Dest": ""
			},
			"Format":
			{
				"Delimiter": "comma",
				"Header": 2,
				"Columns": 7,
				"Date": "dd/mm/yyyy",
				"Time": "hh:mm",
				"DecimalCount": 2
			}
		}
	]`)
	conn := &FConnRDouble{
		connectionErr: false,
		readErr:       false,
		reader:        r,
	}

	subj := CreateSchema(conn)

	// Act
	fds, err := subj.Load()

	// Assert
	assert := require.New(t)
	assert.Nil(err)
	assert.Equal(2, len(fds.defs))
	assert.True(fds.Contains("1"))
	assert.True(fds.Contains("100"))

	srcFd, _ := fds.Get("1")
	assert.True(srcFd.Target().IsSrc())
	assert.Equal("100", srcFd.target.Dest())

	trgFd, _ := fds.Get("100")
	assert.False(trgFd.Target().IsSrc())
	assert.Equal("", trgFd.Target().Dest())
}

func setupDeezFileInfo(id string) deezFileInfo {
	return deezFileInfo{
		ID:        id,
		Kind:      "Index",
		TimeFrame: "Daily",
		Symbol:    "$SPX",
		Name:      "S&P 500",
		Desc:      "S&P 500 Large Caps",
		Path:      "/home/wl/sp500/sp500.xls",
		Target: deezTarg{
			Src:  true,
			Dest: fmt.Sprintf("d-%s", id),
		},
		Fmt: deezFmt{
			Delimiter:     "tab",
			Header:        2,
			Columns:       6,
			Date:          "dd/mm/yyyy",
			Time:          "hh:mm",
			DecimalDigits: 2,
		},
	}
}

type FConnRDouble struct {
	connectionErr bool
	readErr       bool
	reader        io.Reader
}

func (dbl *FConnRDouble) ConnStr() string {
	return ""
}

func (dbl *FConnRDouble) Opened() bool {
	return false
}

func (dbl *FConnRDouble) Close() error {
	return nil
}

func (dbl *FConnRDouble) Open() (io.Reader, error) {
	if dbl.connectionErr {
		return nil, errors.New("Open file error")
	} else if dbl.readErr {
		return badReader{}, nil
	}

	return dbl.reader, nil
}

type badReader struct {
	e error
}

func (bad badReader) Read(b []byte) (n int, err error) {
	bad.e = errors.New("!! no read 4 u")
	return 0, bad.e
}
