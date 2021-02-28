package dataaccess

import (
	"errors"
	"fmt"
	"io"
	"os"
)

// FConn exposes the base definitions for file connection.
type FConn interface {
	ConnStr() string
	Opened() bool
	io.Closer
}

type fileConnection struct {
	f                *os.File
	connectionString string
	opened           bool
}

func (fConn *fileConnection) Close() error {
	if !fConn.opened {
		return nil
	}

	return fConn.f.Close()
}

// FConnR defines opening a file for read.
type FConnR interface {
	FConn
	Open() (io.Reader, error)
}

// FReadConn representes a connection to a file, intent for reading from the file
type FReadConn struct {
	conn *fileConnection
}

// CreateFReadConn initializes new FReadConn value
// Value of the param connStr must not be empty; otherwise, error.
func CreateFReadConn(connStr string) (*FReadConn, error) {
	if connStr == "" {
		return nil, errors.New("connection string cannot be empty")
	}

	fc := &fileConnection{
		f:                nil,
		connectionString: connStr,
		opened:           false,
	}

	return &FReadConn{
			conn: fc,
		},
		nil
}

// ConnStr gets the string used to open a file.
func (frConn *FReadConn) ConnStr() string {
	return frConn.conn.connectionString
}

// Opened gets if the file was opened.
func (frConn *FReadConn) Opened() bool {
	return frConn.conn.opened
}

// Close closes an opened file
func (frConn *FReadConn) Close() error {
	return frConn.conn.Close()
}

// Open opens a file for read
func (frConn *FReadConn) Open() (io.Reader, error) {
	if !frConn.conn.opened {
		f, err := os.Open(frConn.conn.connectionString)
		if err != nil {
			return nil, fmt.Errorf("%v", err)
		}
		frConn.conn.f = f
		frConn.conn.opened = true
	}

	return frConn.conn.f, nil
}
