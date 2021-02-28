package dataaccess

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_CreateFReadConn_ConnStr_is_Empty(t *testing.T) {
	// Arrange + Act
	subj, err := CreateFReadConn("")

	// Assert
	assert := require.New(t)
	assert.NotNil(err)
	assert.Contains(err.Error(), "cannot be empty")
	assert.Nil(subj)
}

func Test_CreateFReadConn(t *testing.T) {
	// Arrange + Act
	subj, err := CreateFReadConn("/home/usr/file.txt")

	// Assert
	assert := require.New(t)
	assert.Nil(err)
	assert.NotNil(subj)
	assert.Equal("/home/usr/file.txt", subj.ConnStr())
	assert.False(subj.Opened())
	assert.Nil(subj.conn.f)
}
