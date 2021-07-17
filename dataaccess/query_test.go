package dataaccess

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_Query_ctor(t *testing.T) {
	// Arrange + Act
	q := new(Query)

	// Assert
	assert.Equal(t, true, q.IsLast())
}

func Test_Query_GT(t *testing.T) {
	// Arrange
	q := new(Query)

	// Act
	q.GT(2020, time.February, 20)

	// Assert
	assert.Equal(t, false, q.IsLast())
	assert.Equal(t, 2020, q.Year())
	assert.Equal(t, time.February, q.Month())
	assert.Equal(t, 20, q.Day())
}

func Test_Query_Last(t *testing.T) {
	// Arrange
	q := new(Query)
	q.GT(2020, time.February, 20)
	assert.Equal(t, false, q.IsLast())

	// Act
	q.Last()

	// Assert
	assert.Equal(t, true, q.IsLast())
}
