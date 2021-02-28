package dataaccess

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_ctor(t *testing.T) {
	fdefs := createFileDefs()

	assert.Equal(t, 0, len(fdefs.defs))
}

func Test_add(t *testing.T) {
	// Arrange
	subj := createFileDefs()
	item := setupFileDef("1", false)

	// Act
	subj.add(item)

	// Assert
	assert.True(t, subj.Contains("1"))
}

func Test_add_Key_Exists(t *testing.T) {
	// Arrange
	subj := createFileDefs()
	item := setupFileDef("1", false)

	// Act
	subj.add(item)
	subj.add(item)

	// Assert
	assert := assert.New(t)
	assert.True(subj.Contains("1"))
	assert.Equal(1, len(subj.defs))
}

func Test_Get_Key_not_Exists(t *testing.T) {
	// Arrange
	subj := createFileDefs()

	// Act
	_, ok := subj.Get("1")

	// Assert
	assert := assert.New(t)
	assert.False(ok)
}

func Test_Get(t *testing.T) {
	// Arrange
	subj := createFileDefs()
	item := setupFileDef("1", false)
	subj.add(item)

	// Act
	fd, ok := subj.Get("1")

	// Assert
	assert := assert.New(t)
	assert.True(ok)
	assert.Equal("1", fd.ID())
}

func Test_SrcDefs(t *testing.T) {
	// Arrange
	subj := createFileDefs()
	items := []FileDef{setupFileDef("1", false), setupFileDef("2", true), setupFileDef("3", true)}
	for _, item := range items {
		subj.add(item)
	}

	// Act
	sources := subj.SrcDefs()

	// Assert
	assert := assert.New(t)
	assert.Equal(2, len(sources))
}

func setupFileDef(ID string, src bool) FileDef {
	rv := FileDef{
		id: ID,
		target: TargetDef{
			isSrc: src,
		},
	}

	return rv
}
