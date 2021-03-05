package storage

import (
	"io/ioutil"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPager_Read(t *testing.T) {

}

func TestPager_Write(t *testing.T) {
	assert := require.New(t)
	testDir, err := ioutil.TempDir("", "tinydb_test")
	assert.NoError(err)

	_, err = Open(path.Join(testDir, "db.tdb"), 1024)
	assert.NoError(err)
}
