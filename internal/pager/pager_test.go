package pager

import (
	"github.com/joeandaverde/tinydb/internal/storage"
	"github.com/stretchr/testify/suite"
	"testing"
)

const testPageSize = 4096

type PagerTestSuite struct {
	suite.Suite
	pager Pager
}

func (s *PagerTestSuite) SetupTest() {
	s.pager = NewPager(storage.NewMemoryFile(testPageSize))
}

func TestPagerTestSuite(t *testing.T) {
	suite.Run(t, &PagerTestSuite{})
}

func (s *PagerTestSuite) TestPager_Alloc() {
	p1, err := s.pager.Allocate(PageTypeLeaf)
	s.NoError(err)

	s.Equal(PageTypeLeaf, p1.header.Type)
	s.Equal(1, p1.pageNumber)
	s.Equal(0, int(p1.header.NumCells))
	s.Equal(true, p1.dirty)
}

func (s *PagerTestSuite) TestPager_Reset_NonPersistedPage() {
	allocPageOne, err := s.pager.Allocate(PageTypeLeaf)
	s.NoError(err)

	err = s.pager.Write(allocPageOne)
	s.NoError(err)

	allocPageOne.AddCell([]byte{0xB, 0xE, 0xE, 0xF})
	s.Equal(true, allocPageOne.dirty)

	s.pager.Reset()

	// page one should not exist unchanged
	_, err = s.pager.Read(1)
	s.Error(err)
}

func (s *PagerTestSuite) TestPager_Reset_PersistedPage() {
	allocPageOne, err := s.pager.Allocate(PageTypeLeaf)
	s.NoError(err)

	allocPageOne.AddCell([]byte{0xB, 0xE, 0xE, 0xF})
	s.Equal(true, allocPageOne.dirty)

	err = s.pager.Write(allocPageOne)
	s.NoError(err)

	expectedData := make([]byte, len(allocPageOne.data))
	copy(expectedData, allocPageOne.data)

	err = s.pager.Flush()
	s.NoError(err)

	allocPageOne.AddCell([]byte{0xD, 0xE, 0xA, 0xD})
	allocPageOne.AddCell([]byte{0xB, 0xE, 0xD, 0xA})

	s.Equal([]byte{0xB, 0xE, 0xE, 0xF}, allocPageOne.data[len(allocPageOne.data)-4:][:4])
	s.Equal([]byte{0xD, 0xE, 0xA, 0xD}, allocPageOne.data[len(allocPageOne.data)-8:][:4])
	s.Equal([]byte{0xB, 0xE, 0xD, 0xA}, allocPageOne.data[len(allocPageOne.data)-12:][:4])

	err = s.pager.Write(allocPageOne)
	s.NoError(err)

	s.pager.Reset()

	actualPageOne, err := s.pager.Read(1)
	s.NoError(err)

	s.Equal(expectedData, actualPageOne.data)
}

func blankMemPage(pageType PageType) *MemPage {
	p := &MemPage{
		header:     NewPageHeader(pageType, testPageSize),
		pageNumber: 2,
		data:       make([]byte, testPageSize),
	}
	return p
}
