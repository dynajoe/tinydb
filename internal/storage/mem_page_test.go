package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func blankMemPage(pageType PageType) *MemPage {
	p := &MemPage{
		header:     NewPageHeader(pageType, 4096),
		pageNumber: 2,
		data:       make([]byte, 4096),
	}
	return p
}

func TestMemPage_AddCell_InteriorNode(t *testing.T) {
	assert := require.New(t)
	page := blankMemPage(PageTypeInternal)

	cell := InteriorNode{
		LeftChild: uint32(2),
		Key:       999, // varint 0x87 0x67
	}
	cellBytes, err := cell.ToBytes()
	assert.NoError(err)
	page.AddCell(cellBytes)
	assert.Equal([]byte{0x5, 0x0, 0x0, 0x0, 0x1}, page.data[:5])
	assert.Equal([]byte{0xf, 0xfa, 0x0}, page.data[5:8])

	startOffset := page.header.CellsOffset - uint16(len(cellBytes))
	startCellCount := page.header.NumCells + 1
	for i := 0; i < 10; i++ {
		page.AddCell(cellBytes)
		assert.Equal(startCellCount+uint16(i), page.header.NumCells)
		assert.Equal(startOffset-uint16(len(cellBytes)*i), page.header.CellsOffset)
		assert.Equal(cellBytes, page.data[page.header.CellsOffset:int(page.header.CellsOffset)+len(cellBytes)])
	}
}
