package storage

// NewPageHeader creates a new PageHeader
func NewPageHeader(pageType PageType, pageSize uint16) PageHeader {
	return PageHeader{
		Type:                pageType,
		CellsOffset:         pageSize,
		FreeBlock:           0,
		NumCells:            0,
		FragmentedFreeBytes: 0,
		RightPage:           0,
	}
}

// PageType type of page. See associated enumeration values.
type PageType byte

const (
	// PageTypeInternal internal table page
	PageTypeInternal PageType = 0x05

	// PageTypeLeaf leaf table page
	PageTypeLeaf PageType = 0x0D

	// PageTypeInternalIndex internal index page
	PageTypeInternalIndex PageType = 0x02

	// PageTypeLeafIndex leaf index page
	PageTypeLeafIndex PageType = 0x0A
)

// PageHeader contains metadata about the page
// BTree Page
// The 100-byte database file header (found on page 1 only)
// The 8 or 12 byte b-tree page header
// The cell pointer array
// Unallocated space
// The cell content area
// The reserved region.
//      The size of the reserved region is determined by the
//      one-byte unsigned integer found at an offset of 20 into
//      the database file header. The size of the reserved region is usually zero.
// Example First page header
// 0D (00 00) (00 01) (0F 8A) (00)
type PageHeader struct {
	// Type is the PageType for the page
	Type PageType

	// FreeBlock The two-byte integer at offset 1 gives the start of the first freeblock on the page, or is zero if there are no freeblocks.
	// A freeblock is a structure used to identify unallocated space within a b-tree page.
	// Freeblocks are organized as a chain. The first 2 bytes of a freeblock are a big-endian integer which is the offset in the b-tree page of the next freeblock in the chain, or zero if the freeblock is the last on the chain.
	FreeBlock uint16

	// NumCells is the number of cells stored in this page.
	NumCells uint16

	// CellsOffset the start of the cell content area. A zero value for this integer is interpreted as 65536.
	// If the page contains no cells, this field contains the value PageSize.
	CellsOffset uint16

	// FragmentedFreeBytes the number of fragmented free bytes within the cell content area.
	FragmentedFreeBytes byte

	// RightPage internal nodes only
	RightPage int
}
