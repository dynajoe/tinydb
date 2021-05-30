package storage

import "fmt"

type MemoryFile struct {
	pageSize int
	data     []byte
}

func NewMemoryFile(pageSize int) *MemoryFile {
	return &MemoryFile{pageSize: pageSize}
}

func (m *MemoryFile) PageSize() int {
	return m.pageSize
}

func (m *MemoryFile) TotalPages() int {
	return len(m.data) / m.pageSize
}

func (m *MemoryFile) Read(page int) ([]byte, error) {
	offset := (page - 1) * m.pageSize
	if offset+m.pageSize > len(m.data) {
		return nil, fmt.Errorf("page does not exist: %d", page)
	}
	return m.data[offset:][:m.pageSize], nil
}

func (m *MemoryFile) Write(pages ...Page) error {
	for _, p := range pages {
		offset := (p.PageNumber - 1) * m.pageSize
		// crudely expand memory linearly
		for offset >= len(m.data) {
			m.data = append(m.data, make([]byte, m.pageSize)...)
		}

		dest := m.data[offset:][:m.pageSize]
		copy(dest, p.Data[:m.pageSize])
	}
	return nil
}

var _ File = (*MemoryFile)(nil)
