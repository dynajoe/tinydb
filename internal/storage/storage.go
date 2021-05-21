package storage

type Payload struct {
	Err    error
	Record *Record
}

type PageReader interface {
	PageSize() int
	TotalPages() int
	Read(page int) ([]byte, error)
}

type Page struct {
	PageNumber int
	Data       []byte
}

type PageWriter interface {
	Write(...Page) error
}
