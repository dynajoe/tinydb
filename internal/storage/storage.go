package storage

var readTimeoutSeconds int = -1

type Payload struct {
	Err    error
	Record *Record
}
