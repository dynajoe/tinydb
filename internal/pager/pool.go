package pager

type Pool struct {
	readPager  PageReader
	writePager Pager
}

func NewPool(p Pager) *Pool {
	return &Pool{
		readPager:  p,
		writePager: p,
	}
}

func (p *Pool) Acquire(id int, mode Mode) (Pager, error) {
	return p.writePager, nil
}

func (p *Pool) Release(id int) {}
