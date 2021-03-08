package pager

import "sync"

type PagerPool struct {
	cond    *sync.Cond
	ownerID int
	pager   Pager
}

func NewPool(p Pager) *PagerPool {
	return &PagerPool{
		pager: p,
		cond:  sync.NewCond(&sync.Mutex{}),
	}
}

func (p *PagerPool) Acquire(id int, mode Mode) (Pager, error) {
	p.cond.L.Lock()

	// Already own the pager
	if p.ownerID == id {
		if mode == ModeWrite {
			p.pager.SetMode(mode)
		}
		p.cond.L.Unlock()
		return p.pager, nil
	}

	for p.ownerID != 0 {
		// Wait for owner to be 0
		p.cond.Wait()
	}

	p.ownerID = id
	p.cond.L.Unlock()
	p.pager.SetMode(mode)
	return p.pager, nil
}

func (p *PagerPool) Release(id int) {
	p.cond.L.Lock()
	if p.ownerID == id {
		p.ownerID = 0
		p.pager.SetMode(ModeRead)
		p.cond.L.Unlock()
		p.cond.Signal()
	}
}
