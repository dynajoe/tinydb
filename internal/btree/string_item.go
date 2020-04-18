package btree

type StringItem struct {
	Data interface{}
	Key  string
}

func (i *StringItem) Less(than Item) bool {
	return i.Key < than.(*StringItem).Key
}
