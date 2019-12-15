package btree

import "sort"

// Item represents an item in the B-Tree
type Item interface {
	Less(than Item) bool
}

// BTree is a B-Tree
type BTree struct {
	degree int
	length int
	root   *node
}

type children []*node

type items []Item

type node struct {
	items    items
	children children
}

// New creates a B-Tree with the specified degree
func New(degree int) *BTree {
	return &BTree{
		degree: degree,
		length: 0,
	}
}

// Find locates the item in the B-Tree or returns nil if not found or the tree is empty
func (b *BTree) Find(item Item) Item {
	if b.root == nil {
		return nil
	}

	return b.root.find(item)
}

// Insert inserts an item into the B-Tree
func (b *BTree) Insert(item Item) Item {
	if b.root == nil {
		b.root = newNodeWithItem(item)
		b.length++
		return item
	}

	// Is the root node full?
	if len(b.root.items) >= b.maxItems() {
		// the root node needs to be split and children need to be added.
		splitItem, newChild := b.root.split(b.maxItems() / 2)
		oldRoot := b.root
		b.root = newNodeWithItem(splitItem)
		b.root.children = append(b.root.children, oldRoot, newChild)
	}

	replacedItem := b.root.insert(item, b.maxItems())

	if replacedItem == nil {
		b.length++
	}

	return replacedItem
}

func newNode() *node {
	return &node{
		items:    make([]Item, 0, 4),
		children: make([]*node, 0, 4),
	}
}

func (s items) find(item Item) (int, bool) {
	// item = 3
	// item = 2
	// s = [ 1, 2, 6, 9]

	// Find the smallest index for which the searched item is less than
	// i = 1
	// i = 0
	i := sort.Search(len(s), func(i int) bool {
		return item.Less(s[i])
	})

	// is this item equal to the one before it?
	// s[i - 1] = 1
	// false
	// i == 0
	if i > 0 && !s[i-1].Less(item) {
		return i - 1, true
	}

	return i, false
}

func (b *BTree) maxItems() int {
	return b.degree*2 - 1
}

func (b *BTree) Upsert(i Item, f func(old Item, new Item)) {
	x := b.Insert(i)

	if x != i {
		f(i, x)
	}
}

func (b *BTree) Len() int {
	return b.length
}

var (
	nilItems    = make(items, 16)
	nilChildren = make(children, 16)
)

func (s *children) truncate(index int) {
	var toClear children
	// Split items at the index into two
	*s, toClear = (*s)[:index], (*s)[index:]

	for len(toClear) > 0 {
		// Write zero values and update slice
		toClear = toClear[copy(toClear, nilChildren):]
	}
}

func (s *children) insertAt(index int, n *node) {
	if index >= len(*s)+1 {
		panic("item would not fit")
	}

	// create space for the new element
	*s = append(*s, nil)

	// shift everything down one
	copy((*s)[index+1:], (*s)[index:])

	// insert the item at the index
	(*s)[index] = n
}

func (s *items) truncate(index int) {
	var toClear items
	// Split items at the index into two
	*s, toClear = (*s)[:index], (*s)[index:]

	for len(toClear) > 0 {
		// Write zero values and update slice
		toClear = toClear[copy(toClear, nilItems):]
	}
}

func (s *items) insertAt(index int, item Item) {
	if index >= len(*s)+1 {
		panic("item would not fit")
	}

	// create space for the new element
	*s = append(*s, nil)

	// shift everything down one
	copy((*s)[index+1:], (*s)[index:])

	// insert the item at the index
	(*s)[index] = item
}

// the original node becomes the left
// and the new node becomes the right
// The item at i becomes the parent of the two nodes
func (n *node) split(i int) (Item, *node) {
	right := newNode()
	right.items = append(right.items, n.items[i+1:]...)
	parent := n.items[i]
	n.items.truncate(i)

	if len(n.children) > 0 {
		// All children after this node are greater and should be moved to the right
		right.children = append(right.children, n.children[i+1:]...)
		n.children.truncate(i + 1)
	}

	return parent, right
}

func newNodeWithItem(item Item) *node {
	node := newNode()
	node.items = append(node.items, item)
	return node
}

func (n *node) find(item Item) Item {
	i, found := n.items.find(item)

	if found {
		return n.items[i]
	}

	return n.children[i].find(item)
}

func (n *node) insert(item Item, maxItems int) Item {
	// Find what index this item goes
	i, found := n.items.find(item)

	// replace the item and return the old one
	if found {
		out := n.items[i]
		n.items[i] = item
		return out
	}

	// no children just insert the item at the position
	if len(n.children) == 0 {
		n.items.insertAt(i, item)
		return nil
	}

	childToInsert := n.children[i]

	// if children is maxed already, we need to split the children and promote one of the values
	if len(childToInsert.items) >= maxItems {
		// need to split the child
		promotedItem, newNode := childToInsert.split(maxItems / 2)
		n.items.insertAt(i, promotedItem)
		n.children.insertAt(i+1, newNode)

		// is the item equal to the one being promoted?
		if !promotedItem.Less(item) && !item.Less(promotedItem) {
			// replace the item and return the original
			n.items[i] = item
			return promotedItem
		} else if promotedItem.Less(item) {
			// Since we split the child which way does this item need to be inserted?
			return n.children[i+1].insert(item, maxItems)
		} else {
			return n.children[i].insert(item, maxItems)
		}
	} else {
		return n.children[i].insert(item, maxItems)
	}
}
