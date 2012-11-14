package storage

import ()

type NeedleValue struct {
	Key    Key
	Offset uint32 "Volume offset" //since aligned to 8 bytes, range is 4G*8=32G
	Size   uint32 "Size of the data portion"
}

const (
	batch = 100000
)

type Key uint64

type CompactSection struct {
	values   []NeedleValue
	overflow map[Key]*NeedleValue
	start    Key
	end      Key
	counter  int
}

func NewCompactSection(start Key) CompactSection {
	return CompactSection{
		values:   make([]NeedleValue, batch),
		overflow: make(map[Key]*NeedleValue),
		start:    start,
	}
}
func (cs *CompactSection) Set(key Key, offset uint32, size uint32) {
	if key > cs.end {
		cs.end = key
	}
	if i := cs.binarySearchValues(key); i >= 0 {
		cs.values[i].Offset, cs.values[i].Size = offset, size
	} else {
		needOverflow := cs.counter >= batch
		needOverflow = needOverflow || cs.counter > 0 && cs.values[cs.counter-1].Key > key
		if needOverflow {
			//println("start", cs.start, "counter", cs.counter, "key", key)
			cs.overflow[key] = &NeedleValue{Key: key, Offset: offset, Size: size}
		} else {
			p := &cs.values[cs.counter]
			p.Key, p.Offset, p.Size = key, offset, size
			//println("added index", cs.counter, "key", key, cs.values[cs.counter].Key)
			cs.counter++
		}
	}
}
func (cs *CompactSection) Delete(key Key) {
	if i := cs.binarySearchValues(key); i >= 0 {
		cs.values[i].Size = 0
	}
	delete(cs.overflow, key)
}
func (cs *CompactSection) Get(key Key) (*NeedleValue, bool) {
	if v, ok := cs.overflow[key]; ok {
		return v, true
	}
	if i := cs.binarySearchValues(key); i >= 0 {
		return &cs.values[i], true
	}
	return nil, false
}
func (cs *CompactSection) binarySearchValues(key Key) int {
	l, h := 0, cs.counter-1
	if h >= 0 && cs.values[h].Key < key {
		return -2
	}
	//println("looking for key", key)
	for l <= h {
		m := (l + h) / 2
		//println("mid", m, "key", cs.values[m].Key, cs.values[m].Offset, cs.values[m].Size)
		if cs.values[m].Key < key {
			l = m + 1
		} else if key < cs.values[m].Key {
			h = m - 1
		} else {
			//println("found", m)
			return m
		}
	}
	return -1
}

//This map assumes mostly inserting increasing keys
type CompactMap struct {
	list []CompactSection
}

func NewCompactMap() CompactMap {
	return CompactMap{}
}

func (cm *CompactMap) Set(key Key, offset uint32, size uint32) {
	x := cm.binarySearchCompactSection(key)
	if x < 0 {
		//println(x, "creating", len(cm.list), "section1, starting", key)
		cm.list = append(cm.list, NewCompactSection(key))
		x = len(cm.list) - 1
	}
	cm.list[x].Set(key, offset, size)
}
func (cm *CompactMap) Delete(key Key) {
	x := cm.binarySearchCompactSection(key)
	if x < 0 {
		return
	}
	cm.list[x].Delete(key)
}
func (cm *CompactMap) Get(key Key) (*NeedleValue, bool) {
	x := cm.binarySearchCompactSection(key)
	if x < 0 {
		return nil, false
	}
	return cm.list[x].Get(key)
}
func (cm *CompactMap) binarySearchCompactSection(key Key) int {
	l, h := 0, len(cm.list)-1
	if h < 0 {
		return -5
	}
	if cm.list[h].start <= key {
		if cm.list[h].counter < batch || key <= cm.list[h].end {
			return h
		} else {
			return -4
		}
	}
	for l <= h {
		m := (l + h) / 2
		if key < cm.list[m].start {
			h = m - 1
		} else { // cm.list[m].start <= key
			if cm.list[m+1].start <= key {
				l = m + 1
			} else {
				return m
			}
		}
	}
	return -3
}

func (cm *CompactMap) Peek() {
	for k, v := range cm.list[0].values {
		if k < 100 {
			println("[", v.Key, v.Offset, v.Size, "]")
		}
	}
	for k, v := range cm.list[0].overflow {
		if k < 100 {
			println("o[", v.Key, v.Offset, v.Size, "]")
		}
	}
}
