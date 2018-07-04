/*
This is free and unencumbered software released into the public domain.

Anyone is free to copy, modify, publish, use, compile, sell, or
distribute this software, either in source code form or as a compiled
binary, for any purpose, commercial or non-commercial, and by any
means.

In jurisdictions that recognize copyright laws, the author or authors
of this software dedicate any and all copyright interest in the
software to the public domain. We make this dedication for the benefit
of the public at large and to the detriment of our heirs and
successors. We intend this dedication to be an overt act of
relinquishment in perpetuity of all present and future rights to this
software under copyright law.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.

For more information, please refer to <http://unlicense.org/>
*/


package hucache

import "container/list"
import "github.com/couchbase/go-slab"
import "fmt"
import "bytes"

var ENotFound = fmt.Errorf("Error: Not Found")
var EAllocation = fmt.Errorf("Error: Allocation")

/* This is all methods, that our cache-implementation needs. Based upon freecache's interface. */

type Cache interface{
	GetInt(key int64) (value []byte, err error)
	SetInt(key int64, value []byte, expireSeconds int) (err error)
	DelInt(key int64) (affected bool)
}

/*
[ recent-ghosts <-[ recent <-!-> frequent ]-> frequent-ghost ]
*/
type area uint
const (
	recent_ghost area = iota
	recent
	frequent
	frequent_ghost
	/* Not an area. */
	num_areas
)

type entry struct {
	key int64
	area area
	buffer []byte
}

func Test(a *slab.Arena) *cachelist {
	cl := new(cachelist)
	cl.init(5,5)
	cl.arena = a
	return cl
}

/*
[ .... recent-ghost ...,GR,... recent ...,M,... frequent ...,GF,... frequent-gost .... ]
*/
type cachelist struct {
	lst     *list.List
	gr,m,gf *list.Element
	index   map[int64] *list.Element
	arena   *slab.Arena
	count   [num_areas]int64
	target  [num_areas]int64
}
func (c *cachelist) init(cache int64, ghost int64) {
	c.lst = list.New()
	c.gr = c.lst.PushBack(nil)
	c.m  = c.lst.PushBack(nil)
	c.gf = c.lst.PushBack(nil)
	c.index = make(map[int64] *list.Element)
	
	c.target[recent  ] = cache
	c.target[frequent] = cache
	c.target[recent_ghost  ] = ghost
	c.target[frequent_ghost] = ghost
}
func (c *cachelist) clearEntry(e *entry, a area) {
	e.area = a
	if len(e.buffer) == 0 { return }
	c.arena.DecRef(e.buffer)
	e.buffer = nil
}
func (c *cachelist) evict() {
	for c.count[recent]>c.target[recent] {
		e := c.gr.Next()
		c.lst.MoveBefore(e,c.gr)
		c.clearEntry(e.Value.(*entry),recent_ghost)
		c.count[recent]--
		c.count[recent_ghost]++
	}
	for c.count[frequent]>c.target[frequent] {
		e := c.gf.Prev()
		c.lst.MoveAfter(e,c.gf)
		c.clearEntry(e.Value.(*entry),frequent_ghost)
		c.count[frequent]--
		c.count[frequent_ghost]++
	}
	for c.count[recent_ghost]>c.target[recent_ghost] {
		e := c.lst.Front()
		c.lst.Remove(e)
		delete(c.index,e.Value.(*entry).key)
		c.count[recent_ghost]--
	}
	for c.count[frequent_ghost]>c.target[frequent_ghost] {
		e := c.lst.Back()
		c.lst.Remove(e)
		delete(c.index,e.Value.(*entry).key)
		c.count[frequent_ghost]--
	}
}
func (c *cachelist) migrate(e1 *list.Element, e2 *entry, a area) {
	c.count[e2.area]--
	c.count[a]++
	e2.area = a
	switch a {
	case recent_ghost:
		c.lst.MoveBefore(e1,c.gr)
	case recent:
		c.lst.MoveBefore(e1,c.m)
	case frequent:
		c.lst.MoveAfter(e1,c.m)
	case frequent_ghost:
		c.lst.MoveAfter(e1,c.gf)
	}
	c.evict()
}
func (c *cachelist) insert(e2 *entry, a area) {
	c.count[a]++
	e2.area = a
	switch a {
	case recent_ghost:
		c.index[e2.key] = c.lst.InsertBefore(e2,c.gr)
	case recent:
		c.index[e2.key] = c.lst.InsertBefore(e2,c.m)
	case frequent:
		c.index[e2.key] = c.lst.InsertAfter(e2,c.m)
	case frequent_ghost:
		c.index[e2.key] = c.lst.InsertAfter(e2,c.gf)
	}
	c.evict()
}
func (c *cachelist) get(i int64) ([]byte,error) {
	if elem,ok := c.index[i]; ok {
		e := elem.Value.(*entry)
		switch e.area {
		case recent:
			c.migrate(elem,e,frequent)
			return e.buffer,nil
		case frequent:
			c.lst.MoveAfter(elem,c.m) /* Move-To-Front */
			return e.buffer,nil
		}
	}
	return nil,ENotFound
}
func (c *cachelist) set(i int64,v []byte) error {
	if elem,ok := c.index[i]; ok {
		e := elem.Value.(*entry)
		switch e.area {
		case recent,frequent:
			if len(e.buffer)>0 {
				c.arena.DecRef(e.buffer)
			}
			if len(v)>0 {
				e.buffer = c.arena.Alloc(len(v))
			}
			if len(e.buffer)<len(v) {
				c.lst.Remove(elem)
				c.count[e.area]--
				delete(c.index,i)
				return EAllocation
			}
			copy(e.buffer,v)
			return nil
		case recent_ghost:
			if len(v)>0 {
				e.buffer = c.arena.Alloc(len(v))
			}
			if len(e.buffer)<len(v) {
				return EAllocation
			}
			copy(e.buffer,v)
			if c.target[frequent]>0 {
				c.target[frequent]--
				c.target[recent]++
			}
			c.migrate(elem,e,recent)
			return nil
		case frequent_ghost:
			if len(v)>0 {
				e.buffer = c.arena.Alloc(len(v))
			}
			if len(e.buffer)<len(v) {
				return EAllocation
			}
			copy(e.buffer,v)
			if c.target[recent]>0 {
				c.target[recent]--
				c.target[frequent]++
			}
			c.migrate(elem,e,frequent)
			return nil
		}
	}
	
	e := &entry{ key: i }
	if len(v)>0 {
		e.buffer = c.arena.Alloc(len(v))
	}
	if len(e.buffer)<len(v) {
		return EAllocation
	}
	copy(e.buffer,v)
	c.insert(e,recent)
	return nil
}
func (c *cachelist) del(i int64) bool {
	elem,ok := c.index[i]
	if !ok { return false }
	c.count[elem.Value.(*entry).area]--
	c.lst.Remove(elem)
	delete(c.index,i)
	return true
}

func (c *cachelist) String() string {
	buf := new(bytes.Buffer)
	buf.WriteString("[")
	
	for e := c.lst.Front(); e!=nil && e!=c.gr; e = e.Next() {
		ee := e.Value.(*entry)
		fmt.Fprintf(buf,"%d (%d),",ee.key,ee.area)
	}
	
	buf.WriteString("GR,")
	
	for e := c.gr.Next(); e!=nil && e!=c.m; e = e.Next() {
		ee := e.Value.(*entry)
		fmt.Fprintf(buf,"%d (%d),",ee.key,ee.area)
	}
	
	buf.WriteString("M,")
	
	for e := c.m.Next(); e!=nil && e!=c.gf; e = e.Next() {
		ee := e.Value.(*entry)
		fmt.Fprintf(buf,"%d (%d),",ee.key,ee.area)
	}
	
	buf.WriteString("GF,")
	
	for e := c.gf.Next(); e!=nil ; e = e.Next() {
		ee := e.Value.(*entry)
		fmt.Fprintf(buf,"%d (%d),",ee.key,ee.area)
	}
	
	buf.WriteString("]")
	fmt.Fprintf(buf,"(%v %v)",c.count,c.target)
	return buf.String()
}
func (c *cachelist) GetInt(key int64) (value []byte, err error) { return c.get(key) }
func (c *cachelist) SetInt(key int64, value []byte, expireSeconds int) (err error) { return c.set(key,value) }
func (c *cachelist) DelInt(key int64) (affected bool) { return c.del(key) }

/* Creates a cache, that roughly resembles an Adaptive Replacement Cache. */
func NewARC(a *slab.Arena,cache, ghost int) Cache {
	cl := new(cachelist)
	cl.init(int64(cache),int64(ghost))
	cl.arena = a
	return cl
}

type splitcache struct{
	little,big Cache
	splitsize int
}
func Split(little,big Cache,splitsize int) Cache {
	return &splitcache{little,big,splitsize}
}
func (s *splitcache) GetInt(key int64) (value []byte, err error) {
	value,err = s.little.GetInt(key)
	if err!=nil {
		value,err = s.big.GetInt(key)
	}
	return
}
func (s *splitcache) SetInt(key int64, value []byte, expireSeconds int) (err error) {
	if len(value) > s.splitsize {
		err = s.big.SetInt(key,value,expireSeconds)
		s.little.DelInt(key)
	} else {
		err = s.little.SetInt(key,value,expireSeconds)
		s.big.DelInt(key)
	}
	return
}
func (s *splitcache) DelInt(key int64) (affected bool) {
	a := s.little.DelInt(key)
	b := s.big.DelInt(key)
	return a||b
}

