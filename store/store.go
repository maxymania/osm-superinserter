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

package store

import "github.com/syndtr/goleveldb/leveldb/table"
import "github.com/syndtr/goleveldb/leveldb"
import "github.com/paulmach/osm"
import "encoding/xml"
//import json2 "github.com/json-iterator/go"
import "encoding/binary"
import "os"
import "github.com/syndtr/goleveldb/leveldb/iterator"

// TableReader

import (
	"io"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/cache"
	"bytes"
	"fmt"
)

type IReader interface{
	Get(id osm.ObjectID,t osm.Object) error
	Iterate() IIterator
}
type IIterator interface{
	Next() bool
	Fetch(t osm.Object) error
	Release()
}
type Iterator struct{
	Iter iterator.Iterator
	On   bool
}
func (i *Iterator) Next() bool {
	if i.On {
		return i.Iter.Next()
	}
	i.On = true
	return i.Iter.First()
}
func (i *Iterator) Fetch(t osm.Object) error {
	return xml.Unmarshal(i.Iter.Value(),t)
}
func (i *Iterator) Release() { i.Iter.Release() }

type Storage struct{
	DB *leveldb.DB
	batch leveldb.Batch
	count int
	buf [8]byte
}
func OpenStore(path string) (*Storage,error) {
	db,err := leveldb.OpenFile(path,nil)
	if err!=nil {
		db,err = leveldb.RecoverFile(path,nil)
	}
	if err!=nil { return nil,err }
	return &Storage{DB:db},nil
}

func (s *Storage) Upsert(o osm.Object) (err error) {
	b,_ := xml.Marshal(o)
	k := s.buf[:]
	binary.BigEndian.PutUint64(k,uint64(o.ObjectID().Ref()))
	s.batch.Put(k,b)
	s.count += len(k)+len(b)
	if s.count > (128<<10) {
		s.count = 0
		err = s.DB.Write(&s.batch,nil)
		s.batch.Reset()
	}
	return
}
func (s *Storage) Flush() (err error) {
	if s.count>0 {
		s.count = 0
		err = s.DB.Write(&s.batch,nil)
		s.batch.Reset()
	}
	return
}
func (s *Storage) Get(id osm.ObjectID,t osm.Object) error {
	k := s.buf[:]
	binary.BigEndian.PutUint64(k,uint64(id.Ref()))
	v,err := s.DB.Get(k,nil)
	if err!=nil { return err }
	return xml.Unmarshal(v,t)
}
func (s *Storage) Iterate() IIterator {
	return &Iterator{Iter:s.DB.NewIterator(nil,nil)}
}
func (s *Storage) ExportFile(fn string) error {
	f,err := os.Create(fn)
	if err!=nil { return err }
	defer f.Close()
	w := table.NewWriter(f,nil)
	defer w.Close()
	iter := s.DB.NewIterator(nil,nil)
	defer iter.Release()
	if !iter.First() { return nil }
	err = w.Append(iter.Key(),iter.Value())
	if err!=nil { return err }
	for iter.Next() {
		err = w.Append(iter.Key(),iter.Value())
		if err!=nil { return err }
	}
	return nil
}

type Importer struct{
	TBL *table.Reader
	buf[8] byte
}
func OpenImporter(f io.ReaderAt, size int64) (*Importer,error) {
	chx := cache.NewCache(cache.NewLRU(1<<28))
	r,err := table.NewReader(f,size,storage.FileDesc{storage.TypeTable,1},&cache.NamespaceGetter{chx,1},util.NewBufferPool(1<<20),nil)
	if err!=nil { return nil,err }
	return &Importer{TBL:r},nil
}
func OpenImporterFile(fn string) (*Importer,error) {
	f,err := os.Open(fn)
	if err!=nil { return nil,err }
	s,err := f.Stat()
	if err!=nil { f.Close(); return nil,err }
	i,err := OpenImporter(f,s.Size())
	if err!=nil { f.Close(); return nil,err }
	return i,nil
}
func (i Importer) Clone() *Importer { return &i }
func (i *Importer) Get(id osm.ObjectID,t osm.Object) error {
	k := i.buf[:]
	binary.BigEndian.PutUint64(k,uint64(id.Ref()))
	nk,v,err := i.TBL.Find(k,true,nil)
	if err!=nil { return err }
	if !bytes.Equal(nk,k) { return fmt.Errorf("NotFound") }
	return xml.Unmarshal(v,t)
}
func (i *Importer) Iterate() IIterator {
	return &Iterator{Iter:i.TBL.NewIterator(nil,nil)}
}


