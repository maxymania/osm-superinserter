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


package restarter

import "github.com/paulmach/osm"
import "github.com/edsrzf/mmap-go"
import "os"
import "unsafe"

type Scanner interface{
	osm.Scanner
	Commit()
}


type wrapper struct{
	osm.Scanner
	count int64
	pcount *int64
	area mmap.MMap
	file *os.File
}
func (w *wrapper) Scan() bool {
	r := w.Scanner.Scan()
	if r { w.count++ }
	return r
}
func (w *wrapper) Commit() {
	*(w.pcount) = w.count
}
func (w *wrapper) skip() {
	for i := w.count; i>0 ; i-- {
		w.Scanner.Scan()
	}
}

func Restartable(chkfile string,s osm.Scanner) (Scanner,error) {
	f,err := os.OpenFile(chkfile,os.O_CREATE|os.O_RDWR,0666)
	if err!=nil { return nil,err }
	f.Truncate(64<<10)
	mm,err := mmap.Map(f,mmap.RDWR,0)
	if err!=nil { return nil,err }
	
	pcnt := (*int64)(unsafe.Pointer(&mm[(1<<10)-8]))
	
	wrap := &wrapper{s,*pcnt,pcnt,mm,f}
	wrap.skip()
	return wrap,nil
}

