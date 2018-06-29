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


package style

import "regexp"
import "bufio"
import "io"

var uline = regexp.MustCompile(`^[^\#]*`)
var data  = regexp.MustCompile(`\S+`)
var jug  = regexp.MustCompile(`[^,]+`)

type Line struct{
	OsmType, Tag, DataType, Flags string
}
func (l *Line) IsFor(s string) bool {
	for _,ot := range jug.FindAllString(l.OsmType,-1) {
		if ot==s { return true }
	}
	return false
}
type Style []Line

func LoadStyle(style io.Reader) (s Style) {
	br := bufio.NewReaderSize(style,1<<13)
	for {
		slc,err := br.ReadSlice('\n')
		if err!=nil { break }
		slc = uline.Find(slc)
		cols := data.FindAll(slc,4)
		if len(cols)<4 { continue }
		
		s = append(s,Line{
			string(cols[0]),
			string(cols[1]),
			string(cols[2]),
			string(cols[3]),
		})
	}
	return
}


