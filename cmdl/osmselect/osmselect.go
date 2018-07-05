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



package main


import "context"
import "github.com/paulmach/osm"
import "github.com/paulmach/osm/osmpbf"
import "github.com/paulmach/osm/osmxml"

import "encoding/xml"
import "fmt"
import "os"
import "flag"
import "io"

var all,nodes,ways,rels bool

var in_stdin, in_pbf bool
var limit,offset uint64

var in_file string

var out_ind bool

func init(){
	flag.BoolVar(&all,"all",false,"select *")
	flag.BoolVar(&nodes,"nodes",false,"select OSM Nodes")
	flag.BoolVar(&ways,"ways",false,"select OSM Ways")
	flag.BoolVar(&rels,"rels",false,"select OSM Relations")
	flag.BoolVar(&in_stdin,"stdin",false,"use stdin as input")
	flag.BoolVar(&in_pbf,"pbf",false,".pbf-format")
	flag.BoolVar(&out_ind,"indent",false,"pretty-print XML")
	
	flag.Uint64Var(&limit,"limit",0,"Max. number of elements, 0==infinity")
	flag.Uint64Var(&offset,"offset",0,"Number of elements to skip")
	
	flag.StringVar(&in_file,"file","","use file as input")
}

var pre = xml.StartElement{xml.Name{"","osm"},[]xml.Attr{
	{xml.Name{"","version"},"0.6"},
	{xml.Name{"","generator"},"osmselect 0.1"},
}}
var post = xml.EndElement{xml.Name{"","osm"}}


func main() {
	flag.Parse()
	fmt.Print(xml.Header)
	enc := xml.NewEncoder(os.Stdout)
	if out_ind { enc.Indent(""," ") }
	enc.EncodeToken(pre)
	
	var src io.Reader
	
	if in_stdin {
		src = os.Stdin
	} else if in_file!="" {
		f,err := os.Open(in_file)
		if err==nil { src = f } else { fmt.Fprintf(os.Stderr,"Opening input-file %q: %v\n",in_file,err) }
	}
	
	if src==nil{
		enc.EncodeToken(post)
		enc.Flush()
		fmt.Println()
		return
	}
	
	var s osm.Scanner
	if in_pbf {
		s = osmpbf.New(context.Background(),src,4)
	} else {
		s = osmxml.New(context.Background(),src)
	}
	defer s.Close()
	
	nolimit := limit==0
	
	for s.Scan() {
		o := s.Object()
		if all { goto skipped }
		switch o.ObjectID().Type() {
		case osm.TypeNode: if !nodes { continue }
		case osm.TypeWay: if !ways { continue }
		case osm.TypeRelation: if !rels { continue }
		}
		skipped:
		if offset>0 {
			offset--
			continue
		}
		if nolimit {} else if limit==0  { break }
		limit--
		enc.Encode(o)
	}
	
	err := s.Err()
	if err!=nil { fmt.Fprintf(os.Stderr,"premature end: %v\n",err) }
	
	enc.EncodeToken(post)
	enc.Flush()
	fmt.Println()
}

