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


package steps

import "github.com/maxymania/osm-superinserter/store"

import "github.com/paulmach/osm"
import "fmt"
import "time"
import "os"

func CT(temps, files [3]string,sca osm.Scanner, tck <- chan time.Time, brkUE bool) error {
	nodes,err := store.OpenStore(temps[0])
	if err!=nil { return err }
	ways,err := store.OpenStore(temps[1])
	if err!=nil { return err }
	relations,err := store.OpenStore(temps[2])
	if err!=nil { return err }
	
	var nds,wys,rels int
	for sca.Scan() {
		o := sca.Object()
		switch o.(type) {
		case *osm.Node:
			nds++
			err := nodes.Upsert(o)
			if err!=nil { return err }
		case *osm.Way:
			wys++
			err := ways.Upsert(o)
			if err!=nil { return err }
		case *osm.Relation:
			rels++
			err := relations.Upsert(o)
			if err!=nil { return err }
		default:
			if brkUE{
				return fmt.Errorf("CT(): Unexpected object of Type %v",o.ObjectID().Type())
			}
		}
		select {
		case <- tck:
			fmt.Printf("CT(): Nodes(%v) Ways(%v) Relation(%v)\n",nds,wys,rels)
		default:
		}
	}
	fmt.Printf("CT(): Nodes(%v) Ways(%v) Relation(%v)\n",nds,wys,rels)
	
	err  = nodes.ExportFile(files[0])
	if err!=nil { return err }
	err  = ways.ExportFile(files[1])
	if err!=nil { return err }
	relations.ExportFile(files[2])
	
	nodes.DB.Close()
	ways.DB.Close()
	relations.DB.Close()
	
	os.RemoveAll(temps[0])
	os.RemoveAll(temps[1])
	os.RemoveAll(temps[2])
	
	err = sca.Err()
	
	fmt.Println("CT(): Error?:",err)
	return err
}

