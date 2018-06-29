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


package sqlins

import (
	"database/sql"
	
	"github.com/lib/pq"
	
	"github.com/maxymania/osm-superinserter/store"
	"github.com/paulmach/osm"
	
	"github.com/maxymania/osm-superinserter/projection"
	"time"
	"fmt"
)

func rescount(res sql.Result,err error) int64 {
	if err!=nil { return 0 }
	i,err := res.RowsAffected()
	if err!=nil { return 0 }
	return i
}

func InsertNodesLegacy(tab store.IReader, db *sql.DB, prj projection.Projection, tck <- chan time.Time) error {
	
	db.Exec(`CREATE TABLE planet_osm_nodes (
		id bigint PRIMARY KEY,
		lat integer NOT NULL,
		lon integer NOT NULL,
		tags text[]
	)`)
	
	var batch *sql.Tx
	var insert,update *sql.Stmt 
	
	const isql = `INSERT INTO planet_osm_nodes (id,lat,lon,tags) VALUES ($1,$2,$3,$4)`
	const usql = `UPDATE planet_osm_nodes SET lat=$2, lon=$3, tags=$4 WHERE id=$1`
	
	stt := func() error {
		var err error
		
		if batch==nil { batch,err = db.Begin() }
		if err!=nil { return err }
		
		if insert==nil { insert,err = batch.Prepare(isql) }
		if err!=nil { return err }
		
		if update==nil { update,err = batch.Prepare(usql) }
		if err!=nil { return err }
		
		return nil
	}
	
	etx := func() error {
		if insert!=nil { insert.Close() }
		if update!=nil { update.Close() }
		insert = nil
		update = nil
		if batch==nil { return nil }
		err := batch.Commit()
		if err!=nil { batch.Rollback() }
		batch = nil
		return err
	}
	defer etx()
	
	node := new(osm.Node)
	iter := tab.Iterate()
	defer iter.Release()
	scale := 10000000.0
	if prj != projection.LatLon { scale = 100.0 }
	uc,ic := 0,0
	count := 0
	
	if err := stt(); err!=nil { return err }
	for iter.Next() {
		*node = osm.Node{}
		err := iter.Fetch(node)
		if err!=nil {
			return err
		}
		p := prj.Point(node.Point())
		oid := node.ObjectID()
		
		arr := pq.StringArray{}
		
		for _,t := range node.Tags {
			arr = append(arr,t.Key,t.Value)
		}
		
		var arrnil interface{}
		if len(arr)>0 { arrnil = arr }
		
		args := []interface{}{oid.Ref(),int64(p.Lat()*scale),int64(p.Lon()*scale),arrnil}
		
		if rescount(update.Exec(args...))<1 {
			insert.Exec(args...)
			ic++
		} else {
			uc++
		}
		count++
		
		if count>10000 {
			if err := etx(); err!=nil { return err }
			if err := stt(); err!=nil { return err }
			count = 0
		}
		
		select {
		case <- tck:
			fmt.Printf("INL(): Nodes(u=%v, i=%v)\n",uc,ic)
		default:
		}
	}
	fmt.Printf("INL(): Nodes(u=%v, i=%v)\n",uc,ic)
	return etx()
}


func InsertWayLegacy(tab store.IReader, db *sql.DB, prj projection.Projection, tck <- chan time.Time) error {
	
	db.Exec(`CREATE TABLE planet_osm_ways (
		id bigint PRIMARY KEY,
		nodes bigint[] NOT NULL,
		tags text[],
		pending boolean NOT NULL
	)`)
	
	var batch *sql.Tx
	var insert,update *sql.Stmt 
	
	const isql = `INSERT INTO planet_osm_ways (id,nodes,tags,pending) VALUES ($1,$2,$3,true)`
	const usql = `UPDATE planet_osm_ways SET nodes=$2, tags=$3, pending=true WHERE id=$1`
	
	stt := func() error {
		var err error
		
		if batch==nil { batch,err = db.Begin() }
		if err!=nil { return err }
		
		if insert==nil { insert,err = batch.Prepare(isql) }
		if err!=nil { return err }
		
		if update==nil { update,err = batch.Prepare(usql) }
		if err!=nil { return err }
		
		return nil
	}
	
	etx := func() error {
		if insert!=nil { insert.Close() }
		if update!=nil { update.Close() }
		insert = nil
		update = nil
		if batch==nil { return nil }
		err := batch.Commit()
		if err!=nil { batch.Rollback() }
		batch = nil
		return err
	}
	defer etx()
	
	node := new(osm.Way)
	iter := tab.Iterate()
	defer iter.Release()
	uc,ic := 0,0
	count := 0
	
	if err := stt(); err!=nil { return err }
	for iter.Next() {
		*node = osm.Way{}
		err := iter.Fetch(node)
		if err!=nil {
			return err
		}
		oid := node.ObjectID()
		
		nds := make(pq.Int64Array,len(node.Nodes))
		for i,way := range node.Nodes {
			nds[i] = int64(way.ID)
		}
		
		arr := make(pq.StringArray,0,len(node.Tags)*2)
		
		for _,t := range node.Tags {
			arr = append(arr,t.Key,t.Value)
		}
		
		var arrnil interface{}
		if len(arr)>0 { arrnil = arr }
		
		args := []interface{}{oid.Ref(),nds,arrnil}
		
		if rescount(update.Exec(args...))<1 {
			insert.Exec(args...)
			ic++
		} else {
			uc++
		}
		count++
		
		if count>10000 {
			if err := etx(); err!=nil { return err }
			if err := stt(); err!=nil { return err }
			count = 0
		}
		
		select {
		case <- tck:
			fmt.Printf("IWL(): Ways(u=%v, i=%v)\n",uc,ic)
		default:
		}
	}
	fmt.Printf("IWL(): Ways(u=%v, i=%v)\n",uc,ic)
	return etx()
}


func InsertRelationsLegacy(tab store.IReader, db *sql.DB, prj projection.Projection, tck <- chan time.Time) error {
	
	db.Exec(`CREATE TABLE planet_osm_rels (
		id bigint PRIMARY KEY,
		way_off smallint,
		rel_off smallint,
		parts bigint[],
		members text[],
		tags text[],
		pending boolean NOT NULL
	)`)
	
	var batch *sql.Tx
	var insert,update *sql.Stmt 
	
	const isql = `INSERT INTO planet_osm_rels (id,way_off,rel_off,parts,members,tags,pending) VALUES ($1,$2,$3,$4,$5,$6,true)`
	const usql = `UPDATE planet_osm_rels SET way_off=$2, rel_off=$3, parts=$4, members=$5, tags=$6, pending=true WHERE id=$1`
	
	stt := func() error {
		var err error
		
		if batch==nil { batch,err = db.Begin() }
		if err!=nil { return err }
		
		if insert==nil { insert,err = batch.Prepare(isql) }
		if err!=nil { return err }
		
		if update==nil { update,err = batch.Prepare(usql) }
		if err!=nil { return err }
		
		return nil
	}
	
	etx := func() error {
		if insert!=nil { insert.Close() }
		if update!=nil { update.Close() }
		insert = nil
		update = nil
		if batch==nil { return nil }
		err := batch.Commit()
		if err!=nil { batch.Rollback() }
		batch = nil
		return err
	}
	defer etx()
	
	node := new(osm.Relation)
	iter := tab.Iterate()
	defer iter.Release()
	uc,ic := 0,0
	count := 0
	
	var types = map[osm.Type]int {
		"node" : 0,
		"way" : 1,
		"relation" : 2,
	}
	var rtypes = "nwr"
	
	if err := stt(); err!=nil { return err }
	for iter.Next() {
		*node = osm.Relation{}
		err := iter.Fetch(node)
		if err!=nil {
			return err
		}
		oid := node.ObjectID()
		
		var elems [3][]int
		
		for i := range node.Members {
			e,ok := types[node.Members[i].Type]
			if !ok { continue }
			elems[e] = append(elems[e],i)
		}
		
		way_off := len(elems[0])
		rel_off := len(elems[1])+way_off
		total   := len(elems[2])+rel_off
		
		parts  := make(pq.Int64Array,0,total)
		strarr := make(pq.StringArray,0,total*2)
		
		for x := range elems {
			for y := range elems[x] {
				i := elems[x][y]
				parts = append(parts,node.Members[i].Ref)
				if x==0 { continue }
				strarr = append(strarr,fmt.Sprintf("%c%d",rtypes[x],node.Members[i].Ref),node.Members[i].Role)
			}
		}
		
		arr := pq.StringArray{}
		
		for _,t := range node.Tags {
			arr = append(arr,t.Key,t.Value)
		}
		
		var arrnil interface{}
		if len(arr)>0 { arrnil = arr }
		
		args := []interface{}{oid.Ref(),way_off,rel_off,parts,strarr,arrnil}
		
		if rescount(update.Exec(args...))<1 {
			insert.Exec(args...)
			ic++
		} else {
			uc++
		}
		count++
		
		if count>10000 {
			if err := etx(); err!=nil { return err }
			if err := stt(); err!=nil { return err }
			count = 0
		}
		
		select {
		case <- tck:
			fmt.Printf("IRL(): Relations(u=%v, i=%v)\n",uc,ic)
		default:
		}
	}
	fmt.Printf("IRL(): Relations(u=%v, i=%v)\n",uc,ic)
	return etx()
}


