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

import (
	"database/sql"
	_ "github.com/lib/pq"
)

import "context"
import "flag"
import "io"
import "github.com/paulmach/osm"
import "github.com/paulmach/osm/osmxml"
import "github.com/paulmach/osm/osmpbf"
import "github.com/maxymania/osm-superinserter/projection"
import "github.com/maxymania/osm-superinserter/sqlins"
import "github.com/maxymania/osm-superinserter/restarter"
import "os"
import "log"
import "github.com/maxymania/osm-superinserter/style"
import "time"

var help bool

var table_prefix string
var file,checkfile,stylefile string
var dburl string
var cache int

var is_pbf bool
var is_latlon, is_pseudomerc, is_truemerc bool

var impproj = projection.WebMercator

var intervall string

func init() {
	flag.BoolVar(&help,"help",false,"Help!")
	flag.StringVar(&table_prefix,"prefix","planet_osm","table prefix")
	flag.StringVar(&file,"file","","osm data file (will use STDIN if not specified)")
	flag.StringVar(&dburl,"dburl","","DB-Connection description")
	flag.StringVar(&checkfile,"cont","","continuation memoization file")
	flag.StringVar(&stylefile,"style","openstreetmap-carto.style","carto.style osm2pgsql - Style file")
	flag.StringVar(&intervall,"intervall","1s","Logging intervall")
	
	flag.IntVar(&cache,"cache",128,"number of megabytes of cache")
	flag.BoolVar(&is_pbf,"pbf",false,".pbf data files")
	flag.BoolVar(&is_latlon,"l",false,"Projection = Latitude / longitude; SRID = 4326")
	flag.BoolVar(&is_pseudomerc,"m",false,"Projection = Pseudo-Mercator; SRID = 900913 (default)")
	flag.BoolVar(&is_truemerc,"M",false,"Projection = WGS84 Mercator; SRID = 3395 (experimental/deprecated in OSM)")
}

var cartostyle style.Style
var intervall_raw = time.Second

func prepare() {
	switch {
	case is_latlon     : impproj = projection.LatLon
	case is_pseudomerc : impproj = projection.PseudoMercator
	case is_truemerc   : impproj = projection.WGS84Mercator
	}
	sf,err := os.Open(stylefile)
	if err!=nil {
		log.Fatalf("open(%s): %v",stylefile,err)
	}
	defer sf.Close()
	cartostyle = style.LoadStyle(sf)
	if d,err := time.ParseDuration(intervall); err==nil { intervall_raw = d }
}

var background = context.Background()

func main() {
	flag.Parse()
	if help { flag.PrintDefaults(); return }
	prepare()
	var scanner osm.Scanner
	var src io.Reader
	
	bdr := new(sqlins.Builder)
	
	src = os.Stdin
	
	if file!="" {
		f,err := os.Open(file)
		if err!=nil {
			log.Fatalf("open(%s): %v",file,err)
		}
		defer f.Close()
		src = f
	}
	
	// TODO decompression?
	
	switch {
	case is_pbf:
		scanner = osmpbf.New(background,src,16)
	default:
		scanner = osmxml.New(background,src)
	}
	defer scanner.Close()
	
	{
		db, err := sql.Open("postgres", dburl)
		if err!=nil {
			log.Fatalf("cannot connect to DB: %v",err)
		}
		bdr.DB = db
	}
	
	if checkfile!="" {
		sca2,err := restarter.Restartable(checkfile,scanner)
		if err!=nil {
			log.Fatalf("cannot connect to DB: %v",err)
		}
		scanner = sca2
		bdr.OnCommit = sca2
	}
	
	bdr.Proj = impproj
	
	if cache<16 {
		bdr.InitCache()
	} else {
		bdr.InitCache(cache<<20)
	}
	
	bdr.InitTables(cartostyle,table_prefix)
	bdr.TouchTables()
	
	tck := time.Tick(intervall_raw)
	
	c0,c1,c2,c3 := 0,0,0,0
	for scanner.Scan() {
		c0++
		o := scanner.Object()
		switch v := o.(type) {
		case *osm.Node:
			err := bdr.NodeAdd(v)
			if err!=nil { log.Println(err) ; goto onerr }
			c1++
		case *osm.Way:
			err := bdr.WayAdd(v)
			if err!=nil { log.Println(err) ; goto onerr }
			c2++
		case *osm.Relation:
			err := bdr.RelationAdd(v)
			if err!=nil { log.Println(err) ; goto onerr }
			c3++
		}
		select {
		case <- tck:
			log.Printf("Nodes(%d) Ways(%d) Relations(%d)\n",c1,c2,c3)
		default:
		}
		continue
	}
	
	bdr.Flush()
	
	if err := scanner.Err(); err!=nil {
		log.Printf("Import ended prematurely after Object #%d due to: %v",c0,err)
	}
	return
	onerr:
	log.Println("error...")
	
}

