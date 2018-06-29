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
	
	_ "github.com/lib/pq"
	"github.com/lib/pq/hstore"
	
	"github.com/paulmach/osm"
	
	"github.com/maxymania/osm-superinserter/projection"
	//"time"
	"fmt"
	"github.com/coocood/freecache"
	"github.com/maxymania/osm-superinserter/style"
	"github.com/twpayne/go-geom"
	//"github.com/twpayne/go-geom/encoding/wkt"
	"github.com/twpayne/go-geom/encoding/wkb"
	"github.com/twpayne/go-geom/encoding/ewkb"
	"strings"
	"strconv"
	"bytes"
	"encoding/binary"
	//"math"
	
	"github.com/maxymania/osm-superinserter/taglists"
	"github.com/maxymania/osm-superinserter/osmcalc"
)

type Table struct{
	b *Builder
	Tname string
	Csql,Isql,Usql string
	HasWayArea,HasZOrder bool
	Style style.Style /* Filtered */
}
func (t *Table) Init(osmtyps []string,tabname string, stl style.Style) *Table {
	t.Tname = tabname
	var create,insert,values,update bytes.Buffer
	
	fmt.Fprintf(&create,"CREATE TABLE %s (osm_id bigint" /*)*/,tabname)
	fmt.Fprintf(&insert,"INSERT INTO %s (osm_id,tags,way" /*)*/,tabname)
	fmt.Fprintf(&values,/*(*/ ") VALUES ($1,$2,ST_GeomFromEWKB($3)")
	fmt.Fprintf(&update,"UPDATE %s SET tags=$2, way=ST_GeomFromEWKB($3)",tabname)
	
	t.Style = make(style.Style,0,len(stl))
	for _,line := range stl {
		f := false
		for _,osmtyp := range osmtyps { f = f || line.IsFor(osmtyp) }
		if !f { continue }
		switch line.Tag {
		case "way_area":
			t.HasWayArea = true
		case "z_order":
			t.HasZOrder = true
		default:
			t.Style = append(t.Style,line)
		}
	}
	off := 4
	if t.HasWayArea {
		fmt.Fprintf(&insert,",way_area")
		fmt.Fprintf(&values,",$%d",off)
		fmt.Fprintf(&update,",way_area = $%d",off)
		off++
	}
	if t.HasZOrder {
		fmt.Fprintf(&insert,",z_order")
		fmt.Fprintf(&values,",$%d",off)
		fmt.Fprintf(&update,",z_order = $%d",off)
		off++
	}
	
	for i,line := range t.Style {
		fmt.Fprintf(&create,",\"%s\" %s",line.Tag,line.DataType)
		fmt.Fprintf(&insert,",\"%s\"",line.Tag)
		fmt.Fprintf(&values,",$%d",i+off)
		fmt.Fprintf(&update,",\"%s\" = $%d",line.Tag,i+off)
	}
	
	if t.HasWayArea { fmt.Fprintf(&create,",\nway_area real") }
	if t.HasZOrder { fmt.Fprintf(&create,",\nz_order integer") }
	
	fmt.Fprintf(&create,/*(*/ ",\ntags hstore, way geometry)")
	values.WriteTo(&insert)
	fmt.Fprintf(&insert,/*(*/ ")")
	
	fmt.Fprintf(&update," WHERE osm_id=$1")
	
	t.Csql = create.String()
	t.Isql = insert.String()
	t.Usql = update.String()
	
	return t
}

func (t *Table) Insert(osm_id int64,way_area float64,z_order int,hs hstore.Hstore,srid int,way geom.T) error {
	return t.Insert2(osm_id,way_area,z_order,hs,srid,way,false)
}
func (t *Table) Insert2(osm_id int64,way_area float64,z_order int,hs hstore.Hstore,srid int,way geom.T,is_insert bool) error {
	waybin,err := ewkb.Marshal(way,binary.LittleEndian)
	//waytxt,err := wkt.Marshal(way)
	if err!=nil { return err }
	/* (osm_id,tags,ways, ...) */
	target := append(
		make([]interface{},0,len(t.Style)+16),
		osm_id,hs,waybin) /* fmt.Sprintf("SRID=%d;%s",srid,waytxt) */
	
	if t.HasWayArea {
		/* (... ,way_area, ...) */
		target = append(target,way_area)
	}
	
	if t.HasZOrder {
		/* (... ,z_order, ...) */
		target = append(target,z_order)
	}
	
	for _,s := range t.Style {
		var targ interface{}
		if r := hs.Map[s.Tag]; r.Valid {
			if strings.HasPrefix(s.DataType,"int") {
				targ,_ = strconv.ParseInt(r.String,0,64)
			} else if strings.HasPrefix(s.DataType,"real") || strings.HasPrefix(s.DataType,"double") {
				targ,_ = strconv.ParseFloat(r.String,64)
			} else {
				targ = r.String
			}
		}
		delete(hs.Map,s.Tag)
		target = append(target,targ)
	}
	if is_insert {
		stm,err := t.b.Get(t.Isql)
		if err!=nil { return err }
		_,err = stm.Exec(target...)
		if err!=nil { return err }
	}else{
		stm,err := t.b.Get(t.Usql)
		if err!=nil { return err }
		if rescount(stm.Exec(target...))<1 {
			stm,err = t.b.Get(t.Isql)
			if err!=nil { return err }
			_,err = stm.Exec(target...)
			if err!=nil { return err }
		}
	}
	t.b.OnWrite()
	
	return nil
}
func (t *Table) ClearDataForObject(id int64) error {
	stm,err := t.b.Get(fmt.Sprintf("DELETE FROM %s WHERE osm_id=$1",t.Tname))
	if err!=nil { return err }
	_,err = stm.Exec(id)
	return err
}


func (t *Table) Read(fields string,id int64, data ...interface{}) error {
	stm,err := t.b.Get(fmt.Sprintf("SELECT %s from %s WHERE osm_id=$1",fields,t.Tname))
	if err!=nil { return err }
	return stm.QueryRow(id).Scan(data...)
}
func (t *Table) GetTags(id int64, hs *hstore.Hstore) error {
	var buffer bytes.Buffer
	data := make([]interface{},1,1+len(t.Style))
	nsp  := make([]sql.NullString,len(t.Style))
	data[0] = hs
	buffer.WriteString("tags")
	for i,line := range t.Style {
		buffer.WriteString(",\"")
		buffer.WriteString(line.Tag)
		buffer.WriteString("\"")
		data = append(data,&nsp[i])
	}
	err := t.Read(buffer.String(),id,data...)
	if err!=nil { return err }
	if hs.Map==nil { hs.Map = make(map[string]sql.NullString) }
	for i,line := range t.Style {
		if !nsp[i].Valid { continue }
		hs.Map[line.Tag] = nsp[i]
	}
	return nil
}
func (t *Table) GetTagNames(id int64, tns *[]string) error {
	var hs hstore.Hstore
	var buffer bytes.Buffer
	data := make([]interface{},1,1+len(t.Style))
	isnl := make([]bool,len(t.Style))
	data[0] = &hs
	buffer.WriteString("tags")
	for i,line := range t.Style {
		buffer.WriteString(",\"")
		buffer.WriteString(line.Tag)
		buffer.WriteString("\" IS NULL")
		data = append(data,&isnl[i])
	}
	err := t.Read(buffer.String(),id,data...)
	if err!=nil { return err }
	if hs.Map!=nil {
		for k := range hs.Map { *tns = append(*tns,k) }
	}
	for i,line := range t.Style {
		if isnl[i] { continue }
		*tns = append(*tns,line.Tag)
	}
	return nil
}


const (
	T_Point uint = iota
	T_Line
	T_Poly
	T_Roads
	T_NUM
)

type TagFlags uint
const (
	TF_Linear TagFlags = 1<<iota
	TF_Polygon
)
func (t TagFlags) Has(o TagFlags) bool { return (t&o)!=0 }

type FuncCommit interface{ Commit() }

type Builder struct{
	DB     *sql.DB
	Tx     *sql.Tx
	Cache  *freecache.Cache
	Proj   projection.Projection
	Tables [T_NUM]Table
	//Temptb [TT_NUM]TempTab
	OnCommit FuncCommit
	Flags  map[string]TagFlags
	psm    map[string]*sql.Stmt
	writes int
	buf    [8]byte
	buf2   [128]byte
}

func (b *Builder) InitCache(size ...int) {
	i := 1<<27
	if len(size)>0 { i = size[0] }
	b.Cache = freecache.NewCache(i)
}
func (b *Builder) InitTables(stl style.Style, prefix string) {
	b.Flags = make(map[string]TagFlags)
	for _,l := range stl {
		tf := TagFlags(0)
		for _,s := range strings.Split(l.Flags,",") {
			switch s {
			case "linear":  tf |= TF_Linear
			case "polygon": tf |= TF_Polygon
			}
		}
		if tf!=0 { b.Flags[l.Tag]=tf }
	}
	
	b.Tables[T_Point].Init([]string{"node"},prefix+"_point",stl)
	b.Tables[T_Line].Init([]string{"way"},prefix+"_line",stl)
	b.Tables[T_Poly].Init([]string{"way","relation"},prefix+"_polygon",stl)
	b.Tables[T_Roads].Init([]string{"way"},prefix+"_roads",stl)
	for i := range b.Tables { b.Tables[i].b = b }
}
func (b *Builder) TouchTables() {
	for i := range b.Tables {
		b.DB.Exec(b.Tables[i].Csql)
		b.DB.Exec(fmt.Sprintf("CREATE INDEX %s_ididx ON %s(osm_id)",b.Tables[i].Tname,b.Tables[i].Tname))
	}
}
func (b *Builder) begin() (err error) {
	if b.Tx==nil {
		b.Tx,err = b.DB.Begin()
	}
	return
}
func (b *Builder) ensurePsmMap() {
	if b.psm==nil { b.psm = make(map[string]*sql.Stmt) }
}
func (b *Builder) Get(sql string) (*sql.Stmt,error) {
	b.ensurePsmMap()
	stm,ok := b.psm[sql]
	if ok { return stm,nil }
	err := b.begin()
	if err!=nil { return nil,err }
	stm,err = b.Tx.Prepare(sql)
	if err!=nil { return nil,err }
	b.psm[sql] = stm
	return stm,nil
}
func (b *Builder) OnWrite() { b.writes++ }
func (b *Builder) AfterWrite() (err error) {
	if b.writes>=(1<<14) {
		for _,stm := range b.psm { stm.Close() }
		b.psm = make(map[string]*sql.Stmt)
		err = b.Tx.Commit()
		if err!=nil {
			b.Tx.Rollback()
		} else if b.OnCommit!=nil {
			b.OnCommit.Commit()
		}
		b.Tx = nil
		b.writes = 0
	}
	return
}
func (b *Builder) Flush() (err error) {
	for _,stm := range b.psm { stm.Close() }
	b.psm = make(map[string]*sql.Stmt)
	err = b.Tx.Commit()
	if err!=nil {
		b.Tx.Rollback()
	} else if b.OnCommit!=nil {
		b.OnCommit.Commit()
	}
	b.Tx = nil
	b.writes = 0
	return
}

func tags2hstore(tags osm.Tags) (r hstore.Hstore) {
	r.Map = make(map[string]sql.NullString)
	for _,tag := range tags {
		r.Map[tag.Key]=sql.NullString{tag.Value,true}
	}
	return
}

const (
	cacheNone = iota
	cacheWay
	cacheTags
	cacheTagNames
)
func (b *Builder) loadWay(id osm.FeatureID) (geom.T,error) {
	var vsr [2]uint
	data,err := b.Cache.GetInt(int64(id.ElementID(cacheWay)))
	if err!=nil {
		var vs []uint
		sign := int64(1)
		switch id.Type() {
		case osm.TypeNode: vs = append(vsr[:0],T_Point)
		case osm.TypeWay: vs = append(vsr[:0],T_Line,T_Poly)
		case osm.TypeRelation: vs = append(vsr[:0],T_Line,T_Poly); sign = -1
		}
		if len(vs)==0 { return nil,fmt.Errorf("not found") }
		for _,v := range vs {
			err = b.Tables[v].Read("ST_AsBinary(way)",sign*id.Ref(),&data)
			if err==nil { break }
		}
		if err!=nil { return nil,err }
		b.Cache.SetInt(int64(id.ElementID(cacheWay)),data,34560000)
	}
	return wkb.Unmarshal(data)
}

func (b *Builder) loadTags(id osm.FeatureID) (hs hstore.Hstore,e error) {
	var vsr [2]uint
	data,err := b.Cache.GetInt(int64(id.ElementID(cacheTags)))
	if err!=nil {
		var vs []uint
		sign := int64(1)
		switch id.Type() {
		case osm.TypeNode: vs = append(vsr[:0],T_Point)
		case osm.TypeWay: vs = append(vsr[:0],T_Line,T_Poly)
		case osm.TypeRelation: vs = append(vsr[:0],T_Line,T_Poly); sign = -1
		}
		if len(vs)==0 { e = fmt.Errorf("not found"); return }
		for _,v := range vs {
			err = b.Tables[v].GetTags(sign*id.Ref(),&hs)
			if err==nil { break }
		}
		ndata,_ := hs.Value() // XXX: This method returns a []byte value.
		b.Cache.SetInt(int64(id.ElementID(cacheTags)),ndata.([]byte),34560000)
		e=err
		return
	}
	if len(data)>0 { e = hs.Scan(data) } else { e = hs.Scan(nil) }
	return
}
func (b *Builder) loadTagNames(id osm.FeatureID) (tns []string,e error) {
	var vsr [2]uint
	data,err := b.Cache.GetInt(int64(id.ElementID(cacheTagNames)))
	if err!=nil {
		var vs []uint
		sign := int64(1)
		switch id.Type() {
		case osm.TypeNode: vs = append(vsr[:0],T_Point)
		case osm.TypeWay: vs = append(vsr[:0],T_Line,T_Poly)
		case osm.TypeRelation: vs = append(vsr[:0],T_Line,T_Poly); sign = -1
		}
		if len(vs)==0 { return nil,fmt.Errorf("not found") }
		for _,v := range vs {
			err = b.Tables[v].GetTagNames(sign*id.Ref(),&tns)
			if err==nil { break }
		}
		if len(tns)==0 {
			b.Cache.SetInt(int64(id.ElementID(cacheTagNames)),[]byte("\x00"),34560000)
		}
		var buffer bytes.Buffer
		for idx,tn := range tns {
			if idx>0 { buffer.WriteString("\x00") }
			buffer.WriteString(tn)
		}
		b.Cache.SetInt(int64(id.ElementID(cacheTagNames)),buffer.Bytes(),34560000)
		e=err
		return
	}
	sdata := string(data)
	if sdata == "\x00" { return }
	tns = strings.Split(sdata,"\x00")
	return
}

func (b *Builder) NodeAdd(n *osm.Node) error {
	var err error
	hs := tags2hstore(n.Tags)
	
	pt := b.Proj.Point(n.Point())
	gt := geom.NewPointFlat(geom.XY,pt[:])
	gt.SetSRID(b.Proj.SRID())
	
	// Insert(osm_id int64,way_area float64,z_order int,hs hstore.Hstore,srid int,way geom.T)
	err = b.Tables[T_Point].Insert(int64(n.ID),0,0,hs,b.Proj.SRID(),gt)
	if err!=nil { return err }
	
	return b.AfterWrite()
}

type metaLayer struct{
	offset int
	roads bool
}
var allLayers = map[string]metaLayer{
	"proposed":       {1 , true },
	"construction":   {2 , false},
	"steps":          {10, false},
	"cycleway":       {10, false},
	"bridleway":      {10, false},
	"footway":        {10, false},
	"path":           {10, false},
	"track":          {11, false},
	"service":        {15, false},
	
	"tertiary_link":  {24, false},
	"secondary_link": {25, true },
	"primary_link":   {27, true },
	"trunk_link":     {28, true },
	"motorway_link":  {29, true },
	
	"raceway":        {30, false},
	"pedestrian":     {31, false},
	"living_street":  {32, false},
	"road":           {33, false},
	"unclassified":   {33, false},
	"residential":    {33, false},
	"tertiary":       {34, false},
	"secondary":      {36, true },
	"primary":        {37, true },
	"trunk":          {38, true },
	"motorway":       {39, true },
}

func (b *Builder) preprocessTagsSub(hs hstore.Hstore) (z_order int,roads bool) {
	i,_ := strconv.ParseInt(hs.Map["layer"].String,10,64)
	z_order = int(100 * i)
	
	if hwi,ok := allLayers[hs.Map["highway"].String]; ok {
		z_order += hwi.offset
		roads = hwi.roads
	}
	
	if hs.Map["railway"].String!="" {
		z_order += 35
		roads = true
	}
	
	if hs.Map["boundary"].String=="administrative" {
		roads = true
	}
	
	if taglists.ValueToBool(hs.Map["bridge"].String,false) { z_order += 100 }
	if taglists.ValueToBool(hs.Map["tunnel"].String,false) { z_order -= 100 }
	return
}

func (b *Builder) preprocessTags(n osm.Object, hs hstore.Hstore, polygon, roads *bool,strict bool) (z_order int) {
	
	add_area_tag := hs.Map["natural"].String=="coastline"
	
	tf := TagFlags(0)
	for k := range hs.Map { tf |= b.Flags[k] }
	
	if roads!=nil {
		z_order,*roads = b.preprocessTagsSub(hs)
	} else {
		z_order,_ = b.preprocessTagsSub(hs)
	}
	
	if polygon!=nil {
		if add_area_tag {
			*polygon = true
			hs.Map["area"] = sql.NullString{"yes",true}
		} else {
			*polygon = taglists.ValueToBool(hs.Map["area"].String,tf.Has(TF_Polygon) )
		}
	}
	return
}

func (b *Builder) WayAdd(n *osm.Way) error {
	hs := tags2hstore(n.Tags)
	
	var polygon,roads bool
	z_order := b.preprocessTags(n,hs,&polygon,&roads,false)
	
	ids := make([]int64,0,len(n.Nodes))
	flt := make([]float64,0,len(n.Nodes)*2)
	for _,node := range n.Nodes {
		ids = append(ids,int64(node.ID))
		gt,err := b.loadWay(node.ID.FeatureID())
		if err!=nil { return err }
		
		flt = append(flt,gt.FlatCoords()...)
	}
	
	var err error
	
	if polygon && osmcalc.Way_IsClosed(n) {
		LR := geom.NewLinearRingFlat(geom.XY,flt)
		poly := geom.NewPolygon(geom.XY)
		poly.Push(LR)
		poly.SetSRID(b.Proj.SRID())
		area := poly.Area()
		
		// Insert(osm_id int64,way_area float64,z_order int,hs hstore.Hstore,srid int,way geom.T)
		err = b.Tables[T_Poly].Insert(int64(n.ID),area,z_order,hs,b.Proj.SRID(),poly)
		if err!=nil { return err }
	} else {
		linestr := geom.NewLineStringFlat(geom.XY,flt)
		linestr.SetSRID(b.Proj.SRID())
		
		// Insert(osm_id int64,way_area float64,z_order int,hs hstore.Hstore,srid int,way geom.T)
		err = b.Tables[T_Line].Insert(int64(n.ID),0,z_order,hs,b.Proj.SRID(),linestr)
		if err!=nil { return err }
		if roads {
			err = b.Tables[T_Roads].Insert(int64(n.ID),0,z_order,hs,b.Proj.SRID(),linestr)
			if err!=nil { return err }
		}
	}
	
	return b.AfterWrite()
}

var networkNumers = map[string]int {
	"lcn": 10,
	"rcn": 11,
	"ncn": 12,
	"lwn": 20,
	"rwn": 21,
	"nwn": 22,
}

func (b *Builder) preprocessRelTags(n *osm.Relation,hs hstore.Hstore, make_boundary, make_polygon *bool, perr *error, allow_typeless bool) (mustDiscard bool) {
	is_route,is_boundary,is_multipolygon := false,false,false
	switch hs.Map["type"].String {
	case "route": is_route = true
	case "boundary": is_boundary = true
	case "multipolygon": is_multipolygon = true
	default:
		if !allow_typeless { return true }
	}
	
	/* If this relation is a route and has the tag name, then copy 'name' as 'route_name'. */
	if k := hs.Map["name"]; is_route && k.Valid { hs.Map["route_name"] = k }
	
	if make_boundary==nil { make_boundary = new(bool) }
	if make_polygon==nil  { make_polygon  = new(bool) }
	if perr==nil { perr = new(error) }
	
	switch {
	case is_route:
		nw := hs.Map["network"]
		if nw.Valid {
			state := hs.Map["state"]
			statetype := sql.NullString{"yes",true}
			switch state.String {
			case "alternate","connection":statetype = state
			}
			if _,ok := networkNumers[nw.String]; ok {
				hs.Map[nw.String] = statetype
			}
		}
		prefcol := hs.Map["preferred_color"]
		switch prefcol.String {
		case "0","1","2","3","4":
		default:
			prefcol = sql.NullString{"0",true}
		}
		hs.Map["route_pref_color"] = prefcol
		
		ref := hs.Map["ref"]
		if _,ok := networkNumers[nw.String]; ok && ref.Valid {
			hs.Map[nw.String+"_ref"] = ref
		}
		
	case is_boundary:
		*make_boundary = true
	case is_multipolygon:
		if hs.Map["boundary"].Valid {
			*make_boundary = true
		} else {
			*make_polygon  = true
			
			poly_new_style := hs.Map["area"].Valid
			if !poly_new_style {
				for k := range hs.Map {
					if b.Flags[k].Has(TF_Polygon) {
						poly_new_style = true
						break
					}
				}
			}
			if !poly_new_style {
				// osm2pgsql fetches the tags of all "inner" members and
				// calculates the set-intersection of all tag-names.
				// The tag-values will be copied from the first "inner menber.
				//
				// After extracting those, osm2pgsql checks, whether one of
				// those has the polygon flag. If not, it returns false.
				var ptags hstore.Hstore
				ptset := make(map[string]int)
				isFirst := true
				for _,member := range n.Members {
					if member.Role != "inner" { continue }
					
					if isFirst {
						// extract the tags of the FIRST "inner" member.
						ptags,*perr = b.loadTags(member.FeatureID())
						if *perr!=nil { return true }
						
						/* If we have a nil-map up-front, terminate the loop. */
						if ptags.Map==nil { break }
						
						/* Insert all existing tag-names into a set. */
						for k := range ptags.Map { ptset[k] = 0 }
						
						isFirst = false
					} else {
						// Abstract:
						//   eliminate all tags from the tag-list of the
						//   FIRST "inner" member that are not present in
						//   the current member.
						
						lst,err := b.loadTagNames(member.FeatureID())
						if err!=nil { *perr = err; return true }
						
						/* Increment all Keys in our node. */
						for _,k := range lst { ptset[k]++ }
						
						/* Decrement all others. */
						for k := range ptags.Map { ptset[k]-- }
						
						/* Lemma: All elements not in this member have been decremented. */
					}
				}
				/* Remove all tags from the tag-list that where not in all "inner" members. */
				for k,v := range ptset {
					if v==0 { continue }
					delete(ptags.Map,k)
				}
				
				mpfl := TagFlags(0)
				for k,v := range ptags.Map {
					mpfl |= b.Flags[k]
					hs.Map[k]=v
				}
				
				if !mpfl.Has(TF_Polygon) { return true }
				
			}
		}
	}
	
	if len(hs.Map)==0 { return true }
	
	// All other processing in osm2pgsql has already been done!
	
	return false
}

func (b *Builder) collectLineStrings(n *osm.Relation) ([]*geom.LineString,error) {
	l := make([]*geom.LineString,0,len(n.Members))
	for _,member := range n.Members {
		if member.Type!=osm.TypeWay { continue }
		t,err := b.loadWay(member.FeatureID())
		//if err!=nil { return nil,err }
		if err!=nil { continue }
		if ls,ok := t.(*geom.LineString) ; ok { l = append(l,ls) }
	}
	return l,nil
}
func (b *Builder) collectPolygons(n *osm.Relation) ([]*geom.Polygon,error) {
	l := make([]*geom.Polygon,0,len(n.Members))
	for _,member := range n.Members {
		if member.Type!=osm.TypeWay { continue }
		t,err := b.loadWay(member.FeatureID())
		//if err!=nil { return nil,err }
		if err!=nil { continue }
		if ls,ok := t.(*geom.Polygon) ; ok { l = append(l,ls) }
	}
	return l,nil
}

func (b *Builder) RelationAdd(n *osm.Relation) error {
	hs := tags2hstore(n.Tags)
	
	var roads bool
	
	z_order := b.preprocessTags(n,hs,nil,&roads,false)
	
	var make_boundary,make_polygon bool
	var err error
	
	if b.preprocessRelTags(n,hs,&make_boundary,&make_polygon, &err, false) {
		return nil
	}
	
	if !make_polygon {
		objs,err := b.collectLineStrings(n)
		if err!=nil { return err }
		
		// Insert(osm_id int64,way_area float64,z_order int,hs hstore.Hstore,srid int,way geom.T)
		err = b.Tables[T_Line].ClearDataForObject(-int64(n.ID))
		if err!=nil { return err }
		for _,obj := range objs {
			obj.SetSRID(b.Proj.SRID())
			err = b.Tables[T_Line].Insert2(-int64(n.ID),0,z_order,hs,b.Proj.SRID(),obj,true)
			if err!=nil { return err }
		}
		if roads {
			err = b.Tables[T_Roads].ClearDataForObject(-int64(n.ID))
			if err!=nil { return err }
			for _,obj := range objs {
				obj.SetSRID(b.Proj.SRID())
				err = b.Tables[T_Roads].Insert2(-int64(n.ID),0,z_order,hs,b.Proj.SRID(),obj,true)
				if err!=nil { return err }
			}
		}
	}
	
	if make_boundary || make_polygon {
		objs,err := b.collectPolygons(n)
		if err!=nil { return err }
		
		var gt geom.T
		switch len(objs) {
		case 0: gt = nil
		case 1:
			objs[0].SetSRID(b.Proj.SRID())
			gt = objs[0]
		default:
			mp := geom.NewMultiPolygon(geom.XY)
			mp.SetSRID(b.Proj.SRID())
			for _,obj := range objs { mp.Push(obj) }
			gt = mp
		}
		
		if gt==nil { goto eofu }
		
		// Insert(osm_id int64,way_area float64,z_order int,hs hstore.Hstore,srid int,way geom.T)
		err = b.Tables[T_Poly].Insert(-int64(n.ID),0,z_order,hs,b.Proj.SRID(),gt)
		if err!=nil { return err }
	}
	
	eofu:
	
	return nil
}


