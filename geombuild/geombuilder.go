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


package geombuilder

import geom "github.com/twpayne/go-geom"

const near0 = 1.0/256.0
const mnear0 = -near0

func isEq(a,b float64) bool {
	c := a-b
	return mnear0<c && c<near0
}
func isEqC(a,b geom.Coord) bool {
	return isEq(a[0],b[0])||isEq(a[1],b[1])
}

func concat(coords ...[]geom.Coord) []float64 {
	i := 0
	for _,coord := range coords { i += len(coord) }
	f := make([]float64,0,i*2)
	for _,coord := range coords {
		for _,xy := range coord { f = append(f,xy[0],xy[1]) }
	}
	return f
}

func isLinearRing(gt geom.T) bool {
	_,ok := gt.(*geom.LinearRing)
	return ok
}


// This builder is for multipolygon-relations
// See: https://wiki.openstreetmap.org/wiki/Relation:multipolygon

type ringElem struct{
	dump geom.T
	role string
}

type ringStack []ringElem

func (r *ringStack) Push(gt geom.T,role string) {
	switch v := gt.(type) {
	case *geom.LinearRing:
	case *geom.LineString:
		
		/* If we have a closed LineString, turn it into a LinearRing. */
		if v.NumCoords()>1 {
			if isEqC(v.Coord(0),v.Coord(v.NumCoords()-1)) {
				gt = geom.NewLinearRingFlat(geom.XY,concat(v.Coords()))
			}
		}
	case *geom.Polygon:
		if v.NumLinearRings()==0 { return }
		gt = v.LinearRing(0)
	default: return
	}
	*r = append(*r,ringElem{gt,role})
	for r.merge() {}
}
func (r *ringStack) merge() bool {
	lng := len(*r)
	if lng<2 { return false }
	a,b := (*r)[lng-2],(*r)[lng-1]
	
	if a.role=="" || b.role=="" {
	} else if a.role!=b.role { return false }
	
	if isLinearRing(a.dump) { return false } // Can't merge if 'A' is complete ring.
	
	if isLinearRing(b.dump) { // 'B' is a linestring and 'A' isn't, remove 'A'
		(*r)[lng-2] = b
		*r = (*r)[:lng-1]
		return true
	}
	
	la := a.dump.(*geom.LineString)
	lb := b.dump.(*geom.LineString)
	
	ca := la.Coords()
	cb := lb.Coords()
	
	/* Check, if the ends don't touch */
	if !isEqC(ca[len(ca)-1],cb[0]) { ca,cb = cb,ca } /* Fail, swap, second chance. */
	if !isEqC(ca[len(ca)-1],cb[0]) { return false  } /* Failed again, out! */
	
	fc := concat(ca,cb[1:])
	
	if isEqC(ca[0],cb[len(cb)-1]) { /* If the first and the last coordinate of this String are equal, we have a valid ring. */
		a.dump = geom.NewLinearRingFlat(geom.XY, fc)
	} else {
		a.dump = geom.NewLineStringFlat(geom.XY, fc)
	}
	
	/* Logical or on string values: if 'A's role is "", use 'B's role. */
	if a.role == "" { a.role = b.role }
	
	(*r)[lng-2] = a
	*r = (*r)[:lng-1]
	return true
}
func (r *ringStack) AssemblePolygons() (polys []*geom.Polygon) {
	polys = make([]*geom.Polygon,0,16)
	currentPolygon := make([]*geom.LinearRing,0,16)
	assumeOuter := true
	for _,e := range *r {
		lr,ok := e.dump.(*geom.LinearRing)
		if !ok { continue } /* Ignore fragments. */
		outer := assumeOuter
		switch e.role {
		case "outer": outer = true
		case "inner": outer = false
		}
		/* The first ring is outer. Others are inner. */
		assumeOuter = false
		
		/* We start a new Polygon. Finish the previous one if needed. */
		if outer && len(currentPolygon)>0 {
			poly := geom.NewPolygon(geom.XY)
			for _,ring := range currentPolygon {  poly.Push(ring) }
			currentPolygon = currentPolygon[:0]
			polys = append(polys,poly)
		}
		
		currentPolygon = append(currentPolygon,lr)
	}
	
	if len(currentPolygon)>0 { /* Finish the last polygon. */
		poly := geom.NewPolygon(geom.XY)
		for _,ring := range currentPolygon {  poly.Push(ring) }
		currentPolygon = currentPolygon[:0]
		polys = append(polys,poly)
	}
	return
}
func (r *ringStack) Reset() {
	*r = (*r)[:0]
}

type RelPolygons interface{
	Push(gt geom.T,role string)
	AssemblePolygons() (polys []*geom.Polygon)
	Reset()
}
func NewRelPolygons() RelPolygons { return new(ringStack) }

