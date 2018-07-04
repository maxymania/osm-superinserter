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

type EValidation uint

const (
	EShortLinearRings EValidation = iota
	ENonClosedLinearRings
	EEmptyPolygon
)

var errReasons = [...]string{
	"Polygon must have at least four points in each ring",
	"geometry contains non-closed rings",
	"Polygon must have at least one ring",
}


func (e EValidation) Error() string {
	if EValidation(uint(len(errReasons)))<=e { return "???" }
	return errReasons[e]
}

func ValidateLinearRing(r *geom.LinearRing) error {
	if r.NumCoords() < 4 { return EShortLinearRings }
	last := r.Coord(r.NumCoords()-1)
	first := r.Coord(0)
	for i := range last {
		if !isEq(last[i],first[i]) { return ENonClosedLinearRings }
	}
	return nil
}

func ValidatePolygon(p *geom.Polygon) error {
	n := p.NumLinearRings()
	if n==0 { return EEmptyPolygon }
	for i := 0; i<n; i++ {
		err := ValidateLinearRing(p.LinearRing(i))
		if err!=nil { return err }
	}
	return nil
}
func ValidateOrRepairPolygon(p *geom.Polygon, l geom.Layout) error {
	n := p.NumLinearRings()
	if n==0 { return EEmptyPolygon }
	var reserve *geom.Polygon
	for i := 0; i<n; i++ {
		err := ValidateLinearRing(p.LinearRing(i))
		if err!=nil {
			if i==0 { return err } /* If the first ring is invalid, abort! */
			if reserve == nil {
				reserve = geom.NewPolygon(l)
				for j := 0; j < i; j++ {
					reserve.Push(p.LinearRing(j))
				}
			}
		} else if reserve != nil {
			reserve.Push(p.LinearRing(i))
		}
	}
	if reserve != nil {
		p.Swap(reserve)
	}
	return nil
}


