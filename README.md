# Super-Inserter

This is experimental and unfinished Software. It sucks.

Requires `postgis` and `hstore`!

# libraries used

For PBF and other OSM formats `github.com/paulmach/osm`

For (E)WKB Geometrys. `github.com/twpayne/go-geom`

and others...

# Installing

```
go get github.com/maxymania/osm-superinserter/cmdl/osminserter
```

## Help

```
osminserter -help
```

Will output:

```
  -M	Projection = WGS84 Mercator; SRID = 3395 (experimental/deprecated in OSM)
  -cache int
    	number of megabytes of cache (default 128)
  -cont string
    	continuation memoization file
  -dburl string
    	DB-Connection description
  -file string
    	osm data file (will use STDIN if not specified)
  -help
    	Help!
  -intervall string
    	Logging intervall (default "1s")
  -l	Projection = Latitude / longitude; SRID = 4326
  -m	Projection = Pseudo-Mercator; SRID = 900913 (default)
  -pbf
    	.pbf data files
  -prefix string
    	table prefix (default "planet_osm")
  -style string
    	carto.style osm2pgsql - Style file (default "openstreetmap-carto.style")
```
