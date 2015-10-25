package maptiles

import (
	"fmt"
	"log"

	"github.com/nkovacs/go-mapnik/mapnik"
)

type TileCoord struct {
	X, Y, Zoom uint64
	Tms        bool
	Layer      string
}

func (c TileCoord) OSMFilename() string {
	return fmt.Sprintf("%d/%d/%d.png", c.Zoom, c.X, c.Y)
}

type TileFetchResult struct {
	Coord   TileCoord
	BlobPNG []byte
	Error   error
}

type TileFetchRequest struct {
	Coord   TileCoord
	OutChan chan<- TileFetchResult
}

func (c *TileCoord) setTMS(tms bool) {
	if c.Tms != tms {
		c.Y = (1 << c.Zoom) - c.Y - 1
		c.Tms = tms
	}
}

func NewTileRendererChan(stylesheet string) chan<- TileFetchRequest {
	c := make(chan TileFetchRequest)

	go func(requestChan <-chan TileFetchRequest) {
		t := NewTileRenderer(stylesheet)
		for request := range requestChan {
			t.ProcessRequest(request)
		}
	}(c)

	return c
}

// TileRenderer renders images as Web Mercator tiles
type TileRenderer struct {
	m  *mapnik.Map
	mp mapnik.Projection
}

// Listen starts listening for TileFetchRequests on c.
// If the channel is closed, it stops.
func (t *TileRenderer) Listen(c <-chan TileFetchRequest) {
	for {
		request, ok := <-c
		if !ok {
			// channel closed, we're done
			return
		}
		t.ProcessRequest(request)
	}
}

func (t *TileRenderer) ProcessRequest(request TileFetchRequest) {
	result := TileFetchResult{request.Coord, nil, nil}
	var err error
	result.BlobPNG, err = t.RenderTile(request.Coord)
	if err != nil {
		log.Println("Error while rendering", request.Coord, ":", err.Error())
		result.BlobPNG = nil
		result.Error = err
	}
	request.OutChan <- result
}

func NewTileRenderer(stylesheet string) *TileRenderer {
	t := new(TileRenderer)
	var err error
	if err != nil {
		log.Fatal(err)
	}
	t.m = mapnik.NewMap(256, 256)
	t.m.Load(stylesheet)
	t.mp = t.m.Projection()

	return t
}

func (t *TileRenderer) RenderTile(c TileCoord) ([]byte, error) {
	c.setTMS(false)
	return t.RenderTileZXY(c.Zoom, c.X, c.Y)
}

// Render a tile with coordinates in Google tile format.
// Most upper left tile is always 0,0. Method is not thread-safe,
// so wrap with a mutex when accessing the same renderer by multiple
// threads or setup multiple goroutinesand communicate with channels,
// see NewTileRendererChan.
func (t *TileRenderer) RenderTileZXY(zoom, x, y uint64) ([]byte, error) {
	// Calculate pixel positions of bottom left & top right
	p0 := [2]float64{float64(x) * 256, (float64(y) + 1) * 256}
	p1 := [2]float64{(float64(x) + 1) * 256, float64(y) * 256}

	// Convert to LatLong(EPSG:4326)
	l0 := fromPixelToLL(p0, zoom)
	l1 := fromPixelToLL(p1, zoom)

	// Convert to map projection (e.g. mercartor co-ords EPSG:3857)
	c0 := t.mp.Forward(mapnik.Coord{X: l0[0], Y: l0[1]})
	c1 := t.mp.Forward(mapnik.Coord{X: l1[0], Y: l1[1]})

	// Bounding box for the Tile
	t.m.Resize(256, 256)
	t.m.ZoomToMinMax(c0.X, c0.Y, c1.X, c1.Y)
	t.m.SetBufferSize(128)

	blob, err := t.m.RenderToMemoryPng()
	return blob, err
}
