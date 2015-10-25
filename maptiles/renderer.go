package maptiles

import (
	"fmt"
	"log"
	"image"
	"image/png"
	"bytes"

	"github.com/nkovacs/go-mapnik/mapnik"
)

type TileCoord struct {
	X, Y, Zoom uint64
	Tms        bool
	Layer      string
}

type MetaTileCoord struct {
	MinX, MinY, MaxX, MaxY, Zoom uint64
	Tms bool
	Layer string
}

func (c TileCoord) OSMFilename() string {
	return fmt.Sprintf("%d/%d/%d.png", c.Zoom, c.X, c.Y)
}

func (c *TileCoord) setTMS(tms bool) {
	if c.Tms != tms {
		c.Y = (1 << c.Zoom) - c.Y - 1
		c.Tms = tms
	}
}

func (c *MetaTileCoord) setTMS(tms bool) {
	if c.Tms != tms {
		c.MinY = (1 << c.Zoom) - c.MinY - 1
		c.MaxY = (1 << c.Zoom) - c.MaxY - 1
		c.MinY, c.MaxY = c.MaxY, c.MinY
		c.Tms = tms
	}
}

func (c *MetaTileCoord) XSize() uint64 {
	if c.MaxX < c.MinX {
		panic(fmt.Errorf("Invalid metatile coordinates"))
	}
	return c.MaxX - c.MinX + 1
}

func (c *MetaTileCoord) YSize() uint64 {
	if c.MaxY < c.MinY {
		panic(fmt.Errorf("Invalid metatile coordinates"))
	}
	return c.MaxY - c.MinY + 1
}

func (c *MetaTileCoord) Count() uint64 {
	return c.XSize() * c.YSize()
}

func (c *MetaTileCoord) TileCoords() []TileCoord {
	xSize := c.XSize()
	ySize := c.YSize()
	coords := make([]TileCoord, 0, xSize * ySize)
	for x := 0; x < int(xSize); x++ {
		for y := 0; y < int(ySize); y++ {
			coords = append(coords, TileCoord{
				X: c.MinX + uint64(x),
				Y: c.MinY + uint64(y),
				Zoom: c.Zoom,
				Tms: c.Tms,
				Layer: c.Layer,
			})
		}
	}
	return coords
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

type MetaTileFetchRequest struct {
	Coord   MetaTileCoord
	// Will output multiple results
	OutChan chan<- TileFetchResult
}

type FetchRequest interface {
	IsMetaTile() bool
	GetCoord() TileCoord
	GetLayer() string
	GetMetaCoord() MetaTileCoord
	GetOutChan() chan<- TileFetchResult
}

func (r TileFetchRequest) IsMetaTile() bool {
	return false
}

func (r TileFetchRequest) GetCoord() TileCoord {
	return r.Coord
}

func (r TileFetchRequest) GetLayer() string {
	return r.Coord.Layer
}

func (r TileFetchRequest) GetMetaCoord() MetaTileCoord {
	panic("GetMetaCoord called on TileFetchRequest")
}

func (r TileFetchRequest) GetOutChan() chan<- TileFetchResult {
	return r.OutChan
}

func (r MetaTileFetchRequest) IsMetaTile() bool {
	return true
}

func (r MetaTileFetchRequest) GetCoord() TileCoord {
	panic("GetCoord called on MetaTileFetchRequest")
}

func (r MetaTileFetchRequest) GetMetaCoord() MetaTileCoord {
	return r.Coord
}

func (r MetaTileFetchRequest) GetLayer() string {
	return r.Coord.Layer
}

func (r MetaTileFetchRequest) GetOutChan() chan<- TileFetchResult {
	return r.OutChan
}

func NewTileRendererChan(stylesheet string) chan<- FetchRequest {
	c := make(chan FetchRequest)

	go func(requestChan <-chan FetchRequest) {
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
func (t *TileRenderer) Listen(c <-chan FetchRequest) {
	for {
		request, ok := <-c
		if !ok {
			// channel closed, we're done
			return
		}
		t.ProcessRequest(request)
	}
}

func (t *TileRenderer) ProcessRequest(request FetchRequest) {
	if request.IsMetaTile() {
		t.processRequestMeta(request.GetMetaCoord(), request.GetOutChan())
	} else {
		t.processRequestTile(request.GetCoord(), request.GetOutChan())
	}
}

func (t *TileRenderer) processRequestTile(coord TileCoord, outchan chan<- TileFetchResult) {
	result := TileFetchResult{coord, nil, nil}
	var err error
	result.BlobPNG, err = t.RenderTile(coord)
	if err != nil {
		log.Println("Error while rendering", coord, ":", err.Error())
		result.BlobPNG = nil
		result.Error = err
	}
	outchan <- result
}

func (t *TileRenderer) processRequestMeta(coord MetaTileCoord, outchan chan<- TileFetchResult) {
	resultCount := coord.Count()
	results, err := t.RenderMetaTile(coord)
	if err != nil {
		// global error, replicate it resultCount times, since receiver expects resultCount results
		xSize := coord.XSize()
		ySize := coord.YSize()
		for x := 0; x < int(xSize); x++ {
			for y := 0; y < int(ySize); y++ {
				outchan <- TileFetchResult{
					Coord: TileCoord{
						X: coord.MinX + uint64(x),
						Y: coord.MinY + uint64(y),
						Zoom: coord.Zoom,
						Tms: coord.Tms,
						Layer: coord.Layer,
					},
					BlobPNG: nil,
					Error: err,
				}
			}
		}
		return
	}
	if len(results) != int(resultCount) {
		panic(fmt.Errorf("metatile rendering result count mismatch: %v != expected %v", len(results), resultCount))
	}
	for _, result := range results {
		outchan <- result
	}
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

type SubImager interface {
	SubImage(r image.Rectangle) image.Image
}

// RenderMetaTile renders multiple tiles as a single tile, then slices them up.
func (t *TileRenderer) RenderMetaTile(c MetaTileCoord) ([]TileFetchResult, error) {
	c.setTMS(false)
	if c.MaxX < c.MinX || c.MaxY < c.MinY {
		return nil, fmt.Errorf("Invalid metatile coordinates")
	}
	xSize := c.XSize()
	ySize := c.YSize()

	xTileSize := 256
	yTileSize := 256

	blob, err := t.renderTileInternal(c.Zoom, c.MinX, c.MinY, uint64(xTileSize), uint64(yTileSize), xSize, ySize, 128)
	if err != nil {
		return nil, err
	}

	results := make([]TileFetchResult, 0, xSize * ySize)

	if xSize == 1 && ySize == 1 {
		results = append(results, TileFetchResult{
			Coord: TileCoord{
				X: c.MinX,
				Y: c.MinY,
				Zoom: c.Zoom,
				Tms: c.Tms,
				Layer: c.Layer,
			},
			BlobPNG: blob,
			Error: nil,
		})
		return results, nil
	}

	r := bytes.NewReader(blob)
	img, err := png.Decode(r)
	if err != nil {
		return nil, err
	}

	bounds := img.Bounds()
	bx := bounds.Min.X
	by := bounds.Min.Y
	simg, ok := img.(SubImager)
	if !ok {
		return nil, fmt.Errorf("Decoded image type does not have SubImage method")
	}

	// cut the image into pieces
	for x := 0; x < int(xSize); x++ {
		for y := 0; y < int(ySize); y++ {
			startX := x * xTileSize
			startY := y * yTileSize
			endX := (x + 1) * xTileSize
			endY := (y + 1) * yTileSize

			subimg := simg.SubImage(image.Rectangle{
				Min: image.Point{
					X: startX + bx,
					Y: startY + by,
				},
				Max: image.Point{
					X: endX + bx,
					Y: endY + by,
				},
			})

			var buf bytes.Buffer

			err := png.Encode(&buf, subimg)

			results = append(results, TileFetchResult{
				Coord: TileCoord{
					X: c.MinX + uint64(x),
					Y: c.MinY + uint64(y),
					Zoom: c.Zoom,
					Tms: c.Tms,
					Layer: c.Layer,
				},
				BlobPNG: buf.Bytes(),
				Error: err,
			})
		}
	}

	return results, nil
}

func (t *TileRenderer) renderTileInternal(zoom, x, y, xTileSize, yTileSize, xMetaTile, yMetaTile, bufferSize uint64) ([]byte, error) {
	// Calculate pixel positions of bottom left & top right
	p0 := [2]float64{float64(x) * float64(xTileSize), (float64(y) + float64(yMetaTile)) * float64(yTileSize)}
	p1 := [2]float64{(float64(x) + float64(xMetaTile)) * float64(xTileSize), float64(y) * float64(yTileSize)}

	// Convert to LatLong(EPSG:4326)
	l0 := fromPixelToLL(p0, zoom)
	l1 := fromPixelToLL(p1, zoom)

	// Convert to map projection (e.g. mercartor co-ords EPSG:3857)
	c0 := t.mp.Forward(mapnik.Coord{X: l0[0], Y: l0[1]})
	c1 := t.mp.Forward(mapnik.Coord{X: l1[0], Y: l1[1]})

	// Bounding box for the Tile
	t.m.Resize(uint32(xTileSize * xMetaTile), uint32(yTileSize * yMetaTile))
	t.m.ZoomToMinMax(c0.X, c0.Y, c1.X, c1.Y)
	t.m.SetBufferSize(int(bufferSize))

	blob, err := t.m.RenderToMemoryPng()
	return blob, err
}

// Render a tile with coordinates in Google tile format.
// Most upper left tile is always 0,0. Method is not thread-safe,
// so wrap with a mutex when accessing the same renderer by multiple
// threads or setup multiple goroutinesand communicate with channels,
// see NewTileRendererChan.
func (t *TileRenderer) RenderTileZXY(zoom, x, y uint64) ([]byte, error) {
	return t.renderTileInternal(zoom, x, y, 256, 256, 1, 1, 128)
}
