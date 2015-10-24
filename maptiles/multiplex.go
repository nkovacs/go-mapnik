package maptiles

import (
	"log"
	"runtime"
)

type LayerMultiplex struct {
	layerChans   map[string]chan<- TileFetchRequest
	numRenderers int
}

func NewLayerMultiplex(numRenderers int) *LayerMultiplex {
	if numRenderers == 0 {
		numRenderers = runtime.GOMAXPROCS(0)
	}
	l := LayerMultiplex{
		layerChans:   make(map[string]chan<- TileFetchRequest),
		numRenderers: numRenderers,
	}
	return &l
}

func DefaultRenderMultiplex(defaultStylesheet string, numRenderers int) *LayerMultiplex {
	l := NewLayerMultiplex(numRenderers)
	renderer := l.CreateRenderer(defaultStylesheet)
	l.AddSource("", renderer)
	l.AddSource("default", renderer)
	return l
}

func (l *LayerMultiplex) CreateRenderer(stylesheet string) chan<- TileFetchRequest {
	c := make(chan TileFetchRequest)
	for i := 0; i < l.numRenderers; i++ {
		renderer := NewTileRenderer(stylesheet)
		go renderer.Listen(c)
	}

	return c
}

func (l *LayerMultiplex) AddRenderer(name string, stylesheet string) {
	l.AddSource(name, l.CreateRenderer(stylesheet))
}

func (l *LayerMultiplex) AddSource(name string, fetchChan chan<- TileFetchRequest) {
	l.layerChans[name] = fetchChan
}

func (l LayerMultiplex) SubmitRequest(r TileFetchRequest) bool {
	c, ok := l.layerChans[r.Coord.Layer]
	if ok {
		c <- r
	} else {
		log.Println("No such layer", r.Coord.Layer)
	}
	return ok
}
