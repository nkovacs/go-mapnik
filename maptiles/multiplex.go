package maptiles

import (
	"log"
	"runtime"
)

type LayerMultiplex struct {
	layerChans   map[string]chan<- FetchRequest
	numRenderers int
}

func NewLayerMultiplex(numRenderers int) *LayerMultiplex {
	if numRenderers == 0 {
		numRenderers = runtime.GOMAXPROCS(0)
	}
	l := LayerMultiplex{
		layerChans:   make(map[string]chan<- FetchRequest),
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

func (l *LayerMultiplex) CreateRenderer(stylesheet string) chan<- FetchRequest {
	c := make(chan FetchRequest)
	for i := 0; i < l.numRenderers; i++ {
		renderer := NewTileRenderer(stylesheet)
		go renderer.Listen(c)
	}

	return c
}

func (l *LayerMultiplex) AddRenderer(name string, stylesheet string) {
	l.AddSource(name, l.CreateRenderer(stylesheet))
}

func (l *LayerMultiplex) AddSource(name string, fetchChan chan<- FetchRequest) {
	l.layerChans[name] = fetchChan
}

func (l LayerMultiplex) SubmitRequest(r FetchRequest) bool {
	c, ok := l.layerChans[r.GetLayer()]
	if ok {
		c <- r
	} else {
		log.Println("No such layer", r.GetLayer())
	}
	return ok
}
