package maptiles

import (
	"crypto/md5"
	"database/sql"
	"fmt"
	"log"
	"sync"

	_ "github.com/mattn/go-sqlite3"
	//"net/http"
	//"strconv"
)

// MBTiles 1.2-compatible Tile Db with multi-layer support.
// Was named Mbtiles before, hence the use of *m in methods.
type TileDb struct {
	db          *sql.DB
	requestChan chan TileFetchRequest
	insertChan  chan TileFetchResult
	layerIds    map[string]int
	qc          chan bool
	dbLock      sync.RWMutex
}

func NewTileDb(path string) *TileDb {
	m := TileDb{}
	var err error
	m.db, err = sql.Open("sqlite3", path)
	if err != nil {
		log.Println("Error opening db", err.Error())
		return nil
	}
	queries := []string{
		"PRAGMA journal_mode = OFF",
		"PRAGMA synchronous=OFF",
		"CREATE TABLE IF NOT EXISTS layers(layer_name text PRIMARY KEY NOT NULL)",
		"CREATE TABLE IF NOT EXISTS metadata (name text PRIMARY KEY NOT NULL, value text NOT NULL)",
		"CREATE TABLE IF NOT EXISTS layered_tiles (layer_id integer, zoom_level integer, tile_column integer, tile_row integer, checksum text, PRIMARY KEY (layer_id, zoom_level, tile_column, tile_row) FOREIGN KEY(checksum) REFERENCES tile_blobs(checksum))",
		"CREATE TABLE IF NOT EXISTS tile_blobs (checksum text, tile_data blob)",
		"CREATE VIEW IF NOT EXISTS tiles AS SELECT layered_tiles.zoom_level as zoom_level, layered_tiles.tile_column as tile_column, layered_tiles.tile_row as tile_row, (SELECT tile_data FROM tile_blobs WHERE checksum=layered_tiles.checksum) as tile_data FROM layered_tiles WHERE layered_tiles.layer_id = (SELECT rowid FROM layers WHERE layer_name='default')",
		"REPLACE INTO metadata VALUES('name', 'go-mapnik cache file')",
		"REPLACE INTO metadata VALUES('type', 'overlay')",
		"REPLACE INTO metadata VALUES('version', '0')",
		"REPLACE INTO metadata VALUES('description', 'Compatible with MBTiles spec 1.2. However, this file may contain multiple overlay layers, but only the layer called default is exported as MBtiles')",
		"REPLACE INTO metadata VALUES('format', 'png')",
		"REPLACE INTO metadata VALUES('bounds', '-180.0,-85,180,85')",
		"INSERT OR IGNORE INTO layers(layer_name) VALUES('default')",
	}

	for _, query := range queries {
		_, err = m.db.Exec(query)
		if err != nil {
			log.Println("Error setting up db", err.Error())
			return nil
		}
	}

	m.readLayers()

	m.insertChan = make(chan TileFetchResult)
	m.requestChan = make(chan TileFetchRequest)
	m.Run()
	return &m
}

func (m *TileDb) readLayers() {
	m.layerIds = make(map[string]int)
	rows, err := m.db.Query("SELECT rowid, layer_name FROM layers")
	if err != nil {
		log.Fatal("Error fetching layer definitions", err.Error())
	}
	var s string
	var i int
	for rows.Next() {
		if err := rows.Scan(&i, &s); err != nil {
			log.Fatal(err)
		}
		m.layerIds[s] = i
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}

var layerMx sync.RWMutex

func (m *TileDb) ensureLayer(layer string) {
	layerMx.RLock()
	if _, ok := m.layerIds[layer]; !ok {
		layerMx.RUnlock()
		layerMx.Lock()
		defer layerMx.Unlock()
		if _, ok := m.layerIds[layer]; ok {
			return
		}
		if _, err := m.db.Exec("INSERT OR IGNORE INTO layers(layer_name) VALUES(?)", layer); err != nil {
			log.Println(err)
		}
		m.readLayers()
		return
	}
	layerMx.RUnlock()
}

func (m *TileDb) Close() {
	close(m.insertChan)
	close(m.requestChan)
	if m.qc != nil {
		<-m.qc // block until channel qc is closed (meaning Run() is finished)
	}
	if err := m.db.Close(); err != nil {
		log.Print(err)
	}

}

func (m *TileDb) InsertQueue() chan<- TileFetchResult {
	return m.insertChan
}

func (m *TileDb) RequestQueue() chan<- TileFetchRequest {
	return m.requestChan
}

// Best executed in a dedicated go routine.
func (m *TileDb) Run() {
	m.qc = make(chan bool)
	go func() {
		requestClosed := false
		insertClosed := false
		for {
			select {
			case r, ok := <-m.requestChan:
				if !ok {
					requestClosed = true
				} else {
					go m.fetch(r)
				}
			case i, ok := <-m.insertChan:
				if !ok {
					insertClosed = true
				} else {
					go m.insert(i)
				}
			}
			if requestClosed && insertClosed {
				break
			}
		}
		m.qc <- true
	}()
}

type batchBlob struct {
	data     []byte
	checksum string
}

type batchTile struct {
	layerID int
	z       uint64
	x       uint64
	y       uint64
	s       string
}

// maximum length of inserts is 199 due to SQLITE_MAX_VARIABLE_NUMBER being 999
func (m *TileDb) BatchInsert(inserts []TileFetchResult) {
	m.dbLock.Lock()
	defer m.dbLock.Unlock()

	tiles := make([]batchTile, 0)
	blobs := make([]batchBlob, 0)

	var tilesMx sync.Mutex
	var blobsMx sync.Mutex
	var wg sync.WaitGroup
	wg.Add(len(inserts))

	tileSql := "REPLACE INTO layered_tiles VALUES" // VALUES(?, ?, ?, ?, ?) m.layerIds[l], z, x, y, s
	blobSql := "REPLACE INTO tile_blobs VALUES"    // VALUES(?,?) checksum, blob

	for idx := range inserts {
		i := &inserts[idx]
		go func() {
			defer wg.Done()
			i.Coord.setTMS(true)
			x, y, z, l := i.Coord.X, i.Coord.Y, i.Coord.Zoom, i.Coord.Layer
			if l == "" {
				l = "default"
			}
			m.ensureLayer(l)
			s := fmt.Sprintf("%x", md5.Sum(i.BlobPNG))

			tilesMx.Lock()
			tiles = append(tiles, batchTile{
				layerID: m.layerIds[l],
				z:       z,
				x:       x,
				y:       y,
				s:       s,
			})
			tilesMx.Unlock()

			row := m.db.QueryRow("SELECT 1 FROM tile_blobs WHERE checksum=?", s)
			var dummy uint64
			err := row.Scan(&dummy)
			switch {
			case err == sql.ErrNoRows:
				blobsMx.Lock()
				defer blobsMx.Unlock()
				blobs = append(blobs, batchBlob{
					data:     i.BlobPNG,
					checksum: s,
				})
				return
			case err != nil:
				log.Println("error during test", err)
				return
			default:
				//log.Println("Reusing blob", s)
			}
		}()
	}

	wg.Wait()

	if len(blobs) > 0 {
		first := true
		args := make([]interface{}, 0, 2*len(blobs))
		for idx := range blobs {
			if first {
				first = false
			} else {
				blobSql += ","
			}
			blobSql += "(?, ?)"
			blob := &blobs[idx]
			args = append(args, blob.checksum, blob.data)
		}

		blobStatement, err := m.db.Prepare(blobSql + ";")
		if err != nil {
			log.Println("error during blob statement preparation", err)
			return
		}

		_, err = blobStatement.Exec(args...)
		if err != nil {
			log.Println("error inserting blobs", err)
			return
		}
	}

	first := true
	args := make([]interface{}, 0, 5*len(blobs))
	for idx := range tiles {
		if first {
			first = false
		} else {
			tileSql += ","
		}
		tileSql += "(?, ?, ?, ?, ?)" // m.layerIds[l], z, x, y, s
		tile := &tiles[idx]
		args = append(args, tile.layerID, tile.z, tile.x, tile.y, tile.s)
	}

	tileStatement, err := m.db.Prepare(tileSql + ";")
	if err != nil {
		log.Println("error during tile statement preparation", err)
		return
	}

	_, err = tileStatement.Exec(args...)
	if err != nil {
		log.Println("error inserting tiles", err)
		return
	}
}

func (m *TileDb) insert(i TileFetchResult) {
	m.dbLock.Lock()
	defer m.dbLock.Unlock()
	i.Coord.setTMS(true)
	x, y, z, l := i.Coord.X, i.Coord.Y, i.Coord.Zoom, i.Coord.Layer
	if l == "" {
		l = "default"
	}
	h := md5.New()
	_, err := h.Write(i.BlobPNG)
	if err != nil {
		log.Println(err)
		return
	}
	s := fmt.Sprintf("%x", h.Sum(nil))
	row := m.db.QueryRow("SELECT 1 FROM tile_blobs WHERE checksum=?", s)
	var dummy uint64
	err = row.Scan(&dummy)
	switch {
	case err == sql.ErrNoRows:
		if _, err = m.db.Exec("REPLACE INTO tile_blobs VALUES(?,?)", s, i.BlobPNG); err != nil {
			log.Println("error during insert", err)
			return
		}
	case err != nil:
		log.Println("error during test", err)
		return
	default:
		//log.Println("Reusing blob", s)
	}
	m.ensureLayer(l)
	sql := "REPLACE INTO layered_tiles VALUES(?, ?, ?, ?, ?)"
	if _, err = m.db.Exec(sql, m.layerIds[l], z, x, y, s); err != nil {
		log.Println(err)
	}
}

func (m *TileDb) fetch(r TileFetchRequest) {
	m.dbLock.RLock()
	defer m.dbLock.RUnlock()
	r.Coord.setTMS(true)
	zoom, x, y, l := r.Coord.Zoom, r.Coord.X, r.Coord.Y, r.Coord.Layer
	if l == "" {
		l = "default"
	}
	result := TileFetchResult{r.Coord, nil, nil}
	queryString := `
		SELECT tile_data 
		FROM tile_blobs 
		WHERE checksum=(
			SELECT checksum 
			FROM layered_tiles 
			WHERE zoom_level=? 
				AND tile_column=? 
				AND tile_row=?
				AND layer_id=(SELECT rowid FROM layers WHERE layer_name=?)
		)`
	var blob []byte
	row := m.db.QueryRow(queryString, zoom, x, y, l)
	err := row.Scan(&blob)
	switch {
	case err == sql.ErrNoRows:
		result.BlobPNG = nil
	case err != nil:
		log.Println(err)
		result.Error = err
	default:
		result.BlobPNG = blob
	}
	r.OutChan <- result
}
