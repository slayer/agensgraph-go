package agensgraph

import (
	"regexp"
	"database/sql"

	"github.com/davecgh/go-spew/spew"
	"github.com/pkg/errors"
	"github.com/jmoiron/sqlx"
)

type Roach struct {
	// Db holds a sqlx.DB pointer that represents a pool of zero or more
	// underlying connections - safe for concurrent use by multiple
	// goroutines -, with freeing/creation of new connections all managed
	// by the `sqlx` package and this again by the 'sql/database' package.
	*sqlx.DB
	cfg Config
}

// Each sql.Row in the Resultset is transform into a Path
// A Path holds the start and end vertex as well as the
// connecting edge
type Path struct {
    Vertices []Vertex
		Edges []Edge
}

// Convenience function to return the start vertex only
func (p *Path) Start() (*Vertex, error) {
		var errNotFound = errors.Errorf("No vertices found")
		if(len(p.Vertices) > 0) {
    	return &p.Vertices[0], nil
		}

		return nil, errNotFound
}

// Convenience function to return the end vertex only
func (p *Path) End() (*Vertex, error) {
		var errNotFound = errors.Errorf("No vertices found in path")
		if(len(p.Vertices) > 0) {
			return &p.Vertices[len(p.Vertices) - 1], nil
		}

		return nil, errNotFound
}

// Convenience function to return the edges
func (p *Path) EdgeList() (*[]Edge) {
    return &p.Edges
}

// Convenience function to return the number of edges
func (p *Path) Len() (int) {
    return len(p.Edges)
}

type mapScan struct {
	// cp are the column pointers
	cp []interface{}
	colCount int
	colNames []string
}

// Initialise a mapScan struct and fill the column
// pointers with sql.RawBytes type
func NewMapScan(columnNames []string) *mapScan {
	lenCN := len(columnNames)
	s := &mapScan{
		cp:       make([]interface{}, lenCN),
		colCount: lenCN,
		colNames: columnNames,
	}

	for i := 0; i < lenCN; i++ {
		s.cp[i] = new(sql.RawBytes)
	}
	return s
}

// Parse a row in the Resultset to a Path struct
func (s *mapScan) ParsePath(rows *sql.Rows) (*Path, error) {
	var err error

	if err = rows.Scan(s.cp...); err != nil {
		if err == sql.ErrNoRows {
			// there were no rows, but otherwise no error occurred
		} else {
			err = errors.Wrapf(err,
				"Error when scanning a row in result set")
		}

		return nil, err
	}

	// Currently regular expressions are used to extract
	// the content of vertices and edges. This is relatively slow
	// but a simplification until a tokenizer is in place
	vreg := regexp.MustCompile(`(.+)\[(\d+)\.(\d+)\](.+)`)
	ereg := regexp.MustCompile(`(.+)\[(\d+)\.(\d+)\]\[(\d+)\.(\d+),(\d+)\.(\d+)\](.*)`)

	isVertex := true;
	vertices := []Vertex{}
	edges := []Edge{}

	// Parse colums as Vertex and Edge repectively
	// We expect an order v -> e -> v
	for i := 0; i < s.colCount; i++ {
		if rb, ok := s.cp[i].(*sql.RawBytes); ok {
			if(isVertex) {
				v, err := ParseVertex(vreg, string(*rb))
				if err != nil {
					err = errors.Wrapf(err,
						"Error when trying to parse vertex (%s)",
						spew.Sdump(string(*rb)))
					return nil, err
				}
				vertices = append(vertices, *v)
			} else {
				e, err := ParseEdge(ereg, string(*rb))
				if err != nil {
					err = errors.Wrapf(err,
						"Error when trying to parse edge (%s)",
						spew.Sdump(string(*rb)))
					return nil, err
				}
				edges = append(edges, *e)
			}
			isVertex = !isVertex
			*rb = nil // reset pointer to discard current value to avoid a bug
		} else {
			err = errors.Wrapf(err,
				"Cannot convert index %d column %s to type *sql.RawBytes", i, s.colNames[i])
			return nil, err
		}
	}

	p := &Path {
		Vertices: vertices,
		Edges: edges,
	}

	return p, nil
}

func (db * Roach) Select(query string) ([]Path, error) {
	var paths []Path
	var err error

	// Query using the *sqlx.DB pointer of the Roach struct
	rows, err := db.Query(query)
	if err != nil {
		err = errors.Wrapf(err,
			"Error when querying using query (%s)",
			spew.Sdump(query))
		return nil, err
  }

	// create a new mapScan struct for the column colNames
	// present in this Resultset
	cols, err := rows.Columns()
	rc := NewMapScan(cols)

	if(len(cols) != 3) {
		err = errors.Errorf(
			"Currently only triples of vertex -> edge -> vertex are supported")
		return nil, err
	}

	// cycle through the Resultset and append the paths containing
	// the vertices and edges for extract from each row
	for rows.Next() {
		p, err := rc.ParsePath(rows)
		if err != nil {
			return paths, err
		}
		paths = append(paths, *p)
	}
	if err = rows.Err(); err != nil {
		err = errors.Wrapf(err,
			"Abnormal termination when looping through the result set for query (%s)",
			spew.Sdump(query))
		return nil, err
	}
	if err = rows.Close(); err != nil {
		err = errors.Wrapf(err,
			"Abnormal termination when closing result set for query (%s)",
			spew.Sdump(query))
		return nil, err
	}

	//Return the paths
	return paths, nil
}
