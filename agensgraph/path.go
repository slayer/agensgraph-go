package agensgraph

import (
	"regexp"
	"database/sql"

	"github.com/pkg/errors"
	"github.com/davecgh/go-spew/spew"
)

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
