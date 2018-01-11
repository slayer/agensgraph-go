package agensgraph

import (
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
