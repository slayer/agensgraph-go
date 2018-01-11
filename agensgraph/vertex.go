package agensgraph

import (
	"regexp"
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/davecgh/go-spew/spew"
)

type GID struct {
    oid string
    id string
}

type Vertex struct {
    label string
    vid GID
    props interface{}
}

func ParseVertex(regexp *regexp.Regexp, value string) (*Vertex, error) {
	var err error

	elems := regexp.FindStringSubmatch(value)
	if(len(elems) < 4) {
		err = errors.Errorf(
			"Vertex string passed to irregular number of elements (%s)",
			spew.Sdump(elems))
		return nil, err
	}

	var props interface{}
	err = json.Unmarshal([]byte(elems[4]), &props)

	if err != nil {
		err = errors.Wrapf(err,
			"Could not unmarshal json for vertex (%s)",
			spew.Sdump(elems[8]))
		return nil, err
	}

	v := &Vertex{
		label: elems[1],
		vid: GID {
			oid: elems[2],
			id: elems[3],
		},
		props: props,
	}

	return v, nil
}
