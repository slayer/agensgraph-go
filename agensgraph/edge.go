package agensgraph

import (
	"regexp"
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/davecgh/go-spew/spew"
)

type Edge struct {
    label string
    eid GID
		svid GID
		evid GID
    props interface{}
}

func ParseEdge(regexp *regexp.Regexp, value string) (*Edge, error) {
	var err error

	elems := regexp.FindStringSubmatch(value)
	if(len(elems) < 8) {
		err = errors.Errorf(
			"Edge string passed to irregular number of elements (%s)",
			spew.Sdump(elems))
		return nil, err
	}

	var props interface{}
	err = json.Unmarshal([]byte(elems[8]), &props)

	if err != nil {
		err = errors.Wrapf(err,
			"Could not unmarshal json for edge (%s)",
			spew.Sdump(elems[8]))
		return nil, err
	}

	e := &Edge{
		label: elems[1],
		eid: GID {
			oid: elems[2],
			id: elems[3],
		},
		svid: GID {
			oid: elems[4],
			id: elems[5],
		},
		evid: GID {
			oid: elems[6],
			id: elems[7],
		},
		props: props,
	}

	return e, err
}
