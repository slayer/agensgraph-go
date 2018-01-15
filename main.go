package main

import (
  "log"
  "github.com/pkg/errors"

  "github.com/davecgh/go-spew/spew"
  "github.com/ps23/agensgraph-go/agensgraph"
)

const MAIN_QUERY = `match (s:Person {name: 'John'})-[p:knows]->(o:Person) return s as s, p as p, o as o`

func main() {
  var err error
  c := agensgraph.Config{Host: "localhost", Port: "5432", User: "agraph", Password: "password", Database: "agraph"}
	db, err := agensgraph.New(c)

	paths, err := db.Select(`match (s:Person {name: 'John'})-[p:knows]->(o:Person) return s as s, p as p, o as o`)

  fck(err)

  if(len(paths) == 0) {
    err = errors.Errorf(
      "No paths returned from query (%s)",
      spew.Sdump(MAIN_QUERY))
    fck(err)
  }

  var path = paths[0];
  log.Println("length of paths array: ", len(paths))
  log.Println("Printing item at index 0")
  spew.Dump(path)
}

func fck(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
