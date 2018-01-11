package agensgraph_test

import (
	"log"
	"os"
	"testing"

	agensgraph "../agensgraph"
)

const (
	TEST_QUERY = `match (s:Person {name: 'John'})-[p:knows]->(o:Person) return s as S, p as P, o as O`
)

func TestMain(m *testing.M) {
	log.Println("init")
	setup()
	retCode := m.Run()
	teardown()
	os.Exit(retCode)
}

func setup() {
	c := agensgraph.Config{Host: "localhost", Port: "5432", User: "agraph", Password: "password", Database: "agraph"}
	db, err := agensgraph.New(c)
	fck(err)
	_, err = db.Query(`CREATE (:person {city: 'test', name: 'John'})`);
	fck(err)
	_, err = db.Query(`CREATE (:person {city: 'test2', name: 'Patrick'})`);
	fck(err)
	_, err = db.Query(`CREATE (:person {name: 'John', city: 'test'})-[:knows]->(:person {name: 'Patrick', city: 'test2'});`)
	fck(err)
}

func teardown() {
	c := agensgraph.Config{Host: "localhost", Port: "5432", User: "agraph", Password: "password", Database: "agraph"}
	db, err := agensgraph.New(c)
	fck(err)
	_, err = db.Query(`match (s:Person) DETACH DELETE (s) `);
}

//from main.go root:
//$ go test -v -bench=. -benchmem ./...
//testing: warning: no tests to run
//PASS
//BenchmarkSelect-8   	    1000	   1410300 ns/op	 1594293 B/op	    2176 allocs/op
func BenchmarkSelect(b *testing.B) {
	c := agensgraph.Config{Host: "localhost", Port: "5432", User: "agraph", Password: "password", Database: "agraph"}
	db, err := agensgraph.New(c)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err = db.Select(TEST_QUERY)
	}

	fck(err)
}

func fck(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
