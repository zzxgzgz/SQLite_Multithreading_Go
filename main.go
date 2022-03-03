package main

import (
	"SQLite_Multithreading_Go/worker_pool"
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"math/rand"
	"sync"
	"time"
)

var (
	// MaxWorker Number of Workers
	MaxWorker = 16
	// MaxQueue Max Size of the Job Queue
	MaxQueue = 1024
	// NumberOfPeople how many people to generate
	NumberOfPeople = 1024
	// JobQueue a queue that sends the sql.Stmt jobs to the workers
	JobQueue chan worker_pool.Job
	letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	// InsertStatement INSERT INTO people (firstname, lastname) VALUES (?, ?)
	InsertStatement *sql.Stmt
	peopleSlice []*People
	allQueryStart time.Time
	allQueryEnd time.Time
	database *sql.DB
)

func main(){
	JobQueue = make(chan worker_pool.Job, MaxQueue)
	log.Println("Hello world!")

	// Open the database, this command creates the .db file if it doesn't exist.
	database, _ = sql.Open("sqlite3", "./rio_testing.db")
	wg := sync.WaitGroup{}
	// run the job dispatcher.
	dispatcher := worker_pool.NewDispatcher(MaxWorker, JobQueue, &wg)
	dispatcher.Run()

	// Create table if it doesn't already exist/
	CreateTableStatement, _ := database.Prepare("CREATE TABLE IF NOT EXISTS people (id INTEGER PRIMARY KEY, firstname TEXT, lastname TEXT)")
	CreateTableStatement.Exec()

	// Clear the table so that it is empty.
	TruncateTableStatement, _ := database.Prepare("DELETE FROM people")
	TruncateTableStatement.Exec()

	GeneratePeople()

	InsertStatement, _ = database.Prepare("INSERT INTO people (firstname, lastname) VALUES (?, ?)")
	insertPeopleStartTime := time.Now()
	InsertPeopleIntoDB()
	insertPeopleEndTime := time.Now()
	log.Printf("Inserting %d people took %v", NumberOfPeople, insertPeopleEndTime.Sub(insertPeopleStartTime))

	wg.Add(NumberOfPeople)
	QueryPeopleFromDB()
	wg.Wait()
	allQueryEnd = time.Now()

	log.Printf("To query %d people, it took time: %v", NumberOfPeople, allQueryEnd.Sub(allQueryStart))

}

// People simple struct that represents a person.
type People struct {
	FirstName string
	LastName string
}

// randString generates random string with length size
func randString(size int) string{
	b := make([]rune, size)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

// GeneratePeople generates people and random first/last names and put them into a slice.
func GeneratePeople(){
	peopleSlice = make([]*People, NumberOfPeople)
	rand.Seed(time.Now().UnixNano())
	for i:=0 ; i < NumberOfPeople ; i ++ {
		peopleSlice[i] = &People{
			FirstName: randString(rand.Intn(10) + 1),
			LastName:  randString(rand.Intn(10) + 1),
		}
	}
	log.Printf("Gernated %d people!", NumberOfPeople)
}

// InsertPeopleIntoDB When writing into a sqlite db, we'd better do it sequentially,
// or it will cause LOCK related errors
func InsertPeopleIntoDB() {
	for i, _ := range peopleSlice {
		ArgumentsInterface := make([]interface{}, 2)
		ArgumentsInterface[0] = peopleSlice[i].FirstName
		ArgumentsInterface[1] = peopleSlice[i].LastName

		_, err := InsertStatement.Exec(ArgumentsInterface[0], ArgumentsInterface[1])
		if err != nil {
			log.Fatalf("Error happened when inserting people %d: %s", i, err.Error())
		}
	}
}

// QueryPeopleFromDB When reading from a sqlite db, we can send queries concurrently
func QueryPeopleFromDB(){
	QueryPeopleStatementSlice := make([]*sql.Stmt, NumberOfPeople)
	for i, _ := range peopleSlice {
		queryPeopleString := fmt.Sprintf("SELECT id, firstname, lastname FROM people WHERE firstname = '%s' AND lastname = '%s'", peopleSlice[i].FirstName, peopleSlice[i].LastName)
		QueryPeopleStatementSlice[i],_ = database.Prepare(queryPeopleString)
	}
	allQueryStart = time.Now()
	for i,_ := range QueryPeopleStatementSlice {
		JobQueue <- worker_pool.Job{
			Payload: QueryPeopleStatementSlice[i],
			Args:    nil,
		}
	}
}