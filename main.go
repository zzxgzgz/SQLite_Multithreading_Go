package main

import (
	"SQLite_Multithreading_Go/worker_pool"
	"database/sql"
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
	NumberOfPeople = 1024
	JobQueue chan worker_pool.Job
	letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	InsertStatement *sql.Stmt
)

func main(){
	JobQueue = make(chan worker_pool.Job, MaxQueue)
	log.Println("Hello world!")
	database, _ := sql.Open("sqlite3", "./rio_testing.db")
	wg := sync.WaitGroup{}
	wg.Add(NumberOfPeople)
	dispatcher := worker_pool.NewDispatcher(MaxWorker, JobQueue, &wg)
	dispatcher.Run()
	CreateTableStatement, _ := database.Prepare("CREATE TABLE IF NOT EXISTS people (id INTEGER PRIMARY KEY, firstname TEXT, lastname TEXT)")
	CreateTableStatement.Exec()
	InsertStatement, _ = database.Prepare("INSERT INTO people (firstname, lastname) VALUES (?, ?)")
	insertPeopleStartTime := time.Now()
	InsertPeopleIntoDB()
	//InsertStatement.Exec("ZX", "Zhu")
	wg.Wait()
	insertPeopleEndTime := time.Now()
	log.Printf("Inserting %d people took %v", NumberOfPeople, insertPeopleEndTime.Sub(insertPeopleStartTime))
	rows, _ := database.Query("SELECT id, firstname, lastname FROM people")
	var id int
	var firstname string
	var lastname string
	for rows.Next() {
		rows.Scan(&id, &firstname, &lastname)
		log.Printf("ID: %d, First Name: %s, Last Name: %s", id, firstname, lastname)
	}
}

type People struct {
	FirstName string
	LastName string
}

func randString(size int) string{
	b := make([]rune, size)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func InsertPeopleIntoDB() {
	rand.Seed(time.Now().UnixNano())
	peopleSlice := make([]*People, NumberOfPeople)
	for i:=0 ; i < NumberOfPeople ; i ++ {
		peopleSlice[i] = &People{
			FirstName: randString(rand.Intn(10)),
			LastName:  randString(rand.Intn(10)),
		}
	}

	for i, _ := range peopleSlice {
		ArgumentsInterface := make([]interface{}, 2)
		ArgumentsInterface[0] = peopleSlice[i].FirstName
		ArgumentsInterface[1] = peopleSlice[i].LastName
		JobQueue <- worker_pool.Job{
			Payload: InsertStatement,
			Args:    ArgumentsInterface,
		}
	}
}