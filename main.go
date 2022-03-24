package main

import (
	"SQLite_Multithreading_Go/worker_pool"
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"math/rand"
	"runtime"
	"sync"
	"time"
)

var (
	// MaxWorker Number of Workers
	MaxWorker = runtime.NumCPU() //* 2
	// MaxQueue Max Size of the Job Queue
	MaxQueue = 1024
	// NumberOfPeople how many people to generate
	NumberOfPeople = 1024 * 1024 * 20
	// JobQueue a queue that sends the sql.Stmt jobs to the workers
	JobQueue chan worker_pool.Job
	letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	// InsertStatement INSERT INTO people (firstname, lastname) VALUES (?, ?)
	InsertStatement *sql.Stmt
	QueryStatement *sql.Stmt
	peopleSlice []*People
	allQueryStart time.Time
	allQueryEnd time.Time
	//database *sql.DB
	//db_connections []*sql.DB
	db_write_connection *sql.DB
	db_create_connection *sql.DB
	db_read_connection *sql.DB
)

func main(){
	JobQueue = make(chan worker_pool.Job, MaxQueue)
	//db_connections = make([]*sql.DB, MaxWorker)
	log.Println("Hello world!")
	//for i := 0 ; i < MaxWorker ; i ++ {
	//	db_connections[i], _ = sql.Open("sqlite3", "file:./rio_testing.db?cache=shared&mode=rwc")
	//}
	// mode: read write
	db_write_connection, _ = sql.Open("sqlite3", "file:./rio_testing.db?&mode=rw&_journal_mode=wal&_txlock=immediate")
	// only ONE writer, as SQLite doesn't support multiple concurrent write.
	db_write_connection.SetMaxOpenConns(1)
	// mode: read only
	db_read_connection, _ = sql.Open("sqlite3", "file:./rio_testing.db?&mode=ro&_journal_mode=wal&_mutex=full&cache=shared")
	// can have many readers
	db_read_connection.SetMaxOpenConns(MaxWorker)
	// mode: read write create
	db_create_connection, _ = sql.Open("sqlite3", "file:./rio_testing.db?&mode=rwc&_journal_mode=wal")

	// Open the database, this command creates the .db file if it doesn't exist.
	//database, _ = sql.Open("sqlite3", "./rio_testing.db?cache=shared&mode=rwc")
	//database.SetMaxOpenConns(MaxWorker)
	wg := sync.WaitGroup{}
	// run the job dispatcher.
	dispatcher := worker_pool.NewDispatcher(MaxWorker, JobQueue, &wg)
	dispatcher.Run()

	// Create table if it doesn't already exist/
	CreateTableStatement, _ := db_create_connection.Prepare("CREATE TABLE IF NOT EXISTS people (id INTEGER PRIMARY KEY, firstname TEXT, lastname TEXT)")
	CreateTableStatement.Exec()

	// Clear the table so that it is empty.
	TruncateTableStatement, _ := db_create_connection.Prepare("DELETE FROM people")
	TruncateTableStatement.Exec()

	CreateFirstNameIndexStatement, _ := db_create_connection.Prepare("CREATE INDEX IF NOT EXISTS idx_people_firstname ON people (firstname)")
	CreateFirstNameIndexStatement.Exec()

	CreateLastNameIndexStatement, _ := db_create_connection.Prepare("CREATE INDEX IF NOT EXISTS idx_people_lastname ON people (lastname)")
	CreateLastNameIndexStatement.Exec()

	// close the create table connection
	db_create_connection.Close()

	GeneratePeople()

	insertPeopleStartTime := time.Now()
	InsertPeopleIntoDB()
	insertPeopleEndTime := time.Now()
	log.Printf("Inserting %d people took %v", NumberOfPeople, insertPeopleEndTime.Sub(insertPeopleStartTime))

	// close the write connection
	db_write_connection.Close()

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
	tx, _ := db_write_connection.Begin()
	InsertStatement, _ = tx.Prepare("INSERT INTO people (firstname, lastname) VALUES (?, ?)")
	for i, _ := range peopleSlice {
		ArgumentsInterface := make([]interface{}, 2)
		ArgumentsInterface[0] = peopleSlice[i].FirstName
		ArgumentsInterface[1] = peopleSlice[i].LastName

		_, err := InsertStatement.Exec(ArgumentsInterface[0], ArgumentsInterface[1])
		if err != nil {
			log.Fatalf("Error happened when inserting people %d: %s", i, err.Error())
		}
	}
	tx.Commit()
	InsertStatement.Close()
}

// QueryPeopleFromDB When reading from a sqlite db, we can send queries concurrently
func QueryPeopleFromDB(){
	//QueryPeopleStatementSlice := make([]*sql.Stmt, NumberOfPeople)
	//for i, _ := range peopleSlice {
	//	queryPeopleString := fmt.Sprintf("SELECT id, firstname, lastname FROM people WHERE firstname = '%s' AND lastname = '%s'", peopleSlice[i].FirstName, peopleSlice[i].LastName)
	//	QueryPeopleStatementSlice[i],_ = database.Prepare(queryPeopleString)
	//}
	//allQueryStart = time.Now()
	//for i,_ := range QueryPeopleStatementSlice {
	//	JobQueue <- worker_pool.Job{
	//		Payload: QueryPeopleStatementSlice[i],
	//		Args:    nil,
	//	}
	//}
	allQueryStart = time.Now()
	//QueryStatementSlice := make([]*sql.Stmt, MaxWorker)
	//for i := 0 ; i < MaxWorker ; i ++ {
	//	QueryStatementSlice[i], _ = db_connections[i].Prepare("SELECT id, firstname, lastname FROM people WHERE firstname = ? AND lastname = ?")
	//}
	//var id int
	//var firstName string
	//var lastName string
	QueryStatement, _ = db_read_connection.Prepare("SELECT id, firstname, lastname FROM people WHERE firstname = ? AND lastname = ?")
	//QueryStatement, _ = database.Prepare("SELECT id, firstname, lastname FROM people WHERE firstname = ? AND lastname = ?")
	for index, _ := range peopleSlice {
		JobQueue <- worker_pool.Job{
			Payload: QueryStatement,
			Args:    []interface{}{peopleSlice[index].FirstName, peopleSlice[index].LastName},
		}
		//row:=db_read_connection.QueryRow("SELECT id, firstname, lastname FROM people WHERE firstname = ? AND lastname = ?", peopleSlice[index].FirstName, peopleSlice[index].LastName)
		////row := QueryStatement.QueryRow(peopleSlice[index].FirstName, peopleSlice[index].LastName)
		//row.Scan(&id, &firstName, &lastName)
		////queryEnd.Sub(queryStart)
		//if firstName != peopleSlice[index].FirstName || lastName != peopleSlice[index].LastName {
		//	panic(fmt.Sprintf("Query for firstname: %s, lastname: %s; got firstname: %s, lastname: %s",
		//		peopleSlice[index].FirstName, peopleSlice[index].LastName, firstName, lastName))
		//}
	}
}