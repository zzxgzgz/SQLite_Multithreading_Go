package main



import (
	"database/sql"
	"fmt"
	"math"
	"os"
	"runtime"
	"sync"
	"time"
	_ "github.com/mattn/go-sqlite3"
)

var stub = make([]byte, 1024 * 1024 * 1024 * 4)

func main(){
	// allow the program to use all cores.
	runtime.GOMAXPROCS(runtime.NumCPU())
	// allocate memory for the program.
	stub[0] = 1
	type BCCode struct {
		B_Code string
		C_Code string
		CodeType int
		IsNew int
	}
	number_of_go_routines := 20

	// Open the connection to the DB, will create the .db file is it does not exist.
	db, err := sql.Open("sqlite3", "file:./rio_testing.db?loc=auto&_journal_mode=wal&_mutex=no")



	// one DB connection can be used by two go-routines
	number_of_db_connections := int(math.Ceil(float64(number_of_go_routines) / 2.0))



	db_connections := make([]*sql.DB, number_of_db_connections)


	// create multiple db connections for concurrent read.
	for i := 0 ; i < number_of_db_connections ; i ++{
		db_connections[i],_ = sql.Open("sqlite3", "file:./rio_testing.db?loc=auto&_journal_mode=wal&_mutex=no")
	}

	db.SetMaxOpenConns(1)
	if err != nil {
		fmt.Println("SQLite:", err)
	}
	fmt.Println("SQLite start")
	//Create the table
	sqlStmt := `create table BC (b_code text not null primary key, c_code text not null, code_type INTEGER, is_new INTEGER);`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		fmt.Printf("create table error->%q: %s\n", err, sqlStmt)
		return
	}
	//Create index on column
	_, err = db.Exec("CREATE INDEX inx_c_code ON BC(c_code);")
	if err != nil {
		fmt.Println("create index error->%q: %s\n", err, sqlStmt)
		return
	}
	//Start time measuring
	start := time.Now().Unix()
	tx, err := db.Begin()
	if err != nil {
		fmt.Printf("%q\n", err)
	}
	// prepare insert statement
	stmt, err := tx.Prepare("insert into BC(b_code, c_code, code_type, is_new ) values(?,?,?,?)")
	if err != nil {
		fmt.Printf("insert err %q\n", err)
	}
	db.Close()
	defer stmt.Close()
	var m int = 1000 * 1000
	var total int = 1 * m
	for i := 0; i < total; i++ {
		_, err = stmt.Exec(fmt.Sprintf("B%024d", i), fmt.Sprintf("C%024d", i), 0, 1)
		if err != nil {
			fmt.Println("%q", err)
		}
	}
	tx.Commit()
	insertEnd := time.Now().Unix()
	// Start the querying
	wg := sync.WaitGroup{}
	// Prepare the select statements for each db read connection.
	query_statements := make([]*sql.Stmt, number_of_db_connections)
	for i := 0 ; i < number_of_db_connections ; i ++{
		query_statements[i], _ = db_connections[i].Prepare("select b_code from BC where c_code = ? ")
	}
	// Start querying in each go-routine, each go-routine query `total` times.
	for i := 0 ; i < number_of_go_routines ; i ++ {
		wg.Add(1)
		go func(i int, query_statement *sql.Stmt) {
			query_start := time.Now().Unix()
			var count int64 = 0
			fmt.Printf("Starting go-routine %d\n", i)
			if err != nil {
				fmt.Println("select err %q", err)
			}
			//bc := new(BCCode)
			var b string
			for j := 0; j < total; j++ {
				//var rowCount int
				err := query_statement.QueryRow(fmt.Sprintf("C%024d", j)).Scan(&b)
				if err != nil {
					fmt.Printf("query err %q", err)
					os.Exit(-1)
				}



				// Comment out the outputs, as it slows down the code.
				//fmt.Println("BCode=", bc.B_Code, "\tCCode=", bc.C_Code, "\tCodeType=", bc.CodeType, "\tIsNew=", bc.IsNew)
				count++
			}
			readEnd := time.Now().Unix()
			fmt.Println("go-routine: ", i , "insert span=", (insertEnd - start),
				"read span=", (readEnd - query_start),
				"avg read=", float64(readEnd-query_start)*1000/float64(count))
			wg.Done()
		}(i, query_statements[i % number_of_db_connections])
	}
	wg.Wait()
	fmt.Println("All finished.")
}