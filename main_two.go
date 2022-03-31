package main

import (
	"database/sql"
	"fmt"
	"math"
	"math/rand"

	//"math/rand"
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
		//B_Code string
		//C_Code string
		IndexedRow int // index this
		NonIndexedRow int    // not index this
	}
	number_of_go_routines := 20
	//SQLite in memory，小心，不能只写:memory:,这样每一次连接都会申请内存
	db, err := sql.Open("sqlite3", "file:./rio_testing.db?loc=auto&_journal_mode=wal&_mutex=no")

	// one DB connection can be used by two go-routines
	number_of_db_connections := int(math.Ceil(float64(number_of_go_routines) / 2.0))

	db_connections := make([]*sql.DB, number_of_db_connections)

	for i := 0 ; i < number_of_db_connections ; i ++{
		db_connections[i],_ = sql.Open("sqlite3", "file:./rio_testing.db?loc=auto&_journal_mode=wal&_mutex=no")
	}

	db.SetMaxOpenConns(runtime.NumCPU())
	if err != nil {
		fmt.Println("SQLite:", err)
	}
	defer db.Close()
	fmt.Println("SQLite start")
	//创建表//delete from BC;，SQLite字段类型比较少，bool型可以用INTEGER，字符串用TEXT
//	sqlStmt := `
//
//CREATE TABLE perfTest ( indexedColumn, nonIndexedColumn );
//
//WITH RECURSIVE
//    randdata(x, y) AS (
//        SELECT RANDOM(), RANDOM()
//            UNION ALL
//        SELECT RANDOM(), RANDOM() FROM randdata
//        LIMIT 1000*1000*1
//    )
//
//INSERT INTO perfTest ( indexedColumn, nonIndexedColumn )
//    SELECT * FROM randdata;
//
//CREATE INDEX perfTestIndexedColumn ON perfTest ( indexedColumn );`
//	_, err = db.Exec(sqlStmt)
//	if err != nil {
//		fmt.Println("create table error->%q: %s\n", err, sqlStmt)
//		return
//	}
	//创建索引，有索引和没索引性能差别巨大，根本就不是一个量级，有兴趣的可以去掉试试
	//_, err = db.Exec("CREATE INDEX index_code_type ON BC(code_type);")
	//if err != nil {
	//	fmt.Println("create index error->%q: %s\n", err, sqlStmt)
	//	return
	//}
	//写入10M条记录
	start := time.Now().Unix()
	//tx, err := db.Begin()
	if err != nil {
		fmt.Println("%q", err)
	}
	//stmt, err := tx.Prepare("insert into BC(code_type, is_new ) values(RANDOM(), RANDOM())")
	if err != nil {
		fmt.Println("insert err %q", err)
	}
	//defer stmt.Close()
	var m int = 1000 * 1000
	var total int = 1 * m
	//for i := 0; i < total; i++ {
	//	//rand.Seed(time.Now().UnixNano())
	//	_, err = stmt.Exec()
	//	if err != nil {
	//		fmt.Println("%q", err)
	//	}
	//}
	//tx.Commit()
	insertEnd := time.Now().Unix()
	//随机检索10M次
	wg := sync.WaitGroup{}
	//query_statement, err := db.Prepare("select b_code, c_code, code_type, is_new from BC where c_code = ? ")
	query_statements := make([]*sql.Stmt, number_of_db_connections)
	for i := 0 ; i < number_of_db_connections ; i ++{
		query_statements[i], _ = db_connections[i].Prepare(" SELECT indexedColumn FROM  perfTest where indexedColumn = ?")
	}
	//defer query_statement.Close()
	for i := 0 ; i < number_of_go_routines ; i ++ {
		wg.Add(1)
		go func(i int, query_statement *sql.Stmt) {
			//c_code_slice := make([]int, total)
			//for k := 0 ; k < total ; k ++ {
			//	//rand.Seed(time.Now().UnixNano())
			//	c_code_slice[k] = -8296290926683469535
			//}
			query_start  := time.Now().Unix()
			var count int64 = 0
			fmt.Printf("Starting go-routine %d\n", i)
			if err != nil {
				fmt.Println("select err %q", err)
			}
			//bc := new(BCCode)
			for j := 0; j < total; j++ {
				query_statement.QueryRow(-8296290926683469535)//.Scan(&bc.IndexedRow, &bc.NonIndexedRow)
				//if err != nil {
				//	fmt.Printf("query err %q", err)
				//	os.Exit(-1)
				//}

				//屏幕输出会花掉好多时间啊，计算耗时的时候还是关掉比较好
				//fmt.Println("\tNonIndexedRow=", bc.NonIndexedRow, "\tIndexedRow=", bc.IndexedRow)
				count++
				if count % 10000 == 0 {
					fmt.Println("Count: ", count)
				}
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
