package main

import (
	"database/sql"
	"fmt"
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

	//SQLite in memory，小心，不能只写:memory:,这样每一次连接都会申请内存
	db, err := sql.Open("sqlite3", "file:./rio_testing.db?cache=shared&loc=auto&_journal_mode=wal")
	db.SetMaxOpenConns(runtime.NumCPU())
	if err != nil {
		fmt.Println("SQLite:", err)
	}
	defer db.Close()
	fmt.Println("SQLite start")
	//创建表//delete from BC;，SQLite字段类型比较少，bool型可以用INTEGER，字符串用TEXT
	sqlStmt := `create table BC (b_code text not null primary key, c_code text not null, code_type INTEGER, is_new INTEGER);`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		fmt.Println("create table error->%q: %s\n", err, sqlStmt)
		return
	}
	//创建索引，有索引和没索引性能差别巨大，根本就不是一个量级，有兴趣的可以去掉试试
	_, err = db.Exec("CREATE INDEX inx_c_code ON BC(c_code);")
	if err != nil {
		fmt.Println("create index error->%q: %s\n", err, sqlStmt)
		return
	}
	//写入10M条记录
	start := time.Now().Unix()
	tx, err := db.Begin()
	if err != nil {
		fmt.Println("%q", err)
	}
	stmt, err := tx.Prepare("insert into BC(b_code, c_code, code_type, is_new ) values(?,?,?,?)")
	if err != nil {
		fmt.Println("insert err %q", err)
	}
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
	//随机检索10M次
	wg := sync.WaitGroup{}
	for i := 0 ; i < 1; i ++ {
		wg.Add(1)
		go func(i int) {
			var count int64 = 0
			fmt.Printf("Starting go-routine %d\n", i)
			stmt, err = db.Prepare("select b_code, c_code, code_type, is_new from BC where c_code = ? ")
			if err != nil {
				fmt.Println("select err %q", err)
			}
			defer stmt.Close()
			bc := new(BCCode)
			for i := 0; i < total; i++ {
				err := stmt.QueryRow(fmt.Sprintf("C%024d", i)).Scan(&bc.B_Code, &bc.C_Code, &bc.CodeType, &bc.IsNew)
				if err != nil {
					fmt.Printf("query err %q", err)
					os.Exit(-1)
				}

				//屏幕输出会花掉好多时间啊，计算耗时的时候还是关掉比较好
				//fmt.Println("BCode=", bc.B_Code, "\tCCode=", bc.C_Code, "\tCodeType=", bc.CodeType, "\tIsNew=", bc.IsNew)
				count++
			}
			readEnd := time.Now().Unix()
			fmt.Println("go-routine: ", i , "insert span=", (insertEnd - start),
				"read span=", (readEnd - insertEnd),
				"avg read=", float64(readEnd-insertEnd)*1000/float64(count))
			wg.Done()
		}(i)
	}
	wg.Wait()
	fmt.Println("All finished.")

}
