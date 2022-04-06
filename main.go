package main

import (
	"database/sql"
	"fmt"
	"github.com/3th1nk/cidr"
	_ "github.com/mattn/go-sqlite3"
	"math"
	"math/big"
	"net"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"
)

var stub = make([]byte, 1024 * 1024 * 1024 * 4)

func main(){
	destination_host_ip := "123.223.32.4"
	destination_host_ip_int := (IP6toInt(net.ParseIP(destination_host_ip)).Int64())
	// allow the program to use all cores.
	runtime.GOMAXPROCS(runtime.NumCPU())
	// allocate memory for the program.
	stub[0] = 1
	type Row struct {
		RowKey int64
		RowValue int64
	}

	vni_slice := make([]int, 0)

	// add a default VNI to this slice, could add more in the future.
	vni_slice = append(vni_slice, 1)

	// Create a CIDR for all VPCs
	cidr_pointer, _ := cidr.ParseCIDR("10.0.0.0/8")

	ip_slice := make([]int, 0)

	if cidr_pointer.IsIPv4(){
		cidr_pointer.ForEachIP(func(ip string) error {
			ip_slice = append(ip_slice, int(IP6toInt(net.ParseIP(ip)).Int64()))
			return nil
		})
	}else if cidr_pointer.IsIPv6(){
		// TODO: Implement IPV6 function
	}else{
		panic("This CIDR is neither v4 nor v6, existing")
	}



	fmt.Printf("There are %v IPs in this CIDR %s\n", len(ip_slice), cidr_pointer.CIDR())

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
	//Create the table, INTEGER key and INTEGER value
	sqlStmt := `create table IF NOT EXISTS BC (RowKey INTEGER not null primary key, RowValue INTEGER not null);`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		fmt.Printf("create table error->%q: %s\n", err, sqlStmt)
		return
	}
	//Create index on column
	_, err = db.Exec("CREATE INDEX index_row_key ON BC(RowKey);")
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
	stmt, err := tx.Prepare("insert into BC(RowKey, RowValue) values(?,?)")
	if err != nil {
		fmt.Printf("insert err %q\n", err)
	}
	db.Close()
	defer stmt.Close()
	var m int = len(ip_slice)//1000 * 1000
	var total int = 1 * m
	for i := 0; i < total; i++ {
		_, err = stmt.Exec(ip_slice[i], destination_host_ip_int)
		if err != nil {
			fmt.Printf("Insert error: %q", err)
		}
	}
	tx.Commit()
	insertEnd := time.Now().Unix()
	// Start the querying
	wg := sync.WaitGroup{}
	// Prepare the select statements for each db read connection.
	query_statements := make([]*sql.Stmt, number_of_db_connections)
	for i := 0 ; i < number_of_db_connections ; i ++{
		query_statements[i], _ = db_connections[i].Prepare("select RowValue from BC where RowKey = ? ")
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
			var b int64
			for j := 0; j < total; j++ {
				//var rowCount int
				err := query_statement.QueryRow(ip_slice[i]).Scan(&b)
				if err != nil {
					fmt.Printf("query err %q", err)
					os.Exit(-1)
				}
				/* Comment out the converting code for testing purpose.
				c := net.IP(big.NewInt(b).Bytes())
				// Comment out the outputs, as it slows down the code.
				b0 := strconv.FormatInt((b>>24)&0xff, 10)
				b1 := strconv.FormatInt((b>>16)&0xff, 10)
				b2 := strconv.FormatInt((b>>8)&0xff, 10)
				b3 := strconv.FormatInt((b & 0xff), 10)
				fmt.Printf("%v.%v.%v.%v\n", b0, b1, b2, b3 )
				fmt.Printf("Raw: %v, IP Address: %v, v4: %v, v6: %v\n",b , c.String(), c.To4().String(), c.To16().String())
				*/
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

//Copied from https://go.dev/play/p/JVJuTsxXR-
func padding(n int32) int32 {
	var p int32 = 1
	for p < n {
		p *= 10
	}
	return p
}

//func ip2Long(ip string) uint32 {
//	var long uint32
//	binary.Read(bytes.NewBuffer(net.ParseIP(ip).To4()), binary.BigEndian, &long)
//	return long
//}


func IP6toInt(IPv6Address net.IP) *big.Int {
	IPv6Int := big.NewInt(0)

	// from http://golang.org/pkg/net/#pkg-constants
	// IPv6len = 16
	IPv6Int.SetBytes(IPv6Address.To16())
	return IPv6Int
}

func backtoIP4(ipInt int64) string {
	// need to do two bit shifting and “0xff” masking
	b0 := strconv.FormatInt((ipInt>>24)&0xff, 10)
	b1 := strconv.FormatInt((ipInt>>16)&0xff, 10)
	b2 := strconv.FormatInt((ipInt>>8)&0xff, 10)
	b3 := strconv.FormatInt((ipInt & 0xff), 10)
	return b0 + "." + b1 + "." + b2 + "." + b3
}