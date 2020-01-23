package main

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type GoogleMatrix struct {
	DistanceMeters  float64
	DurationSeconds int
	TimeSlot        string
	OriginHex       string
	DestinationHex  string
}

type SqlServerObj struct {
	Server   string
	Port     int
	Username string
	Password string
	Database string
}

var db *sql.DB

func init() {
	var err error
	sqlServer := SqlServerObj{
		Server:   "test",
		Port:     3306,
		Username: "test",
		Password: "test",
		Database: "test",
	}
	connString := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		sqlServer.Username, sqlServer.Password, sqlServer.Server, sqlServer.Port, sqlServer.Database)
	// Create connection pool
	db, err = sql.Open("mysql", connString)
	if err != nil {
		fmt.Println("Error creating connection pool: " + err.Error())
	}
	// Connect to mssql server
	sqlServer.connect()
}

func (m *SqlServerObj) connect() {

	var err error
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	if err != nil {
		fmt.Println("Error creating connection pool: " + err.Error())
	}
	ctx := context.Background()
	err = db.PingContext(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
}

func Ping() bool {
	err := db.Ping()
	if err != nil {
		return false
	}
	return true
}

func main() {

	var gMatrixList []GoogleMatrix
	var rows *sql.Rows
	var err error
	// Query the DB
	rows, err = db.Query("SELECT DistanceMeters, DurationSeconds, TimeSlot, OriginHex, DestinationHex FROM cost WHERE (FetchHistoryId = 26799) OR (FetchHistoryId = 26798) LIMIT 10000000")
	if err != nil {
		fmt.Println("Error in getting cost data %s", err)

	}

	defer rows.Close()
	for rows.Next() {
		var g GoogleMatrix
		err := rows.Scan(&g.DistanceMeters, &g.DurationSeconds, &g.TimeSlot, &g.OriginHex, &g.DestinationHex)
		if err != nil {
			fmt.Println(err.Error())
		}
		gMatrixList = append(gMatrixList, g)
	}
	fmt.Println("GmatrixList:", len(gMatrixList))

	file, err := os.OpenFile("redisData.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		log.Fatalf("failed creating file: %s", err)
	}

	datawriter := bufio.NewWriter(file)

	for _, data := range gMatrixList {
		str := ""
		// str = "[{\"DistanceMeters\":" + data.DistanceMeters + ",\"DurationSeconds\":" + data.DurationSeconds + ",\"TimeSlot\": \"" + data.TimeSlot + "\"}]"
		str = fmt.Sprintf("SET redisL-%s_%s '[{\"DistanceMeters\":%.5f,\"DurationSeconds\":%d,\"TimeSlot\":\"%s\"}]'",
			data.OriginHex, data.DestinationHex, data.DistanceMeters, data.DurationSeconds, data.TimeSlot)
		_, _ = datawriter.WriteString(str + "\r\n")
	}

	datawriter.Flush()
	file.Close()

}
