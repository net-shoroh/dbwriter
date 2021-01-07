package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"database/sql"
	"github.com/lib/pq"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	DSN      = "database=devel user=devel sslmode=disable"
	DBPOOL   = 10
	LIFETIME = 0
	ROWS     = 15_000_000
)

type Data struct {
	ID   int
	Name string
}

//create table names (id integer, name varchar);

func main() {
	data := makeData(ROWS)
	fmt.Printf("rows: %d\n", ROWS)

	db, err := libPQConnect(DSN)
	if err != nil {
		log.Fatal(err)
	}

	timerPQ := time.Now()

	if err := libPQWrite(db, data); err != nil {
		log.Fatal(err)
	}

	durPQ := time.Since(timerPQ)

	fmt.Printf("libpq: %s\n", durPQ)

	db.Close()

	var prepare = false

	dbGorm, err := gormConnect(DSN, prepare)
	if err != nil {
		log.Fatal(err)
	}

	timerGORM := time.Now()

	if err := gormWrite(dbGorm, data); err != nil {
		log.Fatal(err)
	}

	durGORM := time.Since(timerGORM)
	fmt.Printf("gorm (prepare: %v) %s\n", prepare, durGORM)

	prepare = false

	dbGorm, err = gormConnect(DSN, prepare)
	if err != nil {
		log.Fatal(err)
	}

	timerGORMSlice := time.Now()

	if err := gormWriteSlice(dbGorm, data); err != nil {
		log.Fatal(err)
	}

	durGORMSlice := time.Since(timerGORMSlice)
	fmt.Printf("gorm slice (prepare: %v) %s\n", prepare, durGORMSlice)
}

func makeData(n int) (data []Data) {
	for i := 1; i <= n; i++ {
		data = append(data, Data{i, "Adam_" + strconv.Itoa(i)})
	}

	return
}

func libPQConnect(dsn string) (*sql.DB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return db, err
	}

	db.SetMaxIdleConns(DBPOOL)
	db.SetMaxOpenConns(DBPOOL)
	db.SetConnMaxLifetime(LIFETIME)

	return db, nil
}

func libPQWrite(db *sql.DB, data []Data) error {
	txn, err := db.Begin()
	if err != nil {
		return err
	}

	stmt, err := txn.Prepare(pq.CopyIn("names", "id", "name"))
	if err != nil {
		return err
	}

	for _, d := range data {
		if _, err = stmt.Exec(d.ID, d.Name); err != nil {
			return err
		}
	}

	if err := stmt.Close(); err != nil {
		return err
	}

	if err := txn.Commit(); err != nil {
		if rollErr := txn.Rollback(); rollErr != nil {
			return rollErr
		}

		return err
	}

	return nil
}

func gormConnect(dsn string, prepare bool) (*gorm.DB, error) {
	conf := &gorm.Config{
		PrepareStmt:              prepare,
		SkipDefaultTransaction:   true,
		DisableNestedTransaction: true,
		Logger: logger.New(
			log.New(os.Stdout, "\r\n", log.LstdFlags),
			logger.Config{
				LogLevel: logger.Error,
				Colorful: false,
			}),
	}

	db, err := gorm.Open(postgres.Open(dsn), conf)
	if err != nil {
		return db, err
	}

	sqlDB, err := db.DB()
	if err != nil {
		return db, err
	}

	sqlDB.SetMaxIdleConns(DBPOOL)
	sqlDB.SetMaxOpenConns(DBPOOL)
	sqlDB.SetConnMaxLifetime(LIFETIME)

	return db, nil
}

func gormWrite(db *gorm.DB, data []Data) error {
	tx := db.Begin()
	if err := tx.Error; err != nil {
		return err
	}

	query := "insert into names (id, name) values (?, ?)"

	for _, d := range data {
		if err := tx.Exec(query, d.ID, d.Name).Error; err != nil {
			return err
		}
	}

	if err := tx.Commit().Error; err != nil {
		if rollErr := tx.Rollback().Error; rollErr != nil {
			return rollErr
		}

		return err
	}

	return nil
}

func gormWriteSlice(db *gorm.DB, data []Data) error {
	tx := db.Begin()
	if err := tx.Error; err != nil {
		return err
	}

	var (
		step  = 30_000
		names []Data
	)

	for i := 0; i < len(data); i += step {
		if j := i + step; j > len(data) {
			names = data[i:]
		} else {
			names = data[i:j]
		}

		if err := tx.Table("names").Create(&names).Error; err != nil {
			return err
		}
	}

	if err := tx.Commit().Error; err != nil {
		if rollErr := tx.Rollback().Error; rollErr != nil {
			return rollErr
		}

		return err
	}

	return nil
}
