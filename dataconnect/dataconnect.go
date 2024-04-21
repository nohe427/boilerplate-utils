// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dataconnect

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"sync"

	"cloud.google.com/go/cloudsqlconn"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

var (
	db   *sql.DB
	once sync.Once
)

type connectionDetails struct {
	dbUser                 string
	dbPwd                  string
	dbName                 string
	instanceConnectionName string
	usePrivate             string
	dbTCPHost              string
	dbPort                 string
}

func mustGetenv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("Fatal error. %s env var not set.", k)
	}
	return v
}

func connectWithConnector(cd connectionDetails) (*sql.DB, error) {
	dsn := fmt.Sprintf("user=%s password=%s database=%s", cd.dbUser, cd.dbPwd, cd.dbName)
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}

	var opts []cloudsqlconn.Option
	if cd.usePrivate != "" {
		opts = append(opts, cloudsqlconn.WithDefaultDialOptions(cloudsqlconn.WithPrivateIP()))
	}
	d, err := cloudsqlconn.NewDialer(context.Background(), opts...)
	if err != nil {
		return nil, err
	}
	// Use the Cloud SQL connector to handle connecting to the instance.
	// This approach does *NOT* require the Cloud SQL proxy.
	config.DialFunc = func(ctx context.Context, network, instance string) (net.Conn, error) {
		return d.Dial(ctx, cd.instanceConnectionName)
	}
	dbURI := stdlib.RegisterConnConfig(config)
	dbPool, err := sql.Open("pgx", dbURI)
	if err != nil {
		return nil, fmt.Errorf("sql.Open: %w", err)
	}
	return dbPool, nil
}

// connectTCPSocket initializes a TCP connection pool for a Cloud SQL
// instance of Postgres.
func connectTCPSocket(cd connectionDetails) (*sql.DB, error) {
	dbURI := fmt.Sprintf("host=%s user=%s password=%s port=%s database=%s",
		cd.dbTCPHost, cd.dbUser, cd.dbPwd, cd.dbPort, cd.dbName)

	// dbPool is the pool of database connections.
	dbPool, err := sql.Open("pgx", dbURI)
	if err != nil {
		return nil, fmt.Errorf("sql.Open: %w", err)
	}

	return dbPool, nil
}

// GetDB creates a new connection using the cloudsql connector if the
// INSTANCE_CONNECTION_NAME environment variable is set, otherwise it
// connects to the local database using TCP.
func GetDB() *sql.DB {
	var (
		dbUser                 = mustGetenv("DB_USER")                 // e.g. 'my-db-user'
		dbPwd                  = mustGetenv("DB_PASS")                 // e.g. 'my-db-password'
		dbName                 = mustGetenv("DB_NAME")                 // e.g. 'my-database'
		instanceConnectionName = os.Getenv("INSTANCE_CONNECTION_NAME") // e.g. 'project:region:instance'
		usePrivate             = os.Getenv("PRIVATE_IP")
		dbTCPHost              = os.Getenv("INSTANCE_HOST") // "127.0.0.1")
		dbPort                 = os.Getenv("DB_PORT")
	)
	cd := connectionDetails{
		dbUser:                 dbUser,
		dbPwd:                  dbPwd,
		dbName:                 dbName,
		instanceConnectionName: instanceConnectionName,
		usePrivate:             usePrivate,
		dbTCPHost:              dbTCPHost,
		dbPort:                 dbPort,
	}
	once.Do(func() {
		var localdb *sql.DB
		var err error
		if cd.instanceConnectionName != "" {
			localdb, err = connectWithConnector(cd)
		} else {
			localdb, err = connectTCPSocket(cd)
		}
		if err != nil {
			log.Fatalf("Could not connect %v", err)
		}
		db = localdb
	})
	return db
}
