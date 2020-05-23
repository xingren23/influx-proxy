// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/gorilla/mux"

	"github.com/shell909090/influx-proxy/backend"
	"github.com/shell909090/influx-proxy/service"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
	redis "gopkg.in/redis.v5"


)

var (
	ErrConfig   = errors.New("config parse error")
	ConfigFile  string
	NodeName    string
	RedisAddr   string
	RedisPwd    string
	RedisDb     int
	LogFilePath string
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)

	flag.StringVar(&LogFilePath, "log-file-path", "", "output file")
	flag.StringVar(&ConfigFile, "config", "", "config file")
	flag.StringVar(&NodeName, "node", "l1", "node name")
	flag.StringVar(&RedisAddr, "redis", "localhost:6379", "config file")
	flag.StringVar(&RedisPwd, "redis-pwd", "", "config file")
	flag.IntVar(&RedisDb, "redis-db", 0, "config file")
	flag.Parse()
}

type Config struct {
	redis.Options
	Node string
}

func LoadJson(configfile string, cfg interface{}) (err error) {
	file, err := os.Open(configfile)
	if err != nil {
		return
	}
	defer file.Close()

	dec := json.NewDecoder(file)
	err = dec.Decode(&cfg)
	return
}

func initLog() {
	if LogFilePath == "" {
		log.SetOutput(os.Stdout)
	} else {
		log.SetOutput(&lumberjack.Logger{
			Filename:   LogFilePath,
			MaxSize:    100,
			MaxBackups: 5,
			MaxAge:     7,
		})
	}
}

func main() {

	// the duration for which the server gracefully wait for existing connections to finish - e.g. 15s or 1m
	wait := time.Second * 15

	initLog()

	var err error
	var cfg Config

	if ConfigFile != "" {
		err = LoadJson(ConfigFile, &cfg)
		if err != nil {
			log.Print("load config failed: ", err)
			return
		}
		log.Printf("json loaded.")
	}

	if NodeName != "" {
		cfg.Node = NodeName
	}

	if RedisAddr != "" {
		cfg.Addr = RedisAddr
		cfg.Password = RedisPwd
		cfg.DB = RedisDb
	}

	rcs := backend.NewRedisConfigSource(&cfg.Options, cfg.Node)

	nodecfg, err := rcs.LoadNode()
	if err != nil {
		log.Printf("config source load failed.")
		return
	}

	ic := backend.NewInfluxCluster(rcs, &nodecfg)
	ic.LoadConfig()

	// http.SeverMux 替换为 gorilla/mux，后者支持Method
	r := mux.NewRouter()
	service.NewHttpService(ic, nodecfg.DB).Register(r)

	srv := &http.Server{
		Addr:        nodecfg.ListenAddr,
		// Good practice to set timeouts to avoid Slowloris attacks.
		WriteTimeout: time.Second * time.Duration(math.Max(float64(nodecfg.WriteTimeout),3)),
		ReadTimeout:  time.Second * time.Duration(math.Max(float64(nodecfg.ReadTimeout), 3)),
		IdleTimeout:  time.Second * time.Duration(math.Max(float64(nodecfg.IdleTimeout), 3)),
		Handler: r, // Pass our instance of gorilla/mux in.
	}

	// Run our server in a goroutine so that it doesn't block.
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Println(err)
		}
	}()

	c := make(chan os.Signal, 1)
	// We'll accept graceful shutdowns when quit via SIGINT (Ctrl+C)
	// SIGKILL, SIGQUIT or SIGTERM (Ctrl+/) will not be caught.
	signal.Notify(c, os.Interrupt)

	// Block until we receive our signal.
	<-c

	// Create a deadline to wait for.
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	// Doesn't block if no connections, but will otherwise wait
	// until the timeout deadline.
	srv.Shutdown(ctx)
	// Optionally, you could run srv.Shutdown in a goroutine and block on
	// <-ctx.Done() if your application should wait for other services
	// to finalize based on context cancellation.
	log.Println("shutting down")
	os.Exit(0)

}
