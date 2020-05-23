// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package service

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/pprof"
	"strings"

	"github.com/gorilla/mux"
	"github.com/shell909090/influx-proxy/backend"
)

type HttpService struct {
	db string
	ic *backend.InfluxCluster
}

func NewHttpService(ic *backend.InfluxCluster, db string) (hs *HttpService) {
	hs = &HttpService{
		db: db,
		ic: ic,
	}
	if hs.db != "" {
		log.Print("http database: ", hs.db)
	}
	return
}

func (hs *HttpService) Register(mux *mux.Router) {
	mux.HandleFunc("/ping", hs.HandlerPing).Methods("GET")
	mux.HandleFunc("/query", hs.HandlerQuery).Methods("POST")
	mux.HandleFunc("/write", hs.HandlerWrite).Methods("POST")

	mux.HandleFunc("/reload", hs.HandlerReload).Methods("POST")
	mux.HandleFunc("/config/backends", hs.GetConfigBackend).Methods("GET")
	mux.HandleFunc("/config/backends", hs.PostConfigBackend).Methods("POST")
	mux.HandleFunc("/config/backends/{key}", hs.DeleteConfigBackend).Methods("DELETE")
	mux.HandleFunc("/config/measurements", hs.GetConfigMeasurements).Methods("GET")
	mux.HandleFunc("/config/measurements", hs.PostConfigMeasurements).Methods("POST")
	mux.HandleFunc("/config/measurements/{key}", hs.DeleteConfigMeasurements).Methods("DELETE")

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
}

func (hs *HttpService) GetConfigBackend(w http.ResponseWriter, req *http.Request) {
	backends, err := hs.ic.Cfgsrc.LoadBackends()
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error()))
		return
	}

	data, err := json.Marshal(backends)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(200)
	w.Write(data)
}

func (hs *HttpService) PostConfigBackend(w http.ResponseWriter, req *http.Request) {
	var backend map[string]backend.BackendConfig
	body, err := ioutil.ReadAll(io.LimitReader(req.Body, 1048576))
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte("post backend too large."))
		return
	}
	if err := req.Body.Close(); err != nil {
		panic(err)
	}
	if err := json.Unmarshal(body, &backend); err != nil {
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(422) // unprocessable entity
		if err := json.NewEncoder(w).Encode(err); err != nil {
			w.Write([]byte("post backend parse json error."))
			return
		}
	}

	if err := hs.ic.Cfgsrc.UpdateBackend(backend); err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(200)
	w.Write([]byte("update success"))
}

func (hs *HttpService) DeleteConfigBackend(w http.ResponseWriter, req *http.Request) {
	backend, ok := mux.Vars(req)["key"]
	if !ok {
		w.WriteHeader(404)
		w.Write([]byte("backend not empty."))
		return
	}

	if err := hs.ic.Cfgsrc.DeleteBackend(backend); err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(200)
	w.Write([]byte("delete success"))
}

func (hs *HttpService) GetConfigMeasurements(w http.ResponseWriter, req *http.Request) {
	measurements, err := hs.ic.Cfgsrc.LoadMeasurements()
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error()))
		return
	}

	data, err := json.Marshal(measurements)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(200)
	w.Write(data)
}

func (hs *HttpService) PostConfigMeasurements(w http.ResponseWriter, req *http.Request) {
	var measurements map[string][]string
	body, err := ioutil.ReadAll(io.LimitReader(req.Body, 1048576))
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte("post measurement too large."))
		return
	}
	if err := req.Body.Close(); err != nil {
		panic(err)
	}
	if err := json.Unmarshal(body, &measurements); err != nil {
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(422) // unprocessable entity
		if err := json.NewEncoder(w).Encode(err); err != nil {
			w.Write([]byte("post measurement parse json error."))
			return
		}
	}

	if err := hs.ic.Cfgsrc.UpdateMeasurement(measurements); err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(200)
	w.Write([]byte("update success"))
}

func (hs *HttpService) DeleteConfigMeasurements(w http.ResponseWriter, req *http.Request) {
	measurement, ok := mux.Vars(req)["key"]
	if !ok {
		w.WriteHeader(404)
		w.Write([]byte("measurement not empty."))
		return
	}

	if err := hs.ic.Cfgsrc.DeleteMeasurement(measurement); err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}

	w.WriteHeader(200)
	w.Write([]byte("delete success"))
}

func (hs *HttpService) HandlerReload(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	w.Header().Add("X-Influxdb-Version", backend.VERSION)

	if err := hs.ic.LoadConfig(); err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error()))
		return
	}

	w.WriteHeader(204)
	return
}

func (hs *HttpService) HandlerPing(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	version, err := hs.ic.Ping()
	if err != nil {
		panic("WTF")
		return
	}
	w.Header().Add("X-Influxdb-Version", version)
	w.WriteHeader(204)
	return
}

func (hs *HttpService) HandlerQuery(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	w.Header().Add("X-Influxdb-Version", backend.VERSION)

	db := req.FormValue("db")
	if hs.db != "" {
		if db != hs.db {
			w.WriteHeader(404)
			w.Write([]byte("database not exist."))
			return
		}
	}

	q := strings.TrimSpace(req.FormValue("q"))
	if err := hs.ic.Query(w, req); err != nil {
		log.Printf("query error: %s,the query is %s,the client is %s\n", err, q, req.RemoteAddr)
		return
	}
	if hs.ic.QueryTracing != 0 {
		log.Printf("the query is %s,the client is %s\n", q, req.RemoteAddr)
	}

	return
}

func (hs *HttpService) HandlerWrite(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	w.Header().Add("X-Influxdb-Version", backend.VERSION)

	if req.Method != "POST" {
		w.WriteHeader(405)
		w.Write([]byte("method not allow."))
		return
	}

	db := req.URL.Query().Get("db")

	if hs.db != "" {
		if db != hs.db {
			w.WriteHeader(404)
			w.Write([]byte("database not exist."))
			return
		}
	}

	body := req.Body
	if req.Header.Get("Content-Encoding") == "gzip" {
		b, err := gzip.NewReader(req.Body)
		if err != nil {
			w.WriteHeader(400)
			w.Write([]byte("unable to decode gzip body"))
			return
		}
		defer b.Close()
		body = b
	}

	p, err := ioutil.ReadAll(body)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error()))
		return
	}

	err = hs.ic.Write(p)
	if err == nil {
		w.WriteHeader(204)
	}
	if hs.ic.WriteTracing != 0 {
		log.Printf("Write body received by handler: %s,the client is %s\n", p, req.RemoteAddr)
	}
	return
}
