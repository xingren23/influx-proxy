// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import (
	"errors"
	"log"
	"reflect"
	"strconv"
	"strings"

	"gopkg.in/redis.v5"
)

const (
	VERSION = "1.1"

	RedisDefaultNode = "influx-proxy:node:default_node"
	RedisNodes       = "influx-proxy:node:"
	RedisMeasurement = "influx-proxy:measurement:"
	RedisBackend     = "influx-proxy:backend:"
)

var (
	ErrIllegalConfig = errors.New("illegal config")
)

func LoadStructFromMap(data map[string]string, o interface{}) (err error) {
	var x int
	val := reflect.ValueOf(o).Elem()
	for i := 0; i < val.NumField(); i++ {
		valueField := val.Field(i)
		typeField := val.Type().Field(i)

		name := strings.ToLower(typeField.Name)
		s, ok := data[name]
		if !ok {
			continue
		}

		switch typeField.Type.Kind() {
		case reflect.String:
			valueField.SetString(s)
		case reflect.Int:
			x, err = strconv.Atoi(s)
			if err != nil {
				log.Printf("%s: %s", err, name)
				return
			}
			valueField.SetInt(int64(x))
		}
	}
	return
}

type NodeConfig struct {
	ListenAddr   string `json:"listenaddr"`
	DB           string `json:"db"`
	Zone         string `json:"zone"`
	Nexts        string `json:"-"`
	Interval     int    `json:"interval"`
	IdleTimeout  int    `json:"idletimeout"`
	WriteTimeout int    `json:"writetimeout"`
	ReadTimeout  int    `json:"readtimeout"`
	WriteTracing int    `json:"writetracing"`
	QueryTracing int    `json:"querytracing"`
}

type BackendConfig struct {
	URL             string `json:"url"`
	DB              string `json:"db"`
	Zone            string `json:"zone"`
	Interval        int    `json:"interval"`
	Timeout         int    `json:"timeout"`
	TimeoutQuery    int    `json:"querytimeout"`
	MaxRowLimit     int    `json:"maxrowlimit"`
	CheckInterval   int    `json:"checkinterval"`
	RewriteInterval int    `json:"rewriteinterval"`
	WriteOnly       int    `json:"writeonly"`
}

type RedisConfigSource struct {
	client *redis.Client
	node   string
	zone   string
}

func NewRedisConfigSource(options *redis.Options, node string) (rcs *RedisConfigSource) {
	rcs = &RedisConfigSource{
		client: redis.NewClient(options),
		node:   node,
	}
	return
}

func (rcs *RedisConfigSource) LoadNode() (nodecfg NodeConfig, err error) {
	val, err := rcs.client.HGetAll(RedisDefaultNode).Result()
	if err != nil {
		log.Printf("redis load error: b:%s", rcs.node)
		return
	}

	err = LoadStructFromMap(val, &nodecfg)
	if err != nil {
		log.Printf("redis load error: b:%s", rcs.node)
		return
	}

	val, err = rcs.client.HGetAll(RedisNodes + rcs.node).Result()
	if err != nil {
		log.Printf("redis load error: b:%s", rcs.node)
		return
	}

	err = LoadStructFromMap(val, &nodecfg)
	if err != nil {
		log.Printf("redis load error: b:%s", rcs.node)
		return
	}
	log.Printf("node config loaded.")
	return
}

func (rcs *RedisConfigSource) LoadBackends() (backends map[string]*BackendConfig, err error) {
	backends = make(map[string]*BackendConfig)

	names, err := rcs.client.Keys(RedisBackend + "*").Result()
	if err != nil {
		log.Printf("read redis error: %s", err)
		return
	}

	var cfg *BackendConfig
	for _, name := range names {
		name = name[len(RedisBackend):len(name)]
		cfg, err = rcs.LoadConfigFromRedis(name)
		if err != nil {
			log.Printf("read redis config error: %s", err)
			return
		}
		backends[name] = cfg
	}
	log.Printf("%d backends loaded from redis.", len(backends))
	return
}

func (rcs *RedisConfigSource) LoadConfigFromRedis(name string) (cfg *BackendConfig, err error) {
	val, err := rcs.client.HGetAll(RedisBackend + name).Result()
	if err != nil {
		log.Printf("redis load error: b:%s", name)
		return
	}

	cfg = &BackendConfig{}
	err = LoadStructFromMap(val, cfg)
	if err != nil {
		return
	}

	if cfg.Interval == 0 {
		cfg.Interval = 1000
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 10000
	}
	if cfg.TimeoutQuery == 0 {
		cfg.TimeoutQuery = 600000
	}
	if cfg.MaxRowLimit == 0 {
		cfg.MaxRowLimit = 10000
	}
	if cfg.CheckInterval == 0 {
		cfg.CheckInterval = 1000
	}
	if cfg.RewriteInterval == 0 {
		cfg.RewriteInterval = 10000
	}
	return
}

func (rcs *RedisConfigSource) LoadMeasurements() (mMap map[string][]string, err error) {
	mMap = make(map[string][]string, 0)

	names, err := rcs.client.Keys(RedisMeasurement + "*").Result()
	if err != nil {
		log.Printf("read redis error: %s", err)
		return
	}

	var length int64
	for _, key := range names {
		length, err = rcs.client.LLen(key).Result()
		if err != nil {
			return
		}
		mMap[key[len(RedisMeasurement):len(key)]], err = rcs.client.LRange(key, 0, length).Result()
		if err != nil {
			return
		}
	}
	log.Printf("%d measurements loaded from redis.", len(mMap))
	return
}

func (rcs *RedisConfigSource) DeleteMeasurement(measurement string) error {
	mKey := RedisMeasurement + measurement
	_, err := rcs.client.Del(mKey).Result()
	if err != nil {
		log.Printf("delete measurement %s error: #{err}", mKey)
	}

	log.Printf("delete measurement %s", mKey)
	return err
}

func (rcs *RedisConfigSource) DeleteBackend(backend string) error {
	mKey := RedisBackend + backend
	_, err := rcs.client.Del(mKey).Result()
	if err != nil {
		log.Printf("delete backend %s error: #{err}", mKey)
	}

	log.Printf("delete backend %s", mKey)
	return err
}

func (rcs *RedisConfigSource) UpdateMeasurement(mMap map[string][]string) error {
	for key, value := range mMap {
		mKey := RedisMeasurement + key
		for item := range value {
			_, err := rcs.client.RPush(mKey, item).Result()
			if err != nil {
				log.Printf("update measurement #{mMap} error: #{err}")
			}
		}
	}

	log.Printf("update measurement #{mMap} to redis.")
	return nil
}

func (rcs *RedisConfigSource) UpdateBackend(backends map[string]BackendConfig) error {
	for key, backend := range backends {
		bKey := RedisBackend + key
		t := reflect.TypeOf(backend)
		v := reflect.ValueOf(backend)
		for i := 0; i < v.NumField(); i++ {
			if v.Field(i).CanInterface() { //判断是否为可导出字段
				// 转为小写
				filedKey := strings.ToLower(t.Field(i).Name)
				value := v.Field(i).Interface()

				_, err := rcs.client.HSet(bKey, filedKey, value).Result()
				if err != nil {
					log.Printf("update backend #{backends} error: #{err}")
				}
			}
		}
	}
	log.Printf("update backend #{backends} to redis.")
	return nil
}
