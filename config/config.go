package config

import (
	clientv3 "go.etcd.io/etcd/client/v3"
	"gopkg.in/yaml.v3"
	"log"
	"os"
	"time"
)

type Config struct {
	Etcd           Etcd           `yaml:"etcd"`
	Runtime        *Runtime       `yaml:"runtime"`
	FileConnectors FileConnectors `yaml:"file-connector"`
	HttpConnectors HttpConnectors `yaml:"http-connector"`
	Worker         Worker         `yaml:"worker"`
	Coordinator    Coordinator    `yaml:"coordinator"`
}

type Runtime struct {
	Catalog        string `yaml:"catalog"`
	Schema         string `yaml:"schema"`
	Table          string `yaml:"table"`
	ParallelNumber int    `yaml:"parallel-number"`
}

type Etcd struct {
	Endpoint             []string `yaml:"endpoint"`
	DialTimeout          int      `yaml:"dial-timeout"`
	DialKeepAliveTimeout int      `yaml:"dial-keepalive-timeout"`
	Username             string   `yaml:"username"`
	Password             string   `yaml:"password"`
}

type Worker struct {
	IP      string `yaml:"ip"`
	TCPPort int    `yaml:"tcp-port"`
	RPCPort int    `yaml:"rpc-port"`
}

type Coordinator struct {
	HttpPort int `yaml:"http-port"`
}

var Conf Config

func Load(fileName string) error {
	var data []byte
	var err error
	if data, err = os.ReadFile(fileName); err != nil {
		log.Fatalf("fail to load the configure file, due to %v ", err.Error())
		return err
	}

	if err = yaml.Unmarshal(data, &Conf); err != nil {
		log.Fatalf("fail to unmarshal the configure file, due to %v", err.Error())
		return err
	}

	if err = Conf.FileConnectors.Check(); err != nil {
		log.Fatalf("%v", err)
		return err
	}

	if err = Conf.HttpConnectors.Check(); err != nil {
		log.Fatalf("%v", err)
		return err
	}

	return nil
}

func NewEtcd() clientv3.Config {
	return clientv3.Config{
		Endpoints:            Conf.Etcd.Endpoint,
		DialTimeout:          time.Duration(Conf.Etcd.DialTimeout) * time.Second,
		DialKeepAliveTimeout: time.Duration(Conf.Etcd.DialKeepAliveTimeout) * time.Second,
		Username:             Conf.Etcd.Username,
		Password:             Conf.Etcd.Password,
	}
}

func NewRuntime() *Runtime {
	return &Runtime{
		Catalog:        "default",
		Schema:         "default",
		ParallelNumber: 4,
	}
}

func WildcardMatch(s, p string) bool {
	ls, lp := len(s), len(p)
	dp := make([][]bool, ls+1)
	for i := 0; i < ls+1; i++ {
		dp[i] = make([]bool, lp+1)
	}
	dp[0][0] = true
	for i := 1; i <= lp; i++ {
		if p[i-1] == '*' {
			dp[0][i] = dp[0][i-1]
		}
	}

	for i := 1; i <= ls; i++ {
		for j := 1; j <= lp; j++ {
			if p[j-1] == '*' {
				dp[i][j] = dp[i-1][j] || dp[i][j-1]
			} else {
				if p[j-1] == s[i-1] || p[j-1] == '?' {
					dp[i][j] = dp[i-1][j-1]
				}
			}
		}
	}
	return dp[ls][lp]
}
