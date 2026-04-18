package config

import (
	"os"

	"github.com/rs/zerolog"
	"gopkg.in/yaml.v2"
)

type Config struct {
	ID        string
	ServerIDS map[string]string
	DBDir     string
	LogLevel  string
	BaseURL   string
	Port      string
}

type PeerClient struct {
	ID     string `yaml:"id"`
	RPCUrl string `yaml:"rpc_url"`
}

type PeerClientInfo struct {
	PeerClients []PeerClient `yaml:"peers"`
}

func (c *Config) LoadConfig() {
	c.ID = os.Getenv("ID")
	c.DBDir = os.Getenv("DB_DIR")
	c.LogLevel = os.Getenv("LOG_LEVEL")
	c.BaseURL = os.Getenv("BASE_URL")
	c.Port = os.Getenv("PORT")

	// Load peer info from yaml file
	peerInfoFile := os.Getenv("PEER_INFO")
	f, err := os.ReadFile(peerInfoFile)
	if err != nil {
		panic("error reading peer info file: " + err.Error())
	}

	var peerInfo PeerClientInfo
	err = yaml.Unmarshal(f, &peerInfo)
	if err != nil {
		panic("error unmarshalling peer info: " + err.Error())
	}

	c.ServerIDS = make(map[string]string)
	for _, peer := range peerInfo.PeerClients {
		if peer.ID == c.ID {
			continue
		}
		c.ServerIDS[peer.ID] = peer.RPCUrl
	}
}

func GetLogLevel(level string) zerolog.Level {
	switch level {
	case "info":
		return zerolog.InfoLevel
	case "debug":
		return zerolog.DebugLevel
	case "warn":
		return zerolog.WarnLevel
	case "error":
		return zerolog.ErrorLevel
	case "fatal":
		return zerolog.FatalLevel
	case "panic":
		return zerolog.PanicLevel
	case "disable":
		return zerolog.Disabled
	default:
		return zerolog.DebugLevel
	}
}
