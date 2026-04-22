package config

import (
	"fmt"
	"os"
	"strconv"
	"sync"

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
	DebugPort string

	RPCTimeoutMs       int
	HeartbeatMs        int
	ElectionMinMs      int
	ElectionMaxMs      int
	ElectionDurationMs int
}

type PeerClient struct {
	ID     string `yaml:"id"`
	RPCUrl string `yaml:"rpc_url"`
}

type PeerClientInfo struct {
	PeerClients []PeerClient `yaml:"peers"`
}

var (
	instance *Config
	once     sync.Once
)

// GetConfig returns the singleton Config instance.
// LoadConfig must be called before GetConfig, otherwise it panics.
func GetConfig() *Config {
	if instance == nil {
		panic("config not initialized: call LoadConfig first")
	}
	return instance
}

// LoadConfig initializes the singleton Config from environment variables.
// Safe to call multiple times — only the first call has effect.
func LoadConfig() *Config {
	once.Do(func() {
		c := &Config{}

		c.ID = os.Getenv("ID")
		c.DBDir = os.Getenv("DB_DIR")
		c.LogLevel = os.Getenv("LOG_LEVEL")
		c.BaseURL = os.Getenv("BASE_URL")
		c.Port = os.Getenv("PORT")
		c.DebugPort = os.Getenv("DEBUG_PORT")

		c.RPCTimeoutMs = getEnvInt("RPC_TIMEOUT_MS", 150)
		c.HeartbeatMs = getEnvInt("HEARTBEAT_MS", 200)
		c.ElectionMinMs = getEnvInt("ELECTION_MIN_MS", 1500)
		c.ElectionMaxMs = getEnvInt("ELECTION_MAX_MS", 2000)
		c.ElectionDurationMs = c.ElectionMaxMs - c.ElectionMinMs

		// Validate timing relationships
		// RPCTimeout < HeartbeatMs < ElectionMinMs is required for Raft correctness
		// If RPC takes longer than heartbeat interval, goroutines pile up
		// If heartbeat >= election timeout, followers always time out before receiving a heartbeat
		if c.RPCTimeoutMs >= c.HeartbeatMs {
			panic(fmt.Sprintf("invalid config: RPC_TIMEOUT_MS (%d) must be less than HEARTBEAT_MS (%d)", c.RPCTimeoutMs, c.HeartbeatMs))
		}

		if c.HeartbeatMs >= c.ElectionMinMs {
			panic(fmt.Sprintf("invalid config: HEARTBEAT_MS (%d) must be less than ELECTION_MIN_MS (%d)", c.HeartbeatMs, c.ElectionMinMs))
		}

		if c.ElectionMinMs >= c.ElectionMaxMs {
			panic(fmt.Sprintf("invalid config: ELECTION_MIN_MS (%d) must be less than ELECTION_MAX_MS (%d)", c.ElectionMinMs, c.ElectionMaxMs))
		}

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

		instance = c
	})

	fmt.Printf("\n-------------------------------\nConfig: %+v\n-------------------------------\n", instance)

	return instance
}

func getEnvInt(key string, defaultVal int) int {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	n, err := strconv.Atoi(val)
	if err != nil {
		return defaultVal
	}
	return n
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
