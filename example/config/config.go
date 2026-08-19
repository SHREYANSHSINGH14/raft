package config

import (
	"fmt"
	"os"
	"strconv"

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

	SnapshotDir       string
	SnapshotInterval  uint // in seconds
	SnapshotThreshold uint // in number of log entries

	InstallSnapshotBaseMs                int
	InstallSnapshotDeadlineScaleSizeByte int
	InstallSnapshotDeadlineScaleTimeMs   int
}

type PeerClient struct {
	ID     string `yaml:"id"`
	RPCUrl string `yaml:"rpc_url"`
}

type PeerClientInfo struct {
	PeerClients []PeerClient `yaml:"peers"`
}

// LoadConfig reads configuration from environment variables and returns it.
// Each call reads the environment fresh — there is no global state.
func LoadConfig() *Config {
	c := &Config{}
	c.ID = os.Getenv("ID")
	c.DBDir = os.Getenv("DB_DIR")
	c.LogLevel = os.Getenv("LOG_LEVEL")
	c.BaseURL = os.Getenv("BASE_URL")
	c.Port = os.Getenv("PORT")
	c.DebugPort = os.Getenv("DEBUG_PORT")

	c.RPCTimeoutMs = getEnvInt("RPC_TIMEOUT_MS", 50)
	c.HeartbeatMs = getEnvInt("HEARTBEAT_MS", 100)
	c.ElectionMinMs = getEnvInt("ELECTION_MIN_MS", 1000)
	c.ElectionMaxMs = getEnvInt("ELECTION_MAX_MS", 5000)
	c.ElectionDurationMs = c.ElectionMaxMs - c.ElectionMinMs

	c.SnapshotDir = os.Getenv("SNAPSHOT_DIR")
	c.SnapshotInterval = uint(getEnvInt("SNAPSHOT_INTERVAL_S", 300))
	c.SnapshotThreshold = uint(getEnvInt("SNAPSHOT_THRESHOLD", 1000))

	// InstallSnapshot RPC deadline scales with snapshot size: allow
	// InstallSnapshotDeadlineScaleTime ms per InstallSnapshotDeadlineScaleSize bytes.
	// Nothing consumes these yet — the leader-side send path doesn't exist.
	// The base is deliberately not RPC_TIMEOUT_MS: that one is validated below to sit
	// under HEARTBEAT_MS, and an InstallSnapshot's fixed cost — two fsyncs, a rename,
	// a full state machine Restore — has nothing to do with a heartbeat's budget.
	c.InstallSnapshotBaseMs = getEnvInt("INSTALL_SNAPSHOT_BASE_MS", 5000)
	// 1s per MB on top of the base: slow enough not to false-fail on a loaded disk,
	// fast enough that a wedged transfer gives up in seconds rather than minutes.
	c.InstallSnapshotDeadlineScaleSizeByte = getEnvInt("INSTALL_SNAPSHOT_DEADLINE_SCALE_SIZE_BYTE", 1024*1024)
	c.InstallSnapshotDeadlineScaleTimeMs = getEnvInt("INSTALL_SNAPSHOT_DEADLINE_SCALE_TIME_MS", 1000)

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

	c.ServerIDS = make(map[string]string)

	// PEER_INFO is optional. An absent or empty value means "no peers", which
	// is valid for tests and single-node setups. A non-empty value that points
	// to a missing or malformed file is still treated as a hard error.
	peerInfoFile := os.Getenv("PEER_INFO")
	if peerInfoFile != "" {
		f, err := os.ReadFile(peerInfoFile)
		if err != nil {
			panic("error reading peer info file: " + err.Error())
		}

		var peerInfo PeerClientInfo
		if err = yaml.Unmarshal(f, &peerInfo); err != nil {
			panic("error unmarshalling peer info: " + err.Error())
		}

		for _, peer := range peerInfo.PeerClients {
			if peer.ID == c.ID {
				continue
			}
			c.ServerIDS[peer.ID] = peer.RPCUrl
		}
	}

	fmt.Printf("\n-------------------------------\nConfig: %+v\n-------------------------------\n", c)

	return c
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
