package config

import (
	"os"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

type Config struct {
	Port int `env:"PORT"`

	ElectionTimeoutStart uint64 `env:"ELECTION_TIMEOUT_START"`
	ElectionTimeoutEnd   uint64 `env:"ELECTION_TIMEOUT_END"`

	ID    string            `env:"ID"`
	Nodes map[string]string `env:"NODES"` // nodeID: url

	HeartbeatTimeout uint64 `env:"HEARTBEAT_TIMEOUT"`
}

func NewConfig() (*Config, error) {
	env := &Config{}
	if err := env.load(); err != nil {
		return nil, err
	}

	return env, nil
}

func (e *Config) load() error {
	if err := godotenv.Load(); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := env.Parse(e); err != nil {
		return err
	}

	return nil
}
