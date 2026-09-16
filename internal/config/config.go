// Package config builds the API's immutable configuration snapshot with
// koanf (A09, DD-09 §4). Precedence is defaults < the service's reviewed
// configuration file < the allowed ANVILKIT_API_* environment overrides. The
// candidate is validated (unknown keys, required values, ranges, cross-field
// rules) before anything else starts; business code receives the validated
// value and never reads the environment or a mutable global.
package config

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

const (
	envPrefix = "ANVILKIT_API_"
	// EnvConfigFile names the configuration file; it is the only variable
	// read before the snapshot exists.
	EnvConfigFile = "ANVILKIT_API_CONFIG"
	// DefaultConfigFile is the file next to the service (config.yaml in the
	// working directory) when EnvConfigFile is unset.
	DefaultConfigFile = "config.yaml"
)

// HTTP is the public listener.
type HTTP struct {
	Listen            string        `koanf:"listen"`
	ReadHeaderTimeout time.Duration `koanf:"read_header_timeout"`
	BodyLimitBytes    int64         `koanf:"body_limit_bytes"`
	ShutdownTimeout   time.Duration `koanf:"shutdown_timeout"`
}

// Control is the gRPC dependency.
type Control struct {
	Address string `koanf:"address"`
}

// Auth selects the identity protocol. Only the DEVELOPMENT_ONLY "fixture"
// mode exists until ENV-07 supplies the IdP inputs.
type Auth struct {
	Mode           string `koanf:"mode"`
	PrincipalsFile string `koanf:"principals_file"`
}

// SSE bounds the event stream transport (contracts.md §6): they are
// transport limits, never business clocks.
type SSE struct {
	HeartbeatInterval time.Duration `koanf:"heartbeat_interval"`
	FrameBuffer       int           `koanf:"frame_buffer"`
	SlowConsumerGrace time.Duration `koanf:"slow_consumer_grace"`
	WriteTimeout      time.Duration `koanf:"write_timeout"`
}

// Artifacts bounds the API's side of API-12: the deadline it sets on a
// transfer it begins. Control bounds it further.
type Artifacts struct {
	TransferWindow time.Duration `koanf:"transfer_window"`
}

type Config struct {
	HTTP      HTTP      `koanf:"http"`
	Control   Control   `koanf:"control"`
	Auth      Auth      `koanf:"auth"`
	SSE       SSE       `koanf:"sse"`
	Artifacts Artifacts `koanf:"artifacts"`
}

// defaults are the reviewed baseline values; the file and the allowed
// overrides refine them.
var defaults = map[string]any{
	"http.listen":               "127.0.0.1:9100",
	"http.read_header_timeout":  "10s",
	"http.body_limit_bytes":     256 << 10,
	"http.shutdown_timeout":     "20s",
	"sse.heartbeat_interval":    "15s",
	"sse.frame_buffer":          64,
	"sse.slow_consumer_grace":   "5s",
	"sse.write_timeout":         "10s",
	"artifacts.transfer_window": "15m",
}

// envOverrides is the complete set of environment variables the service
// accepts: deployment placement only. Any other ANVILKIT_API_* variable is an
// unknown key and rejects the candidate.
var envOverrides = map[string]string{
	"ANVILKIT_API_LISTEN":          "http.listen",
	"ANVILKIT_API_CONTROL_ADDRESS": "control.address",
	"ANVILKIT_API_AUTH_MODE":       "auth.mode",
	"ANVILKIT_API_PRINCIPALS_FILE": "auth.principals_file",
}

// Load builds the snapshot from the file named by EnvConfigFile (or
// DefaultConfigFile) and the process environment.
func Load() (Config, error) {
	path := os.Getenv(EnvConfigFile)
	if path == "" {
		path = DefaultConfigFile
	}
	return LoadFrom(path, os.Environ())
}

// LoadFrom is Load with explicit inputs (tests).
func LoadFrom(path string, environ []string) (Config, error) {
	k := koanf.New(".")
	if err := k.Load(confmap.Provider(defaults, "."), nil); err != nil {
		return Config{}, err
	}
	if err := k.Load(file.Provider(path), yaml.Parser()); err != nil {
		return Config{}, fmt.Errorf("config file %s: %w", path, err)
	}
	if err := applyEnv(k, environ); err != nil {
		return Config{}, err
	}
	var c Config
	if err := k.UnmarshalWithConf("", &c, koanf.UnmarshalConf{DecoderConfig: &mapstructure.DecoderConfig{
		DecodeHook:       mapstructure.StringToTimeDurationHookFunc(),
		ErrorUnused:      true, // unknown keys in the file or the overrides reject the candidate
		WeaklyTypedInput: true,
		Result:           &c,
	}}); err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	return c, c.validate()
}

func applyEnv(k *koanf.Koanf, environ []string) error {
	var unknown []string
	for _, kv := range environ {
		name, value, _ := strings.Cut(kv, "=")
		if !strings.HasPrefix(name, envPrefix) || name == EnvConfigFile {
			continue
		}
		key, ok := envOverrides[name]
		if !ok {
			unknown = append(unknown, name)
			continue
		}
		if err := k.Set(key, value); err != nil {
			return err
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return fmt.Errorf("config: environment variables are not allowed overrides: %s", strings.Join(unknown, ", "))
	}
	return nil
}

func (c Config) validate() error {
	var errs []error
	req := func(name, v string) {
		if v == "" {
			errs = append(errs, fmt.Errorf("%s is required", name))
		}
	}
	req("http.listen", c.HTTP.Listen)
	req("control.address", c.Control.Address)
	if c.HTTP.ReadHeaderTimeout < time.Second || c.HTTP.ReadHeaderTimeout > time.Minute {
		errs = append(errs, fmt.Errorf("http.read_header_timeout %s outside [1s, 1m]", c.HTTP.ReadHeaderTimeout))
	}
	if c.HTTP.BodyLimitBytes < 1<<10 || c.HTTP.BodyLimitBytes > 16<<20 {
		errs = append(errs, fmt.Errorf("http.body_limit_bytes %d outside [1KiB, 16MiB]", c.HTTP.BodyLimitBytes))
	}
	if c.HTTP.ShutdownTimeout < time.Second || c.HTTP.ShutdownTimeout > 5*time.Minute {
		errs = append(errs, fmt.Errorf("http.shutdown_timeout %s outside [1s, 5m]", c.HTTP.ShutdownTimeout))
	}
	switch c.Auth.Mode {
	case "fixture":
		req("auth.principals_file", c.Auth.PrincipalsFile)
	case "":
		errs = append(errs, errors.New("auth.mode is required: only the DEVELOPMENT_ONLY value \"fixture\" exists until ENV-07 supplies the IdP protocol"))
	default:
		errs = append(errs, fmt.Errorf("auth.mode %q is not implemented", c.Auth.Mode))
	}
	if c.SSE.HeartbeatInterval < time.Second || c.SSE.HeartbeatInterval > 5*time.Minute {
		errs = append(errs, fmt.Errorf("sse.heartbeat_interval %s outside [1s, 5m]", c.SSE.HeartbeatInterval))
	}
	if c.SSE.FrameBuffer < 1 || c.SSE.FrameBuffer > 4096 {
		errs = append(errs, fmt.Errorf("sse.frame_buffer %d outside [1, 4096]", c.SSE.FrameBuffer))
	}
	if c.SSE.SlowConsumerGrace < 100*time.Millisecond || c.SSE.SlowConsumerGrace > time.Minute {
		errs = append(errs, fmt.Errorf("sse.slow_consumer_grace %s outside [100ms, 1m]", c.SSE.SlowConsumerGrace))
	}
	if c.SSE.WriteTimeout < 100*time.Millisecond || c.SSE.WriteTimeout > time.Minute {
		errs = append(errs, fmt.Errorf("sse.write_timeout %s outside [100ms, 1m]", c.SSE.WriteTimeout))
	}
	// A stalled write must be given up before the producer's grace expires,
	// otherwise the handler could block past the bound it promises.
	if c.SSE.WriteTimeout > c.SSE.SlowConsumerGrace+c.SSE.HeartbeatInterval {
		errs = append(errs, fmt.Errorf("sse.write_timeout %s must not exceed slow_consumer_grace + heartbeat_interval", c.SSE.WriteTimeout))
	}
	if c.Artifacts.TransferWindow < time.Minute || c.Artifacts.TransferWindow > 24*time.Hour {
		errs = append(errs, fmt.Errorf("artifacts.transfer_window %s outside [1m, 24h]", c.Artifacts.TransferWindow))
	}
	return errors.Join(errs...)
}
