// Package config reads the molq configuration file. It does not read MOLQ_* variables.
package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"

	"github.com/MolCrafts/molq/protocol"
	"gopkg.in/yaml.v3"
)

// File is the on-disk configuration. settings and presets are preserved for
// later serve-owned validation; this loader records the fields needed to start.
type File struct {
	Serve struct {
		Host  string `json:"host" yaml:"host"`
		Port  int    `json:"port" yaml:"port"`
		Token string `json:"token" yaml:"token"`
	} `json:"serve" yaml:"serve"`
	Client struct {
		Endpoint   string `json:"endpoint" yaml:"endpoint"`
		Token      string `json:"token" yaml:"token"`
		Registry   string `json:"registry" yaml:"registry"`
		Executable string `json:"executable" yaml:"executable"`
		Watch      struct {
			Interval      float64  `json:"interval" yaml:"interval"`
			Clusters      []string `json:"clusters" yaml:"clusters"`
			NerveEndpoint string   `json:"nerve_endpoint" yaml:"nerve_endpoint"`
		} `json:"watch" yaml:"watch"`
	} `json:"client" yaml:"client"`
}

// DefaultPath is the configuration file used when --config is omitted.
// It lives next to the registry.
func DefaultPath() string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		home = "."
	}
	return filepath.Join(home, ".molcrafts", "molq", "config.json")
}

// Load reads a JSON or YAML file. An empty path yields an empty file.
func Load(path string) (File, error) {
	if path == "" {
		return File{}, nil
	}
	body, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return File{}, nil
		}
		return File{}, protocol.Fail("INVALID_INPUT", "Cannot read configuration file", map[string]any{"path": path})
	}
	var file File
	if strings.HasSuffix(path, ".yaml") || strings.HasSuffix(path, ".yml") {
		err = yaml.Unmarshal(body, &file)
	} else {
		err = json.Unmarshal(body, &file)
	}
	if err != nil {
		return File{}, protocol.Fail("INVALID_INPUT", "Cannot read configuration file", map[string]any{"path": path})
	}
	return file, nil
}
