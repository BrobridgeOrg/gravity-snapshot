package configs

import (
	"fmt"
	"runtime"
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	Collections []string
}

func GetConfig() *Config {

	// From the environment
	viper.SetEnvPrefix("GRAVITY_SNAPSHOT")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	// From config file
	viper.SetConfigName("config")
	viper.AddConfigPath("./")
	viper.AddConfigPath("./configs")

	if err := viper.ReadInConfig(); err != nil {
		fmt.Println("No configuration file was loaded")
	}

	runtime.GOMAXPROCS(8)

	config := &Config{
		Collections: make([]string, 0),
	}

	// Specify collections from environment variable for watching
	collections := viper.GetStringSlice("COLLECTIONS")
	for _, c := range collections {
		config.Collections = append(config.Collections, c)
	}

	return config
}

func (config *Config) FindCollections(event string) int {

	for i, e := range config.Collections {
		if event == e {
			return i
		}
	}

	return -1
}

func (config *Config) AddCollections(events []string) {

	for _, event := range events {
		if config.FindCollections(event) == -1 {
			config.Collections = append(config.Collections, event)
		}
	}
}
