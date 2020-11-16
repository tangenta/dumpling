package export

import (
	"context"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/dumpling/v4/log"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/utils"
)

type ConfigResolver struct {
	ctx context.Context
}

func NewConfigResolver(ctx context.Context) *ConfigResolver {
	return &ConfigResolver{ctx}
}

func (c *ConfigResolver) ResolveConfig(conf *Config) error {
	return resolve(conf,
		initLogger,
		registerTLSConfig,
		validateFileSize,
		validateSpecifiedSQL)
}

func resolve(conf *Config, fns ...func(*Config) error) error {
	for _, f := range fns {
		err := f(conf)
		if err != nil {
			return err
		}
	}
	return nil
}

func initLogger(conf *Config) error {
	if conf.Logger != nil {
		log.SetAppLogger(conf.Logger)
	} else {
		err := log.InitAppLogger(&log.Config{
			Level:  conf.LogLevel,
			File:   conf.LogFile,
			Format: conf.LogFormat,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func registerTLSConfig(conf *Config) error {
	if len(conf.Security.CAPath) > 0 {
		tlsConfig, err := utils.ToTLSConfig(conf.Security.CAPath, conf.Security.CertPath, conf.Security.KeyPath)
		if err != nil {
			return err
		}
		err = mysql.RegisterTLSConfig("dumpling-tls-target", tlsConfig)
		if err != nil {
			return err
		}
	}
	return nil
}

func validateFileSize(conf *Config) error {
	if conf.Rows != UnspecifiedSize && conf.FileSize != UnspecifiedSize {
		return errors.Errorf("invalid config: cannot set both --rows and --filesize to non-zero")
	}
	return nil
}

func validateSpecifiedSQL(conf *Config) error {
	if conf.Sql != "" && conf.Where != "" {
		return errors.New("can't specify both --sql and --where at the same time. Please try to combine them into --sql")
	}
	return nil
}
