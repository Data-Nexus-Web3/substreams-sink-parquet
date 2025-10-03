package main

import (
	"github.com/streamingfast/cli"
	"github.com/streamingfast/logging"
	"go.uber.org/zap"
)

var zlog, tracer = logging.RootLogger("sink-parquet", "github.com/streamingfast/substreams-sink-parquet/cmd/substreams-sink-parquet")

func init() {
	cli.SetLogger(zlog, tracer)
	logging.InstantiateLoggers(logging.WithDefaultLevel(zap.InfoLevel))
}
