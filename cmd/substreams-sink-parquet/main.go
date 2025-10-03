package main

import (
	"log"
	"net/http"
	_ "net/http/pprof"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/dmetrics"
)

var version = "dev"

func main() {
	go func() { log.Println(http.ListenAndServe(":6060", nil)) }()

	Run("substreams-sink-parquet", "Substreams Parquet Sink",
		sinkRunCmd,
		sinkSetupCmd,

		ConfigureViper("SINK_PARQUET"),
		ConfigureVersion(version),
		PersistentFlags(func(flags *pflag.FlagSet) {
			flags.String("metrics-listen-addr", "localhost:9102", "[Operator] Prometheus metrics listen addr")
			flags.String("pprof-listen-addr", "localhost:6060", "[Operator] pprof listen addr")
		}),
		AfterAllHook(func(cmd *cobra.Command) {
			cmd.PersistentPreRun = func(cmd *cobra.Command, _ []string) {
				if v, _ := cmd.Flags().GetString("metrics-listen-addr"); v != "" {
					go dmetrics.Serve(v)
				}
			}
		}),
	)
}
