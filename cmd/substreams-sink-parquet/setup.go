package main

import (
	"bytes"
	"fmt"
	"io"
	"path"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/cli/sflags"
	"github.com/streamingfast/dstore"
	"go.uber.org/zap"
)

var sinkSetupCmd = Command(sinkSetupE,
	"setup <store_url>",
	"Setup and validate object store access",
	ExactArgs(1),
	Flags(func(flags *pflag.FlagSet) { addSetupFlags(flags) }),
	Example("substreams-sink-parquet setup s3://my-bucket/prefix"),
	OnCommandErrorLogAndExit(zlog),
)

func addSetupFlags(flags *pflag.FlagSet) {
	flags.String("output-prefix", "", "Object key prefix inside store URL")
}

func sinkSetupE(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	storeURL := args[0]
	outputPrefix := sflags.MustGetString(cmd, "output-prefix")

	zlog.Info("validating store access", zap.String("store_url", storeURL), zap.String("prefix", outputPrefix))

	store, err := dstore.NewStore(storeURL, outputPrefix, "", false)
	if err != nil {
		return fmt.Errorf("create store: %w", err)
	}

	key := path.Join(".parquet-sink-setup", fmt.Sprintf("probe-%d", time.Now().UnixNano()))
	content := []byte("ok")
	if err := store.WriteObject(ctx, key, bytes.NewReader(content)); err != nil {
		return fmt.Errorf("write probe: %w", err)
	}
	rc, err := store.OpenObject(ctx, key)
	if err != nil {
		return fmt.Errorf("open probe: %w", err)
	}
	defer rc.Close()
	buf, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("read probe: %w", err)
	}
	if string(buf) != "ok" {
		return fmt.Errorf("probe content mismatch")
	}
	if err := store.DeleteObject(ctx, key); err != nil {
		return fmt.Errorf("delete probe: %w", err)
	}

	zlog.Info("store validation ok")
	return nil
}
