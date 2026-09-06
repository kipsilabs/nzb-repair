package nzbrepair

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/kipsilabs/nzb-repair/internal/app"
	"github.com/kipsilabs/nzb-repair/internal/config"
	"github.com/spf13/cobra"
)

var (
	configFile      string
	outputFileOrDir string
	verbose         bool
	watchDir        string
	dbPath          string
	tmpDir          string
	rootCmd         = &cobra.Command{
		Use:   "nzbrepair [nzb file]",
		Short: "NZB Repair tool",
		Long:  `A command line tool to repair NZB files`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.NewFromFile(configFile)
			if err != nil {
				slog.Error("Failed to load config", "error", err)
				return err
			}

			effectiveTmpDir := tmpDir
			if effectiveTmpDir == "" {
				effectiveTmpDir = os.TempDir()
			}

			return app.RunSingleRepair(cmd.Context(), cfg, args[0], outputFileOrDir, effectiveTmpDir, verbose)
		},
	}
	watchCmd = &cobra.Command{
		Use:   "watch",
		Short: "Scan a directory for NZB files and repair them",
		Long:  `Periodically scans a specified directory for .nzb files and queues them for repair. The scan interval can be configured in the config file.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.NewFromFile(configFile)
			if err != nil {
				slog.Error("Failed to load config", "error", err)
				return err
			}

			effectiveTmpDir := tmpDir
			if effectiveTmpDir == "" {
				effectiveTmpDir = os.TempDir()
			}

			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			return app.RunWatcher(ctx, cfg, watchDir, dbPath, outputFileOrDir, effectiveTmpDir, verbose)
		},
	}
)

func init() {
	rootCmd.PersistentFlags().StringVarP(&configFile, "config", "c", "", "config file path")
	rootCmd.PersistentFlags().StringVarP(&outputFileOrDir, "output", "o", "", "output file path or directory for repaired nzb files (default: next to input / repaired/ dir for watch)")
	rootCmd.PersistentFlags().StringVar(&tmpDir, "tmp-dir", os.TempDir(), "temporary directory for processing files")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "enable verbose logging")
	_ = rootCmd.MarkPersistentFlagRequired("config")

	watchCmd.Flags().StringVarP(&watchDir, "dir", "d", "", "directory to watch for nzb files")
	watchCmd.Flags().StringVarP(&dbPath, "db", "b", "queue.db", "path to the sqlite database file")
	_ = watchCmd.MarkFlagRequired("dir")

	rootCmd.AddCommand(watchCmd)
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		slog.Error("Command execution failed", "error", err)
		os.Exit(1)
	}
}
