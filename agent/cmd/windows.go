// +build windows

package cmd

import (
	"fmt"
	"io"
	"os"
	runtimedebug "runtime/debug"

	"github.com/sensu/sensu-go/util/logging"
	"github.com/sensu/sensu-go/util/path"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/debug"
	"golang.org/x/sys/windows/svc/eventlog"
)

const (
	serviceName        = "SensuAgent"
	serviceDisplayName = "Sensu Agent"
	serviceDescription = "The monitoring agent for sensu-go (https://sensu.io)"
	serviceUser        = "LocalSystem"

	flagLogPath              = "log-file"
	flagLogMaxSize           = "log-max-size"
	flagLogRetentionDuration = "log-retention-duration"
	flagLogRetentionFiles    = "log-retention-files"
)

var (
	defaultConfigPath = fmt.Sprintf("%s\\agent.yml", path.SystemConfigDir())
	defaultLogPath    = fmt.Sprintf("%s\\sensu-agent.log", path.SystemLogDir())
)

// NewWindowsServiceCommand creates a cobra command that offers subcommands
// for installing, uninstalling and running sensu-agent as a windows service.
func NewWindowsServiceCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "service",
		Short: "operate sensu-agent as a windows service",
	}

	command.AddCommand(NewWindowsInstallServiceCommand())
	command.AddCommand(NewWindowsUninstallServiceCommand())
	command.AddCommand(NewWindowsRunServiceCommand())

	return command
}

func numParents(cmd *cobra.Command) int {
	var num int
	for cmd.HasParent() {
		num++
		cmd = cmd.Parent()
	}
	return num
}

// NewWindowsInstallServiceCommand creates a cobra command that installs a
// sensu-agent service in Windows.
func NewWindowsInstallServiceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "install",
		Short:         "install the sensu-agent service",
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			installArgs := append([]string{"service", "run"}, os.Args[numParents(cmd)+1:]...)
			return installService(serviceName, serviceDisplayName, serviceDescription, installArgs...)
		},
	}

	cmd.Flags().StringP(flagLogPath, "", defaultLogPath, "path to the sensu-agent log file")
	cmd.Flags().StringP(flagLogMaxSize, "", "128 MB", "maximum size of log file")
	cmd.Flags().StringP(flagLogRetentionDuration, "", "168h", "log file retention duration (s, m, h)")
	cmd.Flags().Int64P(flagLogRetentionFiles, "", 10, "maximum number of archived files to retain")

	return cmd
}

// NewWindowsUninstallServiceCommand creates a cobra command that uninstalls a
// sensu-agent service in Windows.
func NewWindowsUninstallServiceCommand() *cobra.Command {
	return &cobra.Command{
		Use:           "uninstall",
		Short:         "uninstall the sensu-agent service",
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return removeService(serviceName)
		},
	}
}

func NewWindowsRunServiceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "run",
		Short:         "run the sensu-agent service (blocking)",
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			logrus.SetFormatter(&logrus.JSONFormatter{})
			isIntSession, err := svc.IsAnInteractiveSession()
			if err != nil {
				return fmt.Errorf("failed to determine if process is running in an interactive session: %v", err)
			}
			// setup logging
			elog, err := eventlog.Open(serviceName)
			if err != nil {
				return fmt.Errorf("failed to open eventlog: %s", err)
			}
			defer elog.Close()
			defer func() {
				if e := recover(); e != nil {
					stack := runtimedebug.Stack()
					msg := fmt.Sprintf("%v\n%s", e, stack)
					elog.Error(1, msg)
					panic(e)
				}
			}()
			rotateFileLoggerCfg := logging.RotateFileLoggerConfig{
				Path:              viper.GetString(flagLogPath),
				MaxSizeBytes:      100000000000,
				RetentionDuration: viper.GetDuration(flagLogRetentionDuration),
				RetentionFiles:    viper.GetInt64(flagLogRetentionFiles),
			}
			fileLogger, err := logging.NewRotateFileLogger(rotateFileLoggerCfg)
			if err != nil {
				elog.Error(1, fmt.Sprintf("error opening log file: %s", err))
				return err
			}
			logWriter := io.MultiWriter(fileLogger, os.Stderr)
			// yoloooooooooo
			os.Stderr = logWriter
			os.Stdout = logWriter
			cfg, err := NewAgentConfig(cmd)
			if err != nil {
				if !isIntSession {
					elog.Error(1, fmt.Sprintf("error creating agent config: %s", err))
				}
				logger.Error(err)
				return err
			}
			run := svc.Run
			if isIntSession {
				run = debug.Run
			}
			if err := run(serviceName, NewService(cfg)); err != nil {
				err = fmt.Errorf("error running service: %s", err)
				elog.Error(1, err.Error())
				return err
			}
			return nil
		},
	}

	cmd.Flags().StringP(flagLogPath, "", defaultLogPath, "path to the sensu-agent log file")
	cmd.Flags().StringP(flagLogMaxSize, "", "128 MB", "maximum size of log file")
	cmd.Flags().StringP(flagLogRetentionDuration, "", "168h", "log file retention duration (s, m, h)")
	cmd.Flags().Int64P(flagLogRetentionFiles, "", 10, "maximum number of archived files to retain")

	if err := handleConfig(cmd); err != nil {
		// can only happen if there is developer error, so don't make any mistakes
		panic(err)
	}
	return cmd
}
