package cmd

import (
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/console"
	"github.com/minio/mc/pkg/probe"
)

var adminDiskMaintenanceStartFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "time-range",
		Usage: "the job should only be run between the time-range",
		Value: "02:00:00-08:00:00",
	},
	cli.Float64Flag{
		Name:  "rate",
		Usage: "",
		Value: 0.9,
	},
}

var adminDiskMaintenanceStartCmd = cli.Command{
	Name:            "start",
	Usage:           "start disk maintenance",
	Action:          mainAdminDiskMaintenanceStart,
	Before:          setGlobalsFromContext,
	Flags:           append(adminDiskMaintenanceStartFlags, globalFlags...),
	HideHelpCommand: true,
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS] TARGET

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}
EXAMPLES:
    1. Start disk maintenance
       $ {{.HelpName}} myminio

`,
}

// mainAdminDiskMaintenanceStart - the entry function of profile command
func mainAdminDiskMaintenanceStart(ctx *cli.Context) error {
	// Get the alias parameter from cli
	args := ctx.Args()
	aliasedURL := args.Get(0)

	// Create a new Minio Admin Client
	client, err := newAdminClient(aliasedURL)
	if err != nil {
		fatalIf(err.Trace(aliasedURL), "Cannot initialize admin client.")
		return nil
	}
	rate := ctx.Float64("rate")
	timeRange := ctx.String("time-range")
	cmdErr := client.StartDiskMaintenance(rate, timeRange)
	fatalIf(probe.NewError(cmdErr), "Unable to start disk maintenance.")

	console.Infoln("Disk maintenance successfully started.")
	return nil
}
