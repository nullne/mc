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
	Flags:           append(adminDiskMaintenanceStartFlags, adminDiskMaintenanceFinishFlags...),
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
	es := ctx.StringSlice("endpoint")
	all := ctx.Bool("all-nodes")
	if len(ctx.Args()) != 1 ||
		(!all && len(es) == 0) {
		cli.ShowCommandHelpAndExit(ctx, "start", 1) // last argument is exit code
	}

	// Get the alias parameter from cli
	args := ctx.Args()
	aliasedURL := args.Get(0)

	// Create a new Minio Admin Client
	clients, err := newAdminClients(aliasedURL)
	if err != nil {
		fatalIf(err.Trace(aliasedURL), "Cannot initialize admin client.")
		return nil
	}
	rate := ctx.Float64("rate")
	timeRange := ctx.String("time-range")
	esMap := make(map[string]bool, len(es))
	for _, e := range es {
		esMap[e] = true
	}

	for _, client := range clients {
		if !all && !esMap[client.EndpointAddr()] {
			continue
		}
		if cmdErr := client.StartDiskMaintenance(rate, timeRange); cmdErr != nil {
			fatalIf(probe.NewError(cmdErr), "Unable to start disk maintenance.")
		} else {
			console.Infoln("Disk maintenance successfully started.")
		}
	}
	return nil
}
