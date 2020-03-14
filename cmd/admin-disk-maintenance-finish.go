package cmd

import (
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/console"
	"github.com/minio/mc/pkg/probe"
)

var adminDiskMaintenanceFinishFlags = []cli.Flag{
	cli.StringSliceFlag{
		Name:  "endpoint, e",
		Usage: "endpoint",
	},
	cli.BoolFlag{
		Name:  "all-nodes, a",
		Usage: "finish disk maintenance on all nodes",
	},
}

var adminDiskMaintenanceFinishCmd = cli.Command{
	Name:            "finish",
	Usage:           "finish disk maintenance, -e or -all-nodes must be specified",
	Action:          mainAdminDiskMaintenanceFinish,
	Before:          setGlobalsFromContext,
	Flags:           adminDiskMaintenanceFinishFlags,
	HideHelpCommand: true,
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS] TARGET

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}
EXAMPLES:
    1. Finish disk maintenance
       $ {{.HelpName}} -all-nodes myminio

`,
}

// mainAdminDiskMaintenanceFinish - the entry function of profile command
func mainAdminDiskMaintenanceFinish(ctx *cli.Context) error {
	es := ctx.StringSlice("endpoint")
	all := ctx.Bool("all-nodes")

	if len(ctx.Args()) != 1 ||
		(!all && len(es) == 0) {
		cli.ShowCommandHelpAndExit(ctx, "finish", 1) // last argument is exit code
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

	esMap := make(map[string]bool, len(es))
	for _, e := range es {
		esMap[e] = true
	}

	for _, client := range clients {
		if !all && !esMap[client.EndpointAddr()] {
			continue
		}
		if err := client.FinishDiskMaintenance(); err != nil {
			fatal(probe.NewError(err), "Unable to finish disk maintenance on %s", client.EndpointAddr())
		} else {
			console.Infof("Disk maintenance on %s has been successfully finished. \n", client.EndpointAddr())

		}
	}
	return nil
}
