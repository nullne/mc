package cmd

import (
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/console"
	"github.com/minio/mc/pkg/probe"
)

var adminDiskMaintenanceFinishFlags = []cli.Flag{
// cli.StringFlag{
// 	Name:  "type",
// 	Usage: "start profiler type, possible values are 'cpu', 'mem', 'block', 'mutex' and 'trace'",
// 	Value: "mem",
// },
}

var adminDiskMaintenanceFinishCmd = cli.Command{
	Name:            "finish",
	Usage:           "finish disk maintenance",
	Action:          mainAdminDiskMaintenanceFinish,
	Before:          setGlobalsFromContext,
	Flags:           append(adminDiskMaintenanceFinishFlags, globalFlags...),
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
       $ {{.HelpName}} myminio

`,
}

// mainAdminDiskMaintenanceFinish - the entry function of profile command
func mainAdminDiskMaintenanceFinish(ctx *cli.Context) error {
	// Get the alias parameter from cli
	args := ctx.Args()
	aliasedURL := args.Get(0)

	// Create a new Minio Admin Client
	client, err := newAdminClient(aliasedURL)
	if err != nil {
		fatalIf(err.Trace(aliasedURL), "Cannot initialize admin client.")
		return nil
	}
	cmdErr := client.FinishDiskMaintenance()
	fatalIf(probe.NewError(cmdErr), "Unable to start disk maintenance.")

	console.Infoln("Disk maintenance successfully started.")
	return nil
}
