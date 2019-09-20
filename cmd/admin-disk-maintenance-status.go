package cmd

import (
	"fmt"

	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
)

var adminDiskMaintenanceStatusFlags = []cli.Flag{
// cli.StringFlag{
// 	Name:  "type",
// 	Usage: "start profiler type, possible values are 'cpu', 'mem', 'block', 'mutex' and 'trace'",
// 	Value: "mem",
// },
}

var adminDiskMaintenanceStatusCmd = cli.Command{
	Name:            "status",
	Usage:           "get disk maintenance status",
	Action:          mainAdminDiskMaintenanceStatus,
	Before:          setGlobalsFromContext,
	Flags:           append(adminDiskMaintenanceStatusFlags, globalFlags...),
	HideHelpCommand: true,
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS] TARGET

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}
EXAMPLES:
    1. Status disk maintenance
       $ {{.HelpName}} myminio

`,
}

// mainAdminDiskMaintenanceStatus - the entry function of profile command
func mainAdminDiskMaintenanceStatus(ctx *cli.Context) error {
	// Get the alias parameter from cli
	args := ctx.Args()
	aliasedURL := args.Get(0)

	// Create a new Minio Admin Client
	client, err := newAdminClient(aliasedURL)
	if err != nil {
		fatalIf(err.Trace(aliasedURL), "Cannot initialize admin client.")
		return nil
	}
	status, cmdErr := client.GetDiskMaintenanceStatus()
	fmt.Println(cmdErr)
	fatalIf(probe.NewError(cmdErr), "Unable to get status of disk maintenance.")
	fmt.Println(status)
	return nil
}
