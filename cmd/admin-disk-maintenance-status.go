package cmd

import (
	"github.com/minio/cli"
	json "github.com/minio/mc/pkg/colorjson"
	"github.com/minio/mc/pkg/console"
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
	Flags:           adminDiskMaintenanceStatusFlags,
	HideHelpCommand: true,
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS] TARGET

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}
EXAMPLES:
    1. Get status of disk maintenance
       $ {{.HelpName}} myminio

`,
}

// mainAdminDiskMaintenanceStatus - the entry function of profile command
func mainAdminDiskMaintenanceStatus(ctx *cli.Context) error {
	if len(ctx.Args()) != 1 {
		cli.ShowCommandHelpAndExit(ctx, "status", 1) // last argument is exit code
	}
	// Get the alias parameter from cli
	args := ctx.Args()
	aliasedURL := args.Get(0)

	// Create a new Minio Admin Client
	clients, err := newAdminClients(aliasedURL)
	if err != nil {
		fatal(err.Trace(aliasedURL), "Cannot initialize admin client.")
		return nil
	}
	for _, client := range clients {
		status, cmdErr := client.GetDiskMaintenanceStatus()
		if cmdErr != nil {
			fatal(probe.NewError(cmdErr), "Unable to get status of disk maintenance on %s", client.EndpointAddr())
		} else {
			bytes, err := json.MarshalIndent(status, "", " ")
			fatalIf(probe.NewError(err), "Unable to marshal to JSON.")
			console.Println(client.EndpointAddr())
			console.Println(string(bytes))
		}
	}
	return nil
}
