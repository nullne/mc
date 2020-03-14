package cmd

import (
	"github.com/minio/cli"
)

var adminDiskMaintenanceCmd = cli.Command{
	Name:   "disk-maintain",
	Usage:  "disk maintenance",
	Action: mainAdminDiskMaintenance,
	Before: setGlobalsFromContext,
	Subcommands: []cli.Command{
		adminDiskMaintenanceStartCmd,
		adminDiskMaintenanceFinishCmd,
		adminDiskMaintenanceStatusCmd,
	},
	HideHelpCommand: true,
}

// mainAdminHeal - the entry function of heal command
func mainAdminDiskMaintenance(ctx *cli.Context) error {
	cli.ShowCommandHelp(ctx, ctx.Args().First())
	return nil
}
