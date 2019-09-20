package cmd

import (
	"context"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/fatih/color"
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/console"
	"github.com/minio/minio/pkg/madmin"
)

var adminDiskMaintenanceFlags = []cli.Flag{
	cli.DurationFlag{
		Name:  "refresh-interval, I",
		Usage: "refresh maintenance status every interval",
		Value: time.Second,
	},
}

var adminDiskMaintenanceCmd = cli.Command{
	Name:   "disk-maintain",
	Usage:  "disk maintenance",
	Action: mainAdminDiskMaintenance,
	Before: setGlobalsFromContext,
	Flags:  append(adminDiskMaintenanceFlags, adminDiskMaintenanceStartFlags...),
	Subcommands: []cli.Command{
		adminDiskMaintenanceStartCmd,
		adminDiskMaintenanceFinishCmd,
		adminDiskMaintenanceStatusCmd,
	},
	HideHelpCommand: true,
}

type diskMaintenanceResult struct {
	Endpoint      string
	Status        string
	CurrentJob    string
	CurrentStatus string
	Completed     int
	Total         int
	Message       string
	LastUpdated   time.Time
	Duration      time.Duration
}

const (
	MaintenanceStatusIdle      = "idle"
	MaintenanceStatusRunning   = "running"
	MaintenanceStatusSucceeded = "succeeded"
	MaintenanceStatusFailed    = "failed"
)

const (
	VolumeMaintenanceStatusWaiting    = "waiting"
	VolumeMaintenanceStatusDumping    = "dumping"
	VolumeMaintenanceStatusCompacting = "compacting"
	VolumeMaintenanceStatusCompleted  = "completed"
	VolumeMaintenanceStatusFailed     = "failed"
)

type diskMaintenanceResults []diskMaintenanceResult

// 10.3.18.189 | running | 12/16 | /data1/foo11 (dumpping) | 3s | nil
func (r diskMaintenanceResults) display() {
	// summary line

	printColors := make([]*color.Color, len(r))
	cellText := make([][]string, len(r))
	for i, result := range r {
		switch result.Status {
		case MaintenanceStatusIdle:
			printColors[i] = getPrintCol(colGrey)
		case MaintenanceStatusRunning:
			printColors[i] = getPrintCol(colYellow)
		case MaintenanceStatusSucceeded:
			printColors[i] = getPrintCol(colGreen)
		case MaintenanceStatusFailed:
			printColors[i] = getPrintCol(colRed)
		default:
			continue
		}
		msg := result.Message
		if d := time.Since(result.LastUpdated); d > time.Second {
			msg += fmt.Sprintf("(updated %s ago)", d.Truncate(time.Second).String())
		}

		cur := fmt.Sprintf("%s(%s)", result.CurrentJob, result.CurrentStatus)
		if result.CurrentJob == "" {
			cur = ""
		}

		cellText[i] = []string{
			result.Duration.Truncate(time.Second).String(),
			result.Endpoint,
			result.Status,
			fmt.Sprintf("%d/%d", result.Completed, result.Total),
			cur,
			msg,
		}
	}

	t := console.NewTable(printColors, []bool{false, true, true, true, true, true}, 4)
	t.DisplayTable(cellText)
}

// mainAdminHeal - the entry function of heal command
func mainAdminDiskMaintenance(ctx *cli.Context) error {
	// cli.ShowCommandHelp(ctx, ctx.Args().First())
	// return nil

	args := ctx.Args()
	aliasedURL := args.Get(0)

	// Create a new Minio Admin Client
	clients, err := newAdminClients(aliasedURL)
	if err != nil {
		fatalIf(err.Trace(aliasedURL), "Cannot initialize admin client.")
		return nil
	}

	chs := make([]chan diskMaintenanceResult, len(clients))
	results := make(diskMaintenanceResults, len(clients))

	// var wg sync.WaitGroup
	trapCh := signalTrap(os.Interrupt, syscall.SIGKILL)
	ctx2, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-ctx2.Done():
			return
		case <-trapCh:
			cancel()
		}
	}()

	interval := ctx.Duration("refresh-interval")
	rate := ctx.Float64("rate")
	timeRange := ctx.String("time-range")
	for i, client := range clients {
		chs[i] = foo(ctx2, client, interval, rate, timeRange)
	}
	ticker := time.NewTicker(interval)

	for {
		select {
		case <-ctx2.Done():
			break
		case <-ticker.C:
		}
		for i, ch := range chs {
			results[i] = <-ch
		}
		console.RewindLines(len(results) + 2)
		results.display()
	}
}

func toDiskMaintenanceStatus(endpoint string, res madmin.MaintenanceStatus) diskMaintenanceResult {
	var total, completed int
	var cur, curstatus string

	for k, status := range res.Volumes {
		total += 1
		switch status.Status {
		case VolumeMaintenanceStatusCompleted:
			completed += 1
		case VolumeMaintenanceStatusDumping:
			fallthrough
		case VolumeMaintenanceStatusCompacting:
			fallthrough
		case VolumeMaintenanceStatusFailed:
			cur = k
			curstatus = status.Status
		}
	}

	return diskMaintenanceResult{
		Endpoint:      endpoint,
		Status:        res.Status,
		CurrentJob:    cur,
		CurrentStatus: curstatus,
		Completed:     completed,
		Total:         total,
		Message:       res.Error,
		LastUpdated:   time.Now(),
		Duration:      time.Since(res.StartTime),
	}
}

// 1. if not status is idle, start the maintenance
// 2. for loop to get the status
// 3. if succeeded, finish it
// 4. if failed, try again
// 5.
func foo(ctx context.Context, client *madmin.AdminClient, duration time.Duration, rate float64, timeRange string) chan diskMaintenanceResult {
	ch := make(chan diskMaintenanceResult)
	ch2 := make(chan diskMaintenanceResult, 1)
	ticker := time.NewTicker(duration)
	go func() {
		defer ticker.Stop()
		res, err := client.GetDiskMaintenanceStatus()
		if err != nil {
			panic(err)
		}
		ch2 <- toDiskMaintenanceStatus(client.EndpointAddr(), res)
		if res.Status == "idle" {
			// started
			if err := client.StartDiskMaintenance(rate, timeRange); err != nil {
				panic(err)
			}
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			res, err := client.GetDiskMaintenanceStatus()
			if err != nil {
				panic(err)
			}
			ch2 <- toDiskMaintenanceStatus(client.EndpointAddr(), res)
			if res.Status == "running" {
				continue
			} else if res.Status == "succeeded" {
				break
			} else if res.Status == "failed" {
				break
			}
		}
	}()

	go func() {
		var result diskMaintenanceResult
		for {
			select {
			case result = <-ch2:
			case ch <- result:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch
}
