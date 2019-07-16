/*
 * Minio Client (C) 2017, 2018 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fatih/color"
	"github.com/minio/cli"
	json "github.com/minio/mc/pkg/colorjson"
	"github.com/minio/mc/pkg/console"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/minio/pkg/madmin"
	homedir "github.com/mitchellh/go-homedir"
)

const (
	scanNormalMode = "normal"
	scanDeepMode   = "deep"
)

var adminHealFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "scan",
		Usage: "select the healing scan mode (normal/deep)",
		Value: scanNormalMode,
	},
	cli.StringSliceFlag{
		Name:  "endpoints, e",
		Usage: "heal the object on the specified endpoint, Please make sure data is missing on the endpoint",
	},
	cli.BoolFlag{
		Name:  "single-object, S",
		Usage: "heal the exact specified object",
	},
	cli.StringFlag{
		Name:  "object-list, l",
		Usage: "heal all objects on the list",
	},
	cli.IntFlag{
		Name:  "qps",
		Usage: "it limit the qps per server",
		Value: 1,
	},
	cli.StringFlag{
		Name:  "work-dir",
		Usage: "working directory when healing objects in list, used to record healing progress, default is ~/.mc",
	},
	cli.BoolFlag{
		Name:  "recursive, r",
		Usage: "heal recursively",
	},
	cli.BoolFlag{
		Name:  "dry-run, n",
		Usage: "only inspect data, but do not mutate",
	},
	cli.BoolFlag{
		Name:  "force-start, f",
		Usage: "force start a new heal sequence",
	},
	cli.BoolFlag{
		Name:  "force-stop, s",
		Usage: "Force stop a running heal sequence",
	},
	cli.BoolFlag{
		Name:  "remove",
		Usage: "remove dangling objects in heal sequence",
	},
}

var adminHealCmd = cli.Command{
	Name:            "heal",
	Usage:           "heal disks, buckets and objects on minio server",
	Action:          mainAdminHeal,
	Before:          setGlobalsFromContext,
	Flags:           append(adminHealFlags, globalFlags...),
	HideHelpCommand: true,
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS] TARGET

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}

SCAN MODES:
   normal (default): Heal objects which are missing on one or more disks.
   deep            : Heal objects which are missing on one or more disks. Also heal objects with silent data corruption.

EXAMPLES:
    1. To format newly replaced disks in a Minio server with alias 'play'
       $ {{.HelpName}} play

    2. Heal 'testbucket' in a Minio server with alias 'play'
       $ {{.HelpName}} play/testbucket/

    3. Heal all objects under 'dir' prefix
       $ {{.HelpName}} --recursive play/testbucket/dir/

    4. Issue a dry-run heal operation to inspect objects health but not heal them
       $ {{.HelpName}} --dry-run play

    5. Issue a dry-run heal operation to inspect objects health under 'dir' prefix
       $ {{.HelpName}} --recursive --dry-run play/testbucket/dir/

`,
}

func checkAdminHealSyntax(ctx *cli.Context) {
	if len(ctx.Args()) != 1 {
		cli.ShowCommandHelpAndExit(ctx, "heal", 1) // last argument is exit code
	}

	// Check for scan argument
	scanArg := ctx.String("scan")
	scanArg = strings.ToLower(scanArg)
	if scanArg != scanNormalMode && scanArg != scanDeepMode {
		cli.ShowCommandHelpAndExit(ctx, "heal", 1) // last argument is exit code
	}
}

// stopHealMessage is container for stop heal success and failure messages.
type stopHealMessage struct {
	Status string `json:"status"`
	Alias  string `json:"alias"`
}

// String colorized stop heal message.
func (s stopHealMessage) String() string {
	return console.Colorize("HealStopped", "Heal stopped successfully at `"+s.Alias+"`.")
}

// JSON jsonified stop heal message.
func (s stopHealMessage) JSON() string {
	stopHealJSONBytes, e := json.MarshalIndent(s, "", " ")
	fatalIf(probe.NewError(e), "Unable to marshal into JSON.")

	return string(stopHealJSONBytes)
}

func transformScanArg(scanArg string) madmin.HealScanMode {
	switch scanArg {
	case "deep":
		return madmin.HealDeepScan
	}
	return madmin.HealNormalScan
}

// mainAdminHeal - the entry function of heal command
func mainAdminHeal(ctx *cli.Context) error {

	// Check for command syntax
	checkAdminHealSyntax(ctx)

	// Get the alias parameter from cli
	args := ctx.Args()
	aliasedURL := args.Get(0)

	console.SetColor("Heal", color.New(color.FgGreen, color.Bold))
	console.SetColor("HealUpdateUI", color.New(color.FgYellow, color.Bold))
	console.SetColor("HealStopped", color.New(color.FgGreen, color.Bold))

	// Create a new Minio Admin Client
	client, err := newAdminClient(aliasedURL)
	if err != nil {
		fatalIf(err.Trace(aliasedURL), "Cannot initialize admin client.")
		return nil
	}

	// Compute bucket and object from the aliased URL
	aliasedURL = filepath.ToSlash(aliasedURL)
	splits := splitStr(aliasedURL, "/", 3)
	bucket, prefix := splits[1], splits[2]

	opts := madmin.HealOpts{
		ScanMode:   transformScanArg(ctx.String("scan")),
		Remove:     ctx.Bool("remove"),
		Recursive:  ctx.Bool("recursive"),
		DryRun:     ctx.Bool("dry-run"),
		DisksIndex: make(map[int][]int),
	}
	endpoints := make(map[string]map[string]struct{})
	if es := ctx.StringSlice("endpoints"); len(es) != 0 {
		sets := make(map[string]map[string][2]int)
		serversInfo, e := client.ServerInfo()
		if e != nil {
			return e
		}
		for _, info := range serversInfo {
			disksIndex := make(map[string][2]int)
			if info.Data == nil {
				continue
			}
			for j, drives := range info.Data.StorageInfo.Backend.Sets {
				for k, drive := range drives {
					u, err := url.Parse(drive.Endpoint)
					if err != nil {
						return err
					}
					if u.Host != info.Addr && u.Host != "" {
						continue
					}
					disksIndex[u.Path] = [2]int{j, k}
				}
			}
			sets[info.Addr] = disksIndex
		}
		for _, endpoint := range ctx.StringSlice("endpoints") {
			var ip, port, path string
			if idx := strings.Index(endpoint, "/"); idx != -1 {
				path = endpoint[idx:]
				endpoint = strings.TrimSuffix(endpoint, path)
			}
			if idx := strings.Index(endpoint, ":"); idx != -1 && idx < len(endpoint) {
				port = endpoint[idx+1:]
				endpoint = endpoint[:idx]
			}
			if port == "" {
				port = "9000"
			}
			ip = endpoint
			host := fmt.Sprintf("%s:%s", ip, port)
			idx, ok := sets[host]
			if !ok {
				return fmt.Errorf("invalid path in endpoint %s, should match the servers %+v", endpoint, sets)
			}
			if path == "" {
				endpoints[host] = map[string]struct{}{}
				for _, i := range idx {
					opts.DisksIndex[i[0]] = append(opts.DisksIndex[i[0]], i[1])
				}
			} else {
				i, ok := idx[path]
				if !ok {
					return fmt.Errorf("invalid path in endpoint %s, should match the servers %+v", endpoint, sets)
				}
				endpoints[host] = map[string]struct{}{path: struct{}{}}
				opts.DisksIndex[i[0]] = append(opts.DisksIndex[i[0]], i[1])
			}
		}
	}

	objectList := ctx.String("object-list")
	// heal all objects list in the file
	if objectList != "" {
		workDir := ctx.String("work-dir")
		if workDir == "" {
			homeDir, e := homedir.Dir()
			if e != nil {
				return e
			}
			workDir = path.Join(homeDir, globalMCConfigDir)
		}
		clients, err := newAdminClients(aliasedURL)
		if err != nil {
			fatalIf(err.Trace(aliasedURL), "Cannot initialize admin client.")
			return nil
		}
		var wg sync.WaitGroup
		trapCh := signalTrap(os.Interrupt, syscall.SIGTERM, syscall.SIGKILL)
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
		ui := objectListHealData{
			uiData: uiData{Bucket: bucket,
				Prefix: prefix,
				Client: client,
				// ClientToken:           healStart.ClientToken,
				// ForceStart:            forceStart,
				HealOpts:              &opts,
				ObjectsByOnlineDrives: make(map[int]int64),
				HealthCols:            make(map[col]int64),
				CurChan:               cursorAnimate(),
			},
			Started: time.Now(),
			Total:   0,
		}
		ch := ui.Display(ctx2, &wg, endpoints)
		functions := make([]objectHandleFunc, len(clients))
		for idx := range clients {
			c := clients[idx]
			functions[idx] = func(bucket, key string) error {
				res, err := c.HealObject(bucket, key, opts)
				if err != nil {
					return err
				}
				select {
				case ch <- &res:
				default:
				}
				return nil
			}
		}
		e := healObjectList(ctx2, workDir, objectList, functions, ctx.Int("qps"))
		cancel()
		wg.Wait()
		return e
	}

	singleObject := ctx.Bool("single-object")
	if singleObject {
		res, err := client.HealObject(bucket, prefix, opts)
		errorIf(probe.NewError(err), "Failed to heal single object %s/%s", bucket, prefix)
		fmt.Printf("%+v\n", res)
		return nil
	}

	forceStart := ctx.Bool("force-start")
	forceStop := ctx.Bool("force-stop")
	if forceStop {
		_, _, herr := client.Heal(bucket, prefix, opts, "", forceStart, forceStop)
		errorIf(probe.NewError(herr), "Failed to stop heal sequence.")
		printMsg(stopHealMessage{Status: "success", Alias: aliasedURL})
		return nil
	}

	healStart, _, herr := client.Heal(bucket, prefix, opts, "", forceStart, false)
	errorIf(probe.NewError(herr), "Failed to start heal sequence.")

	ui := uiData{
		Bucket:                bucket,
		Prefix:                prefix,
		Client:                client,
		ClientToken:           healStart.ClientToken,
		ForceStart:            forceStart,
		HealOpts:              &opts,
		ObjectsByOnlineDrives: make(map[int]int64),
		HealthCols:            make(map[col]int64),
		CurChan:               cursorAnimate(),
	}

	res, e := ui.DisplayAndFollowHealStatus(aliasedURL)
	if e != nil {
		if res.FailureDetail != "" {
			data, _ := json.MarshalIndent(res, "", " ")
			traceStr := string(data)
			errorIf(probe.NewError(e).Trace(aliasedURL, traceStr), "Unable to display heal status.")
		} else {
			errorIf(probe.NewError(e).Trace(aliasedURL), "Unable to display heal status.")
		}
	}
	return nil
}
