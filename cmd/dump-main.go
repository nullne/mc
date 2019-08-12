package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/beltran/gohive"
	"github.com/fatih/color"
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/console"
	"github.com/minio/minio/cmd/logger"
	"gitlab.p1staff.com/common-tech/tantan-object-storage/cluster"
)

// dump specific flags.
var (
	dumpFlags = []cli.Flag{
		cli.StringFlag{
			Name:  "source",
			Value: "hive",
			Usage: "from where to dump, available choices: hive(default)",
		},
		cli.StringFlag{
			Name:  "addr",
			Usage: "address with corresponding type",
		},
		cli.StringFlag{
			Name:  "username",
			Usage: "set username if needed",
		},
		cli.StringFlag{
			Name:  "output, o",
			Value: "",
			Usage: "output to specified file",
		},
		// filters
		cli.StringFlag{
			Name:  "since",
			Usage: "start of the time range",
		},
		cli.StringFlag{
			Name:  "until",
			Usage: "end of the time range",
		},
		cli.StringSliceFlag{
			Name:  "cluster-id, c",
			Usage: "cluster id",
		},
	}
)

// list files and folders.
var adminDumpCmd = cli.Command{
	Name:   "dump",
	Usage:  "dump object list",
	Action: mainDump,
	// Before: setGlobadumpFromContext,
	Flags: append(dumpFlags, globalFlags...),
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS] TARGET [TARGET ...]

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}
EXAMPLES:
   1. Dump object list of cluster 01 within the given time range from hive
	  $ {{.HelpName}} dump --source hive --addr 10.compute-r630.bjs.p1staff.com:10000 --username hdp-common --since "2019-07-10 00:00:00" --until "2019-07-11 00:00:00" --cluster-id "01"
`,
}

// mainList - is a handler for mc dump command
func mainDump(ctx *cli.Context) error {
	// Additional command specific theme customization.
	console.SetColor("File", color.New(color.Bold))
	console.SetColor("Dir", color.New(color.FgCyan, color.Bold))
	console.SetColor("Size", color.New(color.FgYellow))
	console.SetColor("Time", color.New(color.FgGreen))

	// check 'dump' cli arguments.
	checkListSyntax(ctx)

	// Set command flags from context.
	since, until := ctx.String("since"), ctx.String("until")
	addr, username := ctx.String("addr"), ctx.String("username")
	clusterIDs := ctx.StringSlice("cluster-id")
	// output := ctx.String("output")
	source := ctx.String("source")

	var writer io.Writer
	writer = os.Stdout
	if p := ctx.String("output"); p != "" {
		file, err := os.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0644)
		if err != nil {
			logger.Fatal(err, "cannot open file")
		}
		defer file.Close()
		writer = file
	}

	var ch chan string
	var err error
	switch source {
	case "hive":
		ch, err = dumpFromHive(context.Background(), addr, username, since, until, clusterIDs)
	// case "kafka":
	// 	return dumpFromKafka()
	default:
		return fmt.Errorf("source %s not supported", source)
	}
	if err != nil {
		return err
	}

	for s := range ch {
		if _, err := fmt.Fprintln(writer, s); err != nil {
			return err
		}
	}
	return nil
}

func dumpFromHive(ctx context.Context, addr, username, since, until string, clusterIDs []string) (chan string, error) {
	if addr == "" || since == "" || until == "" || len(clusterIDs) == 0 {
		return nil, errors.New("invalid params")
	}
	var err error
	var host string
	var port int
	if ss := strings.Split(addr, ":"); len(ss) == 1 {
		host = ss[0]
		port = 10000
	} else if len(ss) == 2 {
		host = ss[0]
		port, err = strconv.Atoi(ss[1])
		if err != nil {
			return nil, err
		}
	}
	configuration := gohive.NewConnectConfiguration()
	configuration.Username = username
	// This may not be necessary
	// configuration.Username = "myPassword"
	connection, err := gohive.Connect(host, port, "NONE", configuration)
	if err != nil {
		return nil, err
	}
	cursor := connection.Cursor()

	query := fmt.Sprintf("SELECT key, bucket FROM common_tech.yule_s3_objects_0714 WHERE")
	cursor.Exec(ctx, query)

	if cursor.Err != nil {
		return nil, err
	}

	ch := make(chan string)
	go func() {
		defer connection.Close()
		defer cursor.Close()
		defer close(ch)
		clusterMap := make(map[string]struct{}, len(clusterIDs))
		for _, c := range clusterIDs {
			clusterMap[c] = struct{}{}
		}

		var key, bucket string
		for cursor.HasMore(ctx) {
			cursor.FetchOne(ctx, &key, &bucket)
			if cursor.Err != nil {
				return
				log.Fatal(cursor.Err)
			}
			cid := cluster.ParseClusterID(key)
			if _, ok := clusterMap[cid]; !ok {
				continue
			}
			select {
			case <-ctx.Done():
				return
			case ch <- strings.Join([]string{bucket, key}, ","):
			}
		}
	}()

	return ch, nil
}
