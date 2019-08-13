package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/fatih/color"
	"github.com/minio/cli"
	json "github.com/minio/mc/pkg/colorjson"
	"github.com/minio/mc/pkg/console"
	"github.com/minio/minio/cmd/logger"
	kafka "github.com/segmentio/kafka-go"
	"gitlab.p1staff.com/common-tech/tantan-object-storage/cluster"
	"gitlab.p1staff.com/common-tech/tantan-object-storage/schema"
)

// dump specific flags.
var (
	dumpFlags = []cli.Flag{
		cli.StringFlag{
			Name:  "source",
			Value: "kafka",
			Usage: "from where to dump, available choices: kafka(default)",
		},
		cli.StringFlag{
			Name:  "addr",
			Usage: "address with corresponding type",
		},
		cli.StringFlag{
			Name:  "topic",
			Usage: "kafka topic",
		},
		// cli.StringFlag{
		// 	Name:  "username",
		// 	Usage: "set username if needed",
		// },
		cli.StringFlag{
			Name:  "output, o",
			Value: "",
			Usage: "output to specified file",
		},
		// filters
		cli.StringFlag{
			Name:  "since",
			Usage: "start of the time range(CST), format like 2019-08-10 12:00:00:00",
		},
		cli.StringFlag{
			Name:  "until",
			Usage: "end of the time range(CST), format like 2019-08-10 12:00:00:00",
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
	Flags: dumpFlags,
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS] TARGET [TARGET ...]

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}
EXAMPLES:
   1. Dump object list of cluster 01 within the given time range from kafka
	  $ {{.HelpName}} --source kafka --addr node.kafka.tt:9092 --topic "objects-meta-minio" --since "2019-07-10 00:00:00" --until "2019-07-11 00:00:00" --cluster-id "01" --output /tmp/objects-list-from-kafka
`,
}

const dumpLayout = "2006-01-02 15:04:05 MST"

func checkDumpSyntax(ctx *cli.Context) {
	cli.ShowCommandHelpAndExit(ctx, "dump", 1) // last argument is exit code
}

// mainList - is a handler for mc dump command
func mainDump(ctx *cli.Context) error {
	// Additional command specific theme customization.
	console.SetColor("File", color.New(color.Bold))
	console.SetColor("Dir", color.New(color.FgCyan, color.Bold))
	console.SetColor("Size", color.New(color.FgYellow))
	console.SetColor("Time", color.New(color.FgGreen))

	// check 'dump' cli arguments.
	checkDumpSyntax(ctx)

	// Set command flags from context.
	since, err := time.Parse(dumpLayout, ctx.String("since")+" CST")
	if err != nil {
		return err
	}
	until, err := time.Parse(dumpLayout, ctx.String("until")+" CST")
	if err != nil {
		return err
	}
	addr := ctx.String("addr")
	clusterIDs := ctx.StringSlice("cluster-id")
	inClusters := func(id string) bool {
		for _, cid := range clusterIDs {
			if cid == id {
				return true
			}
		}
		return false
	}
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
	switch source {
	// case "hive":
	// 	username := ctx.String("username")
	// 	ch, err = dumpFromHive(context.Background(), addr, username, since, until, inClusters)
	case "kafka":
		topic := ctx.String("topic")
		ch, err = dumpFromKafka(context.Background(), addr, topic, since, until, inClusters)
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

func dumpFromKafka(ctx context.Context, addr, topic string, since, until time.Time, inClusters func(string) bool) (chan string, error) {
	conn, err := kafka.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return nil, err
	}

	ch := make(chan string, 1000)
	ech := make(chan error, len(partitions))
	var wg sync.WaitGroup
	for _, p := range partitions {
		wg.Add(1)
		go fetchKafkaMessages(ctx, &wg, ch, ech, addr, topic, p.ID, since, until, inClusters)
	}
	go func() {
		wg.Wait()
		close(ch)
		close(ech)
	}()
	go func() {
		for err := range ech {
			fmt.Println(err)
		}
	}()
	return ch, nil
}

func fetchKafkaMessages(ctx context.Context, wg *sync.WaitGroup, ch chan string, ech chan error, addr, topic string, partitionID int, since, until time.Time, inClusters func(string) bool) {
	defer wg.Done()
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{addr},
		Topic:     topic,
		Partition: partitionID,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	if err := r.SetOffsetAt(ctx, since); err != nil {
		ech <- err
		return
	}

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			ech <- err
			return
		}
		if m.Time.After(until) {
			return
		}
		var obj schema.Object
		if err := json.Unmarshal(m.Value, &obj); err != nil {
			continue
		}
		if !inClusters(cluster.ParseClusterID(obj.Key)) {
			continue
		}
		select {
		case ch <- fmt.Sprintf("%s,%s", obj.BucketName, obj.Key):
		case <-ctx.Done():
			return
		}
	}
}

func dumpFromHive(ctx context.Context, addr, username string, since, until time.Time, inClusters func(string) bool) (chan string, error) {
	return nil, nil
	// if addr == "" || since == "" || until == "" || len(clusterIDs) == 0 {
	// 	return nil, errors.New("invalid params")
	// }
	// var err error
	// var host string
	// var port int
	// if ss := strings.Split(addr, ":"); len(ss) == 1 {
	// 	host = ss[0]
	// 	port = 10000
	// } else if len(ss) == 2 {
	// 	host = ss[0]
	// 	port, err = strconv.Atoi(ss[1])
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// }
	// configuration := gohive.NewConnectConfiguration()
	// configuration.Username = username
	// // This may not be necessary
	// // configuration.Username = "myPassword"
	// connection, err := gohive.Connect(host, port, "NONE", configuration)
	// if err != nil {
	// 	return nil, err
	// }
	// cursor := connection.Cursor()
	//
	// query := fmt.Sprintf("SELECT key, bucket FROM common_tech.yule_s3_objects_0714 WHERE")
	// cursor.Exec(ctx, query)
	//
	// if cursor.Err != nil {
	// 	return nil, err
	// }
	//
	// ch := make(chan string)
	// go func() {
	// 	defer connection.Close()
	// 	defer cursor.Close()
	// 	defer close(ch)
	// 	clusterMap := make(map[string]struct{}, len(clusterIDs))
	// 	for _, c := range clusterIDs {
	// 		clusterMap[c] = struct{}{}
	// 	}
	//
	// 	var key, bucket string
	// 	for cursor.HasMore(ctx) {
	// 		cursor.FetchOne(ctx, &key, &bucket)
	// 		if cursor.Err != nil {
	// 			return
	// 			log.Fatal(cursor.Err)
	// 		}
	// 		cid := cluster.ParseClusterID(key)
	// 		if _, ok := clusterMap[cid]; !ok {
	// 			continue
	// 		}
	// 		select {
	// 		case <-ctx.Done():
	// 			return
	// 		case ch <- strings.Join([]string{bucket, key}, ","):
	// 		}
	// 	}
	// }()
	//
	// return ch, nil
}
