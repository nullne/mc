package cmd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/fatih/color"
	json "github.com/minio/mc/pkg/colorjson"
	"github.com/minio/mc/pkg/console"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/minio/pkg/madmin"
	prompt "github.com/segmentio/go-prompt"
	"go.uber.org/multierr"
	"golang.org/x/time/rate"
)

const (
	healStatusWait byte = '0'
	// return error from heal endpoint
	healStatusFailed byte = '2'

	// error is nil
	healStatusOk              byte = '1' // all parts incorrupted
	healStatusMissingParts    byte = '3' // missing parts
	healStatusCannnotBeHealed byte = '4' // cannot be healed for losing too much parts
)

func needProcess(status byte) bool {
	switch status {
	case healStatusOk:
		fallthrough
	case healStatusCannnotBeHealed:
		return false
	default:
		return true
	}
}

func missionComplete(status map[byte]int) bool {
	complete := true
	for k := range status {
		switch k {
		case healStatusWait:
			fallthrough
		case healStatusFailed:
			fallthrough
		case healStatusMissingParts:
			complete = false
		}
	}
	return complete
}

func processNumber(status map[byte]int) int {
	num := 0
	for k, v := range status {
		switch k {
		case healStatusWait:
			fallthrough
		case healStatusFailed:
			fallthrough
		case healStatusMissingParts:
			num += v
		}
	}
	return num
}

const (
	objectHealListFile   = "list"
	objectHealStatusFile = "status"
	objectsHealDir       = "heal/object-list"
	objectsListHealLock  = "LOCK"
)

// only following field required
// ObjectsHealed, ItemHealed int64
// HealthCols                map[col]int64
// HealOpts                  *madmin.HealOpts
// HealDuration              time.Duration
type objectListHealData struct {
	uiData
	Total   int64
	Started time.Time
}

// change the unknow result to ok
func fakeHealResult(item *madmin.HealResultItem, endpoints map[string]map[string]struct{}) {
	for i, drive := range item.Before.Drives {
		if drive.State != madmin.DriveStateUnknown {
			continue
		}
		u, err := url.Parse(drive.Endpoint)
		if err != nil {
			continue
		}
		if u.Host == "" {
			u.Host = "127.0.0.1:9000"
		}
		paths, ok := endpoints[u.Host]
		if !ok {
			item.Before.Drives[i].State = madmin.DriveStateOk
			continue
		}
		// 0 means all
		if len(paths) == 0 {
			item.Before.Drives[i].State = madmin.DriveStateMissing
			continue
		}
		if _, ok := paths[u.Path]; ok {
			item.Before.Drives[i].State = madmin.DriveStateMissing
			continue
		}
		item.Before.Drives[i].State = madmin.DriveStateOk
	}

	for i, drive := range item.After.Drives {
		if drive.State != madmin.DriveStateUnknown {
			continue
		}
		u, err := url.Parse(drive.Endpoint)
		if err != nil {
			continue
		}
		if u.Host == "" {
			u.Host = "127.0.0.1:9000"
		}
		paths, ok := endpoints[u.Host]
		if !ok {
			item.After.Drives[i].State = madmin.DriveStateOk
			continue
		}
		if _, ok := paths[u.Path]; ok {
			item.After.Drives[i].State = madmin.DriveStateMissing
			continue
		}
		item.After.Drives[i].State = madmin.DriveStateOk
	}
}

func printRes(item *madmin.HealResultItem) {
	fmt.Println(item.Bucket, item.Object)
	fmt.Println("Before")
	for _, drive := range item.Before.Drives {
		fmt.Println(drive.Endpoint, drive.State)
	}
	fmt.Println("After")
	for _, drive := range item.After.Drives {
		fmt.Println(drive.Endpoint, drive.State)
	}
}

func (ui *objectListHealData) Display(ctx context.Context, wg *sync.WaitGroup, endpoints map[string]map[string]struct{}) chan *madmin.HealResultItem {
	ch := make(chan *madmin.HealResultItem, 1000)
	firstIter := true
	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case res := <-ch:
				fakeHealResult(res, endpoints)
				ui.HealDuration = time.Now().Sub(ui.Started)
				ui.updateStats(*res)

				// Update display
				switch {
				case globalJSON:
					err = ui.printItemsJSON(res)
				// case globalQuiet:
				// 	err = ui.printItemsQuietly(res)
				default:
					select {
					case <-ticker.C:
						if firstIter {
							firstIter = false
						} else {
							if !globalQuiet && !globalJSON {
								console.RewindLines(8)
							}
						}
						err = ui.updateUI(res)
					default:
					}
				}
			}
			if err != nil {
				panic(err)
			}
		}
	}()
	return ch
}

func (ui *objectListHealData) printItemsJSON(item *madmin.HealResultItem) (err error) {
	type change struct {
		Before string `json:"before"`
		After  string `json:"after"`
	}
	type healRec struct {
		Status string `json:"status"`
		Error  string `json:"error,omitempty"`
		Type   string `json:"type"`
		Name   string `json:"name"`
		Before struct {
			Color     string                 `json:"color"`
			Offline   int                    `json:"offline"`
			Online    int                    `json:"online"`
			Missing   int                    `json:"missing"`
			Corrupted int                    `json:"corrupted"`
			Drives    []madmin.HealDriveInfo `json:"drives"`
		} `json:"before"`
		After struct {
			Color     string                 `json:"color"`
			Offline   int                    `json:"offline"`
			Online    int                    `json:"online"`
			Missing   int                    `json:"missing"`
			Corrupted int                    `json:"corrupted"`
			Drives    []madmin.HealDriveInfo `json:"drives"`
		} `json:"after"`
		Size int64 `json:"size"`
	}
	makeHR := func(h *hri) (r healRec, err error) {
		r.Status = "success"
		r.Type, r.Name = h.getHRTypeAndName()

		var b, a col
		switch h.Type {
		case madmin.HealItemMetadata, madmin.HealItemBucket:
			b, a, err = h.getReplicatedFileHCCChange()
		default:
			if h.Type == madmin.HealItemObject {
				r.Size = h.ObjectSize
			}
			b, a, err = h.getObjectHCCChange()
		}
		if err != nil {
			return r, err
		}
		r.Before.Color = strings.ToLower(string(b))
		r.After.Color = strings.ToLower(string(a))
		r.Before.Online, r.After.Online = h.GetOnlineCounts()
		r.Before.Missing, r.After.Missing = h.GetMissingCounts()
		r.Before.Corrupted, r.After.Corrupted = h.GetCorruptedCounts()
		r.Before.Offline, r.After.Offline = h.GetOfflineCounts()
		r.Before.Drives = h.Before.Drives
		r.After.Drives = h.After.Drives
		return r, nil
	}

	// for _, item := range s.Items {
	h := newHRI(item)
	r, err := makeHR(h)
	if err != nil {
		return err
	}
	jsonBytes, err := json.MarshalIndent(r, "", " ")
	fatalIf(probe.NewError(err), "Unable to marshal to JSON.")
	console.Println(string(jsonBytes))
	// }
	return nil
}

func (ui *objectListHealData) printItemsQuietly(item *madmin.HealResultItem) (err error) {
	// lpad := func(s col) string {
	// 	return fmt.Sprintf("%-6s", string(s))
	// }
	// rpad := func(s col) string {
	// 	return fmt.Sprintf("%6s", string(s))
	// }
	// printColStr := func(before, after col) {
	// 	console.PrintC("[" + lpad(before) + " -> " + rpad(after) + "] ")
	// }
	//
	// var b, a col
	// for _, item := range s.Items {
	// 	h := newHRI(&item)
	// 	switch h.Type {
	// 	case madmin.HealItemMetadata, madmin.HealItemBucket:
	// 		b, a, err = h.getReplicatedFileHCCChange()
	// 	default:
	// 		b, a, err = h.getObjectHCCChange()
	// 	}
	// 	if err != nil {
	// 		return err
	// 	}
	// 	printColStr(b, a)
	// 	hrStr := h.getHealResultStr()
	// 	switch h.Type {
	// 	case madmin.HealItemMetadata, madmin.HealItemBucketMetadata:
	// 		console.PrintC(fmt.Sprintln("**", hrStr, "**"))
	// 	default:
	// 		console.PrintC(hrStr, "\n")
	// 	}
	// }
	return nil
}

func (ui *objectListHealData) updateUI(item *madmin.HealResultItem) (err error) {
	h := newHRI(item)
	scannedStr := "** waiting for status from server **"
	if h != nil {
		scannedStr = lineTrunc(h.makeHealEntityString(), lineWidth-len("Scanned: "))
	}

	totalObjects, totalSize, totalTime := ui.getProgress()
	healedStr := fmt.Sprintf("%s objects total, %s scanned, %s healed; %s in %s",
		humanize.Comma(ui.Total), totalObjects, humanize.Comma(ui.ObjectsHealed),
		totalSize, totalTime)

	console.Print(console.Colorize("HealUpdateUI", fmt.Sprintf(" %s", <-ui.CurChan)))
	console.PrintC(fmt.Sprintf("  %s\n", scannedStr))
	console.PrintC(fmt.Sprintf("    %s\n", healedStr))

	dspOrder := []col{colGreen, colYellow, colRed, colGrey}
	msgs := []string{
		"no part corrupted",
		"missing parts",
		"one foot in the grave",
		"died",
	}
	printColors := []*color.Color{}
	for _, c := range dspOrder {
		printColors = append(printColors, getPrintCol(c))
	}
	t := console.NewTable(printColors, []bool{false, true, true}, 4)

	percentMap, barMap := ui.getPercentsNBars()
	cellText := make([][]string, len(dspOrder))
	for i := range cellText {
		cellText[i] = []string{
			// string(dspOrder[i]),
			msgs[i],
			fmt.Sprintf(humanize.Comma(ui.HealthCols[dspOrder[i]])),
			fmt.Sprintf("%5.1f%% %s", percentMap[dspOrder[i]], barMap[dspOrder[i]]),
		}
	}

	t.DisplayTable(cellText)
	return nil
}

// 1-byte status will be recored into status file
type objectHandleFunc func(bucket, key string) (status byte)

func healStatusFrom(item madmin.HealResultItem) byte {
	return healStatusOk
}

type objectListHeal struct {
	workingDir              string
	objectsFile, statusFile *os.File
}

type handleObject struct {
	bucket   string
	key      string
	callback func(status byte) error
}

func shouldResume() (ok bool) {
	return prompt.Confirm(colorGreenBold("the object list existed, choose yes to resume, no to restart [Yes|No]"))
}

func newObjectListHeal(workingDir, objectListFile string) (ol objectListHeal, err error) {
	dir := path.Join(workingDir, objectsHealDir, path.Base(objectListFile))
	ol.workingDir = dir

	create := false
	if _, err := os.Stat(dir); err == nil {
		if !shouldResume() {
			if e := os.RemoveAll(dir); e != nil {
				return ol, err
			}
			create = true
		}
	} else if os.IsNotExist(err) {
		create = true
	} else {
		return ol, err
	}

	if create {
		total, e := checkAndCountLines(objectListFile)
		if e != nil {
			return ol, e
		}
		if e := os.MkdirAll(dir, 0755); e != nil {
			return ol, e
		}
		if e := os.Link(objectListFile, path.Join(dir, objectHealListFile)); e != nil {
			return ol, e
		}
		f, e := os.Create(path.Join(dir, objectHealStatusFile))
		if e != nil {
			return ol, e
		}
		ol.statusFile = f
		for i := int64(0); i < total; i++ {
			_, e := f.Write([]byte{healStatusWait})
			if e != nil {
				return ol, e
			}
		}
		if e := f.Sync(); e != nil {
			return ol, e
		}
	}
	ol.objectsFile, err = os.OpenFile(path.Join(dir, objectHealListFile), os.O_RDONLY, 0644)
	if err != nil {
		return ol, err
	}
	if ol.statusFile == nil {
		ol.statusFile, err = os.OpenFile(path.Join(dir, objectHealStatusFile), os.O_RDWR, 0644)
		if err != nil {
			return ol, err
		}
	}
	return ol, nil
}

func (ol objectListHeal) healStatus(p bool) (map[byte]int, error) {
	if _, err := ol.statusFile.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	status := make(map[byte]int)
	scanner := bufio.NewScanner(ol.statusFile)
	scanner.Split(bufio.ScanBytes)
	for scanner.Scan() {
		bs := scanner.Bytes()
		if len(bs) != 1 {
			return nil, errors.New("invalid token read")
		}
		status[bs[0]] += 1
	}
	if p {
		for k, v := range status {
			switch k {
			case healStatusWait:
				fmt.Printf("%d wait to be healed\n", v)
			case healStatusFailed:
				fmt.Printf("%d failed to be healed\n ", v)
			case healStatusOk:
				fmt.Printf("%d have been healed\n", v)
			case healStatusMissingParts:
				fmt.Printf("%d missing parts\n", v)
			case healStatusCannnotBeHealed:
				fmt.Printf("%d canont be healed\n ", v)
			default:
				fmt.Printf("unknow status %v:%d\n", k, v)
			}
		}
	}
	return status, nil
}

func (ol *objectListHeal) run(ctx context.Context, functions []objectHandleFunc, qps int) {
	ch := ol.scan(ctx)
	var wg sync.WaitGroup
	for _, fn := range functions {
		limiter := rate.NewLimiter(rate.Every(time.Second/time.Duration(qps)), 1)
		for i := 0; i < qps; i++ {
			wg.Add(1)
			go func(fn objectHandleFunc, limiter *rate.Limiter, wg *sync.WaitGroup) {
				defer wg.Done()
				log.Println("fuck")
				for obj := range ch {
					if err := limiter.Wait(ctx); err != nil {
						// log.Println(err)
						return
					}
					if err := obj.callback(fn(obj.bucket, obj.key)); err != nil {
						log.Println(err)
						return
					}
				}
			}(fn, limiter, &wg)
		}
	}
	wg.Wait()
}

func (ol *objectListHeal) scan(ctx context.Context) chan handleObject {
	ch := make(chan handleObject)
	go func() {
		defer close(ch)
		if _, err := ol.objectsFile.Seek(0, io.SeekStart); err != nil {
			log.Println(err)
			return
		}

		scanner := bufio.NewScanner(ol.objectsFile)
		var offset int64 = -1
		bs := make([]byte, 1)
		for scanner.Scan() {
			select {
			case <-ctx.Done():
				return
			default:
			}
			offset++
			_, err := ol.statusFile.ReadAt(bs, offset)
			if err != nil {
				log.Println(err)
				return
			}
			if !needProcess(bs[0]) {
				continue
			}
			ss := strings.Split(scanner.Text(), ",")
			if len(ss) != 2 {
				log.Println("invalid format")
				continue
			}
			select {
			case <-ctx.Done():
				return
			case ch <- handleObject{
				bucket:   ss[0],
				key:      ss[1],
				callback: ol.save(offset),
			}:
			}
		}
		if err := scanner.Err(); err != nil {
			log.Println(err)
			return
		}
	}()
	return ch
}

func (ol *objectListHeal) save(offset int64) func(byte) error {
	return func(s byte) error {
		_, err := ol.statusFile.WriteAt([]byte{s}, offset)
		return err
	}
}

func (ol *objectListHeal) Close() (err error) {
	err = multierr.Append(err, ol.objectsFile.Close())
	err = multierr.Append(err, ol.statusFile.Close())
	return
}

func (ol *objectListHeal) Remove() error {
	return os.RemoveAll(ol.workingDir)
}

func checkAndCountLines(p string) (int64, error) {
	file, err := os.Open(p)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	var counter int64
	for scanner.Scan() {
		ss := strings.Split(scanner.Text(), ",")
		// length 2: bucket object-key
		// length 3: bucket object-key heal-status
		if len(ss) < 2 || len(ss) > 3 {
			return 0, errors.New("invalid format")
		}
		counter++
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	return counter, nil
}

func listObjectList(workingDir string) ([]string, error) {
	fis, err := ioutil.ReadDir(path.Join(workingDir, objectsHealDir))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var entries []string
	for _, fi := range fis {
		if !fi.IsDir() {
			continue
		}
		if strings.HasPrefix(fi.Name(), ".") {
			continue
		}
		entries = append(entries, fi.Name())
	}
	return entries, nil
}
