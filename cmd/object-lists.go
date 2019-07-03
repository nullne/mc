package cmd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/time/rate"
)

type objectListHeal struct {
	workingDir              string
	objectsFile, statusFile *os.File
	succeeded, failed       uint32
}

type handleObject struct {
	bucket   string
	key      string
	callback func(err error) error
}

const (
	healStatusWait   string = "0"
	healStatusOk     string = "1"
	healStatusFailed string = "2"
)

func newObjectList() {
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
				for obj := range ch {
					if err := limiter.Wait(ctx); err != nil {
						log.Println(err)
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
	fmt.Printf("%d object(s) are healed\n", atomic.LoadUint32(&(ol.succeeded)))
	if failed := atomic.LoadUint32(&(ol.failed)); failed != 0 {
		fmt.Printf("%d object(s) failed to be healed\n", failed)
		fmt.Println("check the result in", ol.workingDir)
	}
}

func (ol *objectListHeal) Close() error {
	ol.objectsFile.Close()
	ol.statusFile.Close()
	return nil
}

func (ol *objectListHeal) Remove() error {
	return nil
}

func (ol *objectListHeal) scan(ctx context.Context) chan handleObject {
	ch := make(chan handleObject)
	go func() {
		defer close(ch)

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
			if string(bs) == healStatusOk {
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

func (ol *objectListHeal) save(offset int64) func(error) error {
	return func(e error) error {
		var bs []byte
		if e == nil {
			bs = []byte(healStatusOk)
			atomic.AddUint32(&(ol.succeeded), 1)
		} else {
			bs = []byte(healStatusFailed)
			atomic.AddUint32(&(ol.failed), 1)
		}
		_, err := ol.statusFile.WriteAt(bs, offset)
		return err
	}
}

// only following field required
// ObjectsHealed, ItemHealed int64
// HealthCols                map[col]int64
// HealOpts                  *madmin.HealOpts
// HealDuration              time.Duration
type objectListHealData struct {
	uiData
	Total int64
}

type objectHandleFunc func(bucket, key string) error

const (
	objectHealListFile   = "list"
	objectHealStatusFile = "status"
	objectsHealDir       = "heal/object-list"
	objectsListHealLock  = "LOCK"
)

// need to lock
// ~/.mc/heal/object-list/objectsFile/
func healObjectList(workingDir, objectsFile string, functions []objectHandleFunc, qps int) error {
	if qps <= 0 {
		qps = 1
	}

	trapCh := signalTrap(os.Interrupt, syscall.SIGTERM, syscall.SIGKILL)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-trapCh:
			cancel()
		}
	}()

	dir := path.Join(workingDir, objectsHealDir, path.Base(objectsFile))
	ol := objectListHeal{
		workingDir: dir,
	}
	// create one if not existed
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		total, e := checkAndCountLines(objectsFile)
		if e != nil {
			return e
		}
		if e := os.MkdirAll(dir, 0755); e != nil {
			return e
		}
		if e := os.Link(objectsFile, path.Join(dir, objectHealListFile)); e != nil {
			return e
		}
		f, e := os.Create(path.Join(dir, objectHealStatusFile))
		if e != nil {
			return e
		}
		ol.statusFile = f
		defer f.Close()
		for i := int64(0); i < total; i++ {
			_, e := f.Write([]byte(healStatusWait))
			if e != nil {
				return e
			}
		}
	} else if err != nil {
		return err
	}

	var err error
	ol.objectsFile, err = os.OpenFile(path.Join(dir, objectHealListFile), os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer ol.objectsFile.Close()
	ol.statusFile, err = os.OpenFile(path.Join(dir, objectHealStatusFile), os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer ol.statusFile.Close()

	ol.run(ctx, functions, qps)
	return nil
}

// func displayHealObjectLists() {
// 	for res := range ch {
//
// 	}
// }
//
// func healObjectWorker(client *madmin.AdminClient, objectCh chan handleObject, resultCh chan string, stopCh chan bool, limiter *rate.Limiter) {
// 	for obj := range objectCh {
// 		limiter.Wait(context.Background())
// 		res, err := client.HealObject(obj.bucket, obj.key)
// 		if err != nil {
// 			continue
// 		}
// 		select {
// 		case <-stopCh:
// 			return
// 		case resultCh <- res:
// 		}
// 	}
// }

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
