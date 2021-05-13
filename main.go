package main

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

var once = false

type result struct {
	id    string
	title string
	plot  string
}

type filter struct {
	index int
	value string
}

const (
	CHAN_SIZE = 128
	READ_SIZE = 2 * 1024
	N_THREAD  = 20
)

func process(
	results *[]result,
	resultsMtx *sync.Mutex,
	filters []filter,
	sigCh chan os.Signal,
	readCh chan []byte,
	dataCh chan []byte,
	wg *sync.WaitGroup,
	zr *gzip.Reader) {
	buf := make([]byte, READ_SIZE)
	var err error
	var n, i, offset int

	defer wg.Done()

	for {
		select {
		case <-sigCh:
			sigCh <- syscall.SIGINT
			return
		case data := <-dataCh:
			lines := strings.Split(string(data), "\n")
			for _, line := range lines {
				vs := strings.Split(line, "\t")
				for _, filter := range filters {
					if vs[filter.index] != filter.value {
						goto SKIP
					}
				}
				{
					var omdbResp struct {
						title string
						plot  string
					}
					if !once {
						once = true

						if resp, err := http.Get("http://www.omdbapi.com?i=" + vs[0]); err != nil {
							log.Fatal(err)
						} else if err = json.NewDecoder(resp.Body).Decode(&omdbResp); err != nil {
							log.Fatal(err)
						} else if err = resp.Body.Close(); err != nil {
							log.Fatal(err)
						}
					}
					resultsMtx.Lock()
					*results = append(*results, result{id: vs[0], title: omdbResp.title, plot: omdbResp.plot})
					resultsMtx.Unlock()
				}
			SKIP:
			}
			break
		case prefix := <-readCh:
			n, err = zr.Read(buf)
			if err == io.EOF {
				fmt.Printf("Done\n")
				return
			}
			if err != nil {
				log.Fatal(err)
			}
			if n == 0 {
				readCh <- make([]byte, 0)
			} else {
				for i = n - 1; i != 0; i-- {
					if buf[i] == 0xA {
						break
					}
				}
				nextPrefix := make([]byte, n-i)
				copy(nextPrefix, buf[i+1:n])
				readCh <- nextPrefix
				offset = len(prefix)
				data := make([]byte, offset+i)
				copy(data, prefix)
				copy(data[offset:], buf[:i])
				dataCh <- data
			}
			break
		}

	}
}

func main() {
	sigCh := make(chan os.Signal, 1)
	go func() {
		time.Sleep(1000 * time.Second)
		sigCh <- syscall.SIGINT
	}()

	if fp, err := os.Open("./title.basics.tsv.gz"); err != nil {
		log.Fatal(err)
	} else if zr, err := gzip.NewReader(fp); err != nil {
		log.Fatal(err)
	} else {
		var results []result
		var resultsMtx sync.Mutex
		filters := []filter{{1, "movie"}}
		readCh := make(chan []byte, CHAN_SIZE)
		dataCh := make(chan []byte, CHAN_SIZE)
		var wg sync.WaitGroup

		signal.Notify(sigCh, syscall.SIGINT)
		for i := 0; i != N_THREAD; i++ {
			go process(&results, &resultsMtx, filters, sigCh, readCh, dataCh, &wg, zr)
		}

		wg.Add(N_THREAD)
		readCh <- make([]byte, 0)
		wg.Wait()

		zr.Close()
		fp.Close()
		fmt.Println("\nIMDB_ID     |   Title               |   Plot")
		for _, result := range results {
			fmt.Printf("%s | %s | %s\n", result.id, result.title, result.plot)
		}
	}
}
