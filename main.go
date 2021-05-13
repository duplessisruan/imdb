package main

import (
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	OMDB_KEY  = "d171b5cf"
	CHAN_SIZE = 128
	READ_SIZE = 8 * 1024
	N_THREAD  = 1024
)

type filter struct {
	index int
	value string
}

type parameters struct {
	filters     []filter
	reqCount    int
	reqCountMtx sync.Mutex
	plotRe      *regexp.Regexp
}

type result struct {
	id    string
	title string
	plot  string
}

func process(
	results *[]result,
	resultsMtx *sync.Mutex,
	params *parameters,
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
		case sig := <-sigCh:
			sigCh <- sig
			return
		case data := <-dataCh:
			lines := strings.Split(string(data), "\n")
			for _, line := range lines {
				vs := strings.Split(line, "\t")
				for _, filter := range params.filters {
					if vs[filter.index] != filter.value {
						goto SKIP
					}
				}
				{
					var omdbResp struct{ Plot string }
					params.reqCountMtx.Lock()
					if params.reqCount == 0 {
						params.reqCountMtx.Unlock()
					} else {
						params.reqCount--
						params.reqCountMtx.Unlock()
						if resp, err := http.Get("http://www.omdbapi.com?" + "apikey=" + OMDB_KEY + "&i=" + vs[0]); err != nil {
							log.Fatal(err)
						} else if err = json.NewDecoder(resp.Body).Decode(&omdbResp); err != nil {
							log.Fatal(err)
						} else if err = resp.Body.Close(); err != nil {
							log.Fatal(err)
						}
					}
					if params.plotRe != nil {
						if !params.plotRe.MatchString(omdbResp.Plot) {
							goto SKIP
						}
					}
					resultsMtx.Lock()
					*results = append(*results, result{id: vs[0], title: vs[2], plot: omdbResp.Plot})
					resultsMtx.Unlock()
				}
			SKIP:
			}
			break
		case prefix := <-readCh:
			n, err = zr.Read(buf)
			if err == io.EOF {
				sigCh <- syscall.SIGKILL
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

func parse(params *parameters) (string, time.Duration) {
	filePath := flag.String("filePath", "./title.basics.tsv.gz", "absolute path to the inflated `title.basics.tsv.gz` file")

	titleType := flag.String("titleType", "", "filter on `titleType` column")
	primaryTitle := flag.String("primaryTitle", "", "filter on `primaryTitle` column")
	originalTitle := flag.String("originalTitle", "", "filter on `originalTitle` column")
	startYear := flag.String("startYear", "", "filter on `startYear` column")
	endYear := flag.String("endYear", "", "filter on `endYear` column")
	runtimeMinutes := flag.String("runtimeMinutes", "", "filter on `runtimeMinutes` column")
	genres := flag.String("genres", "", "filter on `genres` column")

	maxRunTime := flag.String("maxRunTime", "10m", "maximum run time of the application. Format is a `time.Duration` string see [here](https://godoc.org/time#ParseDuration)")

	maxApiRequests := flag.Int("maxApiRequests", 0, "maximum number of requests to be made to [omdbapi](https://www.omdbapi.com/)")
	maxRequests := flag.Int("maxRequests", 0, "maximum number of requests to send to [omdbapi](https://www.omdbapi.com/)")
	plotFilter := flag.String("plotFilter", "", "regex pattern to apply to the plot of a film retrieved from [omdbapi](https://www.omdbapi.com/)")

	flag.Parse()

	duration, err := time.ParseDuration(*maxRunTime)
	if err != nil {
		log.Fatal(err)
	}

	if *maxApiRequests != 0 {
		params.reqCount = *maxApiRequests
	} else {
		params.reqCount = *maxRequests
	}

	if *titleType != "" {
		params.filters = append(params.filters, filter{1, *titleType})
	}
	if *primaryTitle != "" {
		params.filters = append(params.filters, filter{2, *primaryTitle})
	}
	if *originalTitle != "" {
		params.filters = append(params.filters, filter{3, *originalTitle})
	}
	if *startYear != "" {
		params.filters = append(params.filters, filter{5, *startYear})
	}
	if *endYear != "" {
		params.filters = append(params.filters, filter{6, *startYear})
	}
	if *runtimeMinutes != "" {
		params.filters = append(params.filters, filter{7, *startYear})
	}
	if *genres != "" {
		params.filters = append(params.filters, filter{8, *startYear})
	}

	if *plotFilter != "" {
		if params.plotRe, err = regexp.Compile(*plotFilter); err != nil {
			log.Fatal(err)
		}
	}

	return *filePath, duration
}

func main() {
	var params parameters
	filePath, timeout := parse(&params)
	sigCh := make(chan os.Signal, 1)
	go func() {
		time.Sleep(timeout)
		select {
		case sig := <-sigCh:
			sigCh <- sig
			break
		default:
			sigCh <- syscall.SIGKILL
		}
	}()

	if fp, err := os.Open(filePath); err != nil {
		log.Fatal(err)
	} else if zr, err := gzip.NewReader(fp); err != nil {
		log.Fatal(err)
	} else {
		results := []result{{id: "IMDB_ID", title: "Title", plot: "Plot"}}
		{
			var resultsMtx sync.Mutex
			readCh := make(chan []byte, CHAN_SIZE)
			dataCh := make(chan []byte, CHAN_SIZE)
			var wg sync.WaitGroup

			signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
			for i := 0; i != N_THREAD; i++ {
				go process(&results, &resultsMtx, &params, sigCh, readCh, dataCh, &wg, zr)
			}

			wg.Add(N_THREAD)
			readCh <- make([]byte, 0)
			wg.Wait()

			zr.Close()
			fp.Close()
		}

		select {
		case sig := <-sigCh:
			if sig == syscall.SIGINT || sig == syscall.SIGTERM {
				return
			}
		}

		idSize := 0
		titleSize := 0
		for _, result := range results {
			if idSize < len(result.id) {
				idSize = len(result.id)
			}
			if titleSize < len(result.title) {
				titleSize = len(result.title)
			}
		}
		for _, result := range results {
			fmt.Printf("%-*s   |   %-*s    |   ", idSize, result.id, titleSize, result.title)
			if result.plot == "" {
				fmt.Println(nil)
			} else {
				fmt.Println(result.plot)
			}
		}
	}
}
