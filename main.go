package main

import (
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
)

const READ_SIZE = 2 * 1024
const N_THREAD = 20

func process(readCh *chan []byte, dataCh *chan []byte, wg *sync.WaitGroup, zr *gzip.Reader) {
	buf := make([]byte, READ_SIZE)
	var err error
	var n, i, offset int

	for {
		select {
		case data := <-(*dataCh):
			fmt.Printf("Data: %s\n\n\n", string(data))
			break
		case prefix := <-(*readCh):
			n, err = zr.Read(buf)
			if err == io.EOF {
				wg.Done()
				return
			}
			if err != nil {
				log.Fatal(err)
			}
			if n == 0 {
				(*readCh) <- make([]byte, 0)
			} else {
				for i = n - 1; i != 0; i-- {
					if buf[i] == 0xA {
						break
					}
				}
				nextPrefix := make([]byte, n-i)
				copy(nextPrefix, buf[i+1:n])
				(*readCh) <- nextPrefix
				offset = len(prefix)
				data := make([]byte, offset+i)
				copy(data, prefix)
				copy(data[offset:], buf[:i])
				(*dataCh) <- data
			}
			break
		}

	}

}

func main() {
	if fp, err := os.Open("./title.basics.tsv.gz"); err != nil {
		log.Fatal(err)
	} else if zr, err := gzip.NewReader(fp); err != nil {
		fp.Close()
		log.Fatal(err)
	} else {
		defer func() {
			zr.Close()
			fp.Close()
			fmt.Printf("Completed\n")
		}()

		{
			readCh := make(chan []byte)
			dataCh := make(chan []byte)
			var wg sync.WaitGroup
			for i := 0; i != N_THREAD; i++ {
				go process(&readCh, &dataCh, &wg, zr)
			}
			wg.Add(N_THREAD)
			readCh <- make([]byte, 0)
			wg.Wait()
		}

	}

}
