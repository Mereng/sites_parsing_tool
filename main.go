package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"golang.org/x/net/html"
	"net/http"
	"os"
	"path"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"
)

type categoryFile struct {
	m   sync.Mutex
	raw strings.Builder
}

func (f *categoryFile) add(link, title, description string) {
	f.m.Lock()
	f.raw.WriteString(link)
	f.raw.WriteString("\t")
	f.raw.WriteString(title)
	f.raw.WriteString("\t")
	f.raw.WriteString(description)
	f.raw.WriteString("\n")
	f.m.Unlock()
}

type categoryFilesMap struct {
	sync.RWMutex
	m map[string]*categoryFile
}

func (m *categoryFilesMap) get(category string) (*categoryFile, bool) {
	m.RLock()
	f, ok := m.m[category]
	m.RUnlock()
	return f, ok
}

func (m *categoryFilesMap) new(category string) *categoryFile {
	m.Lock()
	f, ok := m.m[category]
	if !ok {
		f = &categoryFile{}
		m.m[category] = f
	}
	m.Unlock()
	return f
}

type item struct {
	Url        string   `json:"url"`
	Categories []string `json:"categories"`
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("usage: parsing <input file> <output path>")
		os.Exit(1)
	}

	f, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Printf("cannot open the file: %s\n", err)
		os.Exit(1)
	}

	if _, err := os.Stat(os.Args[2]); os.IsNotExist(err) {
		fmt.Println("no such output path")
		os.Exit(1)
	}

	filesMap := &categoryFilesMap{m:make(map[string]*categoryFile)}
	ch := make(chan []byte, 1)
	var wg sync.WaitGroup
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		wg.Add(1)
		go worker(&wg, ch, filesMap)
	}

	r := bufio.NewReader(f)

	for {
		isPrefix := true
		var line, partLine []byte
		var err error

		for isPrefix && err == nil {
			partLine, isPrefix, err = r.ReadLine()
			line = append(line, partLine...)
		}
		if err != nil {
			break
		}

		if len(line) == 0 {
			continue
		}

		ch <- line
	}
	close(ch)
	f.Close()

	wg.Wait()

	for name, cf := range filesMap.m {
		f, err := os.Create(path.Join(os.Args[2], name + ".tsv"))
		if err != nil {
			fmt.Printf("cannot open file for %s category: %s\n", name, err)
			os.Exit(1)
		}

		f.WriteString(cf.raw.String())
		f.Close()
	}
}

var regexSpace = regexp.MustCompile(`\s+`)

func worker(wg *sync.WaitGroup, ch <-chan []byte,  filesMap *categoryFilesMap) {
	cli := http.Client{Timeout: 15 * time.Second}

	for line := range ch {
		i := &item{}
		if err := json.Unmarshal(line, i); err != nil {
			fmt.Fprintf(os.Stderr, "cannot unmarshal %s: %s\n", line, err)
			continue
		}

		resp, err := cli.Get(i.Url)
		if err != nil {
			fmt.Fprintf(os.Stderr, "cannot fetch %s: %s\n", i.Url, err)
			continue
		}
		if resp.StatusCode >= 300 {
			fmt.Fprintf(os.Stderr, "%s returned status %d\n", i.Url, resp.StatusCode)
			continue
		}

		tokens := html.NewTokenizer(resp.Body)
		title := ""
		description := ""
		titleFind := false
		descriptionFind := false
		for {
			tt := tokens.Next()
			err := false
			switch tt {
			case html.ErrorToken:
				err = true
			case html.StartTagToken, html.EndTagToken, html.SelfClosingTagToken:
				tkn := tokens.Token()
				switch tkn.Data {
				case "title":
					if tt == html.StartTagToken {
						titleText := tokens.Next()
						if titleText == html.TextToken {
							title = strings.TrimSpace(regexSpace.ReplaceAllString(tokens.Token().Data, " "))
						}
						titleFind = true
					}
				case "meta":
					for _, attr := range tkn.Attr {
						if attr.Key == "name" {
							if strings.ToLower(attr.Val) == "description" {
								for _, attr := range tkn.Attr {
									if attr.Key == "content" {
										description = strings.TrimSpace(regexSpace.ReplaceAllString(attr.Val, " "))
										break
									}
								}
								descriptionFind = true
								break
							}
						}
					}
				}
			}
			if (titleFind && descriptionFind) || err {
				break
			}
		}
		resp.Body.Close()

		if len(i.Categories) > 0 {
			for _, c := range i.Categories {
				f, ok := filesMap.get(c)
				if !ok {
					f = filesMap.new(c)
				}
				f.add(i.Url, title, description)
			}
		} else {
			f, ok := filesMap.get("unknown_category")
			if !ok {
				f = filesMap.new("unknown_category")
			}
			f.add(i.Url, title, description)
		}

		fmt.Printf("handled %s\n", i.Url)
	}
	wg.Done()
}
