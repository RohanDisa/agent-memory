package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

func main() {
	addr := flag.String("addr", env("MEM_ADDR", "http://127.0.0.1:8081"), "node HTTP base URL")
	flag.Parse()
	args := flag.Args()
	if len(args) < 1 {
		usage()
		os.Exit(2)
	}
	c := &http.Client{Timeout: 5 * time.Second}
	var err error
	switch args[0] {
	case "write":
		err = cmdWrite(c, *addr, args[1:])
	case "read":
		err = cmdRead(c, *addr, args[1:])
	case "put-fact":
		err = cmdFact(c, *addr, args[1:], false)
	case "get-fact":
		err = cmdFact(c, *addr, args[1:], true)
	case "add-tag":
		err = cmdTag(c, *addr, args[1:], http.MethodPost)
	case "remove-tag":
		err = cmdTag(c, *addr, args[1:], http.MethodDelete)
	case "history":
		err = cmdHistory(c, *addr, args[1:])
	case "health":
		err = dump(c, http.MethodGet, *addr+"/health", nil)
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `memctl — client for the agent memory store

  memctl [-addr URL] write   -key K -data V [-w 2] [-policy LWW]
  memctl [-addr URL] read    -key K [-r 2]
  memctl [-addr URL] put-fact -entity E -attr A -value V [-w 2] [-policy LWW]
  memctl [-addr URL] get-fact -entity E -attr A [-r 2]
  memctl [-addr URL] add-tag|remove-tag -entity E -tag T [-w 2]
  memctl [-addr URL] history -key K
  memctl [-addr URL] health
`)
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func cmdWrite(c *http.Client, addr string, args []string) error {
	fs := flag.NewFlagSet("write", flag.ExitOnError)
	key := fs.String("key", "", "")
	data := fs.String("data", "", "")
	w := fs.Int("w", 2, "")
	pol := fs.String("policy", "LWW", "")
	_ = fs.Parse(args)
	return dump(c, http.MethodPost, addr+"/write", map[string]any{
		"key": *key, "data": *data, "w": *w, "policy": *pol,
	})
}

func cmdRead(c *http.Client, addr string, args []string) error {
	fs := flag.NewFlagSet("read", flag.ExitOnError)
	key := fs.String("key", "", "")
	r := fs.Int("r", 2, "")
	_ = fs.Parse(args)
	return dump(c, http.MethodGet, fmt.Sprintf("%s/read?key=%s&r=%d", addr, *key, *r), nil)
}

func cmdFact(c *http.Client, addr string, args []string, get bool) error {
	fs := flag.NewFlagSet("fact", flag.ExitOnError)
	e := fs.String("entity", "", "")
	a := fs.String("attr", "", "")
	v := fs.String("value", "", "")
	w := fs.Int("w", 2, "")
	r := fs.Int("r", 2, "")
	pol := fs.String("policy", "LWW", "")
	_ = fs.Parse(args)
	if get {
		return dump(c, http.MethodGet, fmt.Sprintf("%s/fact?entity=%s&attribute=%s&r=%d", addr, *e, *a, *r), nil)
	}
	return dump(c, http.MethodPost, addr+"/fact", map[string]any{
		"entity": *e, "attribute": *a, "value": *v, "w": *w, "policy": *pol,
	})
}

func cmdTag(c *http.Client, addr string, args []string, method string) error {
	fs := flag.NewFlagSet("tag", flag.ExitOnError)
	e := fs.String("entity", "", "")
	tag := fs.String("tag", "", "")
	w := fs.Int("w", 2, "")
	_ = fs.Parse(args)
	return dump(c, method, addr+"/tag", map[string]any{"entity": *e, "tag": *tag, "w": *w})
}

func cmdHistory(c *http.Client, addr string, args []string) error {
	fs := flag.NewFlagSet("history", flag.ExitOnError)
	key := fs.String("key", "", "")
	_ = fs.Parse(args)
	return dump(c, http.MethodGet, addr+"/history?key="+*key, nil)
}

func dump(c *http.Client, method, url string, body any) error {
	var rdr io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	fmt.Printf("%s\n%s\n", resp.Status, b)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}
