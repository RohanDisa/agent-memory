package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/rdisa/agent-memory-store/internal/cluster"
	netx "github.com/rdisa/agent-memory-store/internal/net"
	"github.com/rdisa/agent-memory-store/internal/node"
	"github.com/rdisa/agent-memory-store/internal/store"
	"github.com/rdisa/agent-memory-store/internal/types"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	id := flag.String("id", env("NODE_ID", "A"), "node id")
	httpAddr := flag.String("http", env("HTTP_ADDR", ":8080"), "HTTP listen address")
	peers := flag.String("peers", env("PEERS", ""), "id=host:port,... including self")
	walPath := flag.String("wal", env("WAL_PATH", ""), "WAL file path (empty = memory only)")
	ae := flag.Duration("anti-entropy", 500*time.Millisecond, "anti-entropy interval (0 disables)")
	flag.Parse()

	self := types.NodeID(*id)
	mem, err := cluster.ParsePeers(self, *peers)
	if err != nil {
		log.Fatal(err)
	}
	if *walPath == "" {
		*walPath = filepath.Join("data", strings.ToLower(string(self))+".wal")
	}
	wal, err := store.OpenWAL(*walPath)
	if err != nil {
		log.Fatal(err)
	}

	addrs := map[types.NodeID]string{}
	for _, p := range mem.Peers {
		addrs[p.ID] = p.Addr
	}
	tr := netx.NewHTTPTransport(self, addrs)

	n := node.New(node.Config{
		ID:               self,
		Peers:            mem,
		Transport:        tr,
		WAL:              node.NewReplica(self, wal),
		AntiEntropyEvery: *ae,
	})
	n.StartAntiEntropy()

	srv := node.NewHTTPServer(n, *httpAddr)
	if err := srv.Start(); err != nil {
		log.Fatal(err)
	}
	log.Printf("node %s http=%s peers=%v wal=%s", self, srv.Addr, mem.IDs(), *walPath)

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	_ = srv.Close()
	_ = n.Close()
}
