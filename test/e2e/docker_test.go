//go:build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func composeDir(t *testing.T) string {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "../.."))
}

func TestDockerComposeConflictAndKill(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not installed")
	}
	root := composeDir(t)
	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command("docker", append([]string{"compose"}, args...)...)
		cmd.Dir = root
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			t.Fatalf("docker compose %v: %v", args, err)
		}
	}
	run("up", "-d", "--build")
	t.Cleanup(func() { run("down", "-v") })

	waitHealth(t, "http://127.0.0.1:8081/health")
	waitHealth(t, "http://127.0.0.1:8082/health")
	waitHealth(t, "http://127.0.0.1:8083/health")

	// Two sessions, contradicting plan facts.
	postJSON(t, "http://127.0.0.1:8081/fact", map[string]any{
		"entity": "customer-e2e", "attribute": "plan", "value": "enterprise", "policy": "LWW", "w": 2,
	})
	postJSON(t, "http://127.0.0.1:8082/fact", map[string]any{
		"entity": "customer-e2e", "attribute": "plan", "value": "free", "policy": "LWW", "w": 2,
	})
	got := getJSON(t, "http://127.0.0.1:8083/fact?entity=customer-e2e&attribute=plan&r=2")
	if got["error"] != nil && got["error"] != "" {
		t.Fatalf("read: %v", got)
	}

	// Kill C, cluster must keep serving on A.
	run("kill", "node-c")
	time.Sleep(300 * time.Millisecond)
	wr := postJSON(t, "http://127.0.0.1:8081/write", map[string]any{
		"key": "survive:kill", "data": "yes", "policy": "LWW", "w": 2,
	})
	if ok, _ := wr["OK"].(bool); !ok {
		if ok2, _ := wr["ok"].(bool); !ok2 {
			t.Fatalf("write during kill: %v", wr)
		}
	}

	run("start", "node-c")
	waitHealth(t, "http://127.0.0.1:8083/health")
	// poke anti-entropy from A
	postJSON(t, "http://127.0.0.1:8081/debug/anti-entropy", map[string]any{})
	postJSON(t, "http://127.0.0.1:8081/debug/handoff", map[string]any{})
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		kv := getJSON(t, "http://127.0.0.1:8083/debug/kv?key=survive:kill")
		if found, _ := kv["found"].(bool); found {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("node-c did not catch up after restart")
}

func waitHealth(t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", url)
}

func postJSON(t *testing.T, url string, body any) map[string]any {
	t.Helper()
	b, _ := json.Marshal(body)
	resp, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return out
}

func getJSON(t *testing.T, url string) map[string]any {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return out
}
