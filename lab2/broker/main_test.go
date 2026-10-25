package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"lab2/common"
	"lab2/gen/brokerpb"
)

// Teste pe binarul real: go build ./broker, proces separat, semnale reale.

var binary string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "lab2-broker-bin")
	if err != nil {
		panic(err)
	}
	binary = filepath.Join(dir, "broker")
	if out, err := exec.Command("go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "go build: %v\n%s", err, out)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

type proc struct {
	cmd  *exec.Cmd
	addr string
	mu   sync.Mutex
	logs strings.Builder
	exit chan error
}

func (p *proc) output() string { p.mu.Lock(); defer p.mu.Unlock(); return p.logs.String() }

// run pornește binarul în dir, cu mediul dat, și așteaptă linia "broker pornit".
func run(t *testing.T, dir string, env ...string) *proc {
	t.Helper()
	p := &proc{cmd: exec.Command(binary), exit: make(chan error, 1)}
	p.cmd.Dir = dir
	p.cmd.Env = append(os.Environ(), env...)
	out, _ := p.cmd.StdoutPipe()
	p.cmd.Stderr = p.cmd.Stdout
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	started := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			line := sc.Text()
			p.mu.Lock()
			p.logs.WriteString(line + "\n")
			p.mu.Unlock()
			var ev struct{ Event, Addr string }
			if json.Unmarshal([]byte(line), &ev) == nil && ev.Event == "broker.started" {
				started <- ev.Addr
			}
		}
		p.exit <- p.cmd.Wait()
	}()
	t.Cleanup(func() { p.cmd.Process.Kill() })
	select {
	case p.addr = <-started:
	case err := <-p.exit:
		t.Fatalf("brokerul s-a oprit la pornire: %v\n%s", err, p.output())
	case <-time.After(5 * time.Second):
		t.Fatalf("brokerul nu a pornit\n%s", p.output())
	}
	return p
}

func (p *proc) wait(t *testing.T) error {
	t.Helper()
	select {
	case err := <-p.exit:
		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("procesul nu s-a oprit\n%s", p.output())
		return nil
	}
}

func client(t *testing.T, addr string) brokerpb.BrokerClient {
	conn, err := common.Dial(addr, common.ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return brokerpb.NewBrokerClient(conn)
}

func freeAddr(t *testing.T) string {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	return ln.Addr().String()
}

// #1 + #6: binarul real răspunde la health, reflection e activ, iar la SIGTERM health
// devine NOT_SERVING, receiverele primesc UNAVAILABLE și procesul iese cu codul 0.
func TestSIGTERMShutsDownGracefully(t *testing.T) {
	dir := t.TempDir()
	p := run(t, dir, "BROKER_ADDR=127.0.0.1:0", "BROKER_DATA_DIR=", "SHUTDOWN_DRAIN_MS=300")
	if !strings.Contains(p.output(), `"journal":""`) {
		t.Fatalf("BROKER_DATA_DIR gol trebuia să dezactiveze jurnalul:\n%s", p.output())
	}
	conn, _ := common.Dial(p.addr, common.ClientOptions{})
	defer conn.Close()
	hc := healthpb.NewHealthClient(conn)
	watch, err := hc.Watch(context.Background(), &healthpb.HealthCheckRequest{Service: "broker.v1.Broker"})
	if err != nil {
		t.Fatal(err)
	}
	if r, err := watch.Recv(); err != nil || r.Status != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("health la pornire: %v %v", r, err)
	}
	stream, _ := brokerpb.NewBrokerClient(conn).Subscribe(context.Background(), &brokerpb.SubscribeRequest{ReceiverId: "r", Topics: []string{"news"}})
	stream.Header()

	p.cmd.Process.Signal(syscall.SIGTERM)
	if r, err := watch.Recv(); err != nil || r.Status != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("health după SIGTERM: %v %v", r, err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.Unavailable {
		t.Fatalf("Subscribe după SIGTERM: %v", err)
	}
	if err := p.wait(t); err != nil {
		t.Fatalf("cod de ieșire: %v\n%s", err, p.output())
	}
	if _, err := os.Stat(filepath.Join(dir, "data")); !os.IsNotExist(err) {
		t.Fatalf("directorul data/ a fost creat deși persistența e dezactivată: %v", err)
	}
	for _, ev := range []string{`"signal":"terminated"`, `"event":"broker.stopping"`, `"reason":"shutdown"`, `"event":"broker.stopped"`} {
		if !strings.Contains(p.output(), ev) {
			t.Fatalf("lipsește %s din log:\n%s", ev, p.output())
		}
	}
}

// #4: binarul citește .env din directorul curent; mediul real are prioritate.
func TestReadsDotEnv(t *testing.T) {
	dir, addr := t.TempDir(), freeAddr(t)
	os.WriteFile(filepath.Join(dir, ".env"), []byte("BROKER_ADDR="+addr+"\nBROKER_DATA_DIR=din-env\nMAX_QUEUE=7\n"), 0o644)
	p := run(t, dir, "MAX_QUEUE=2") // MAX_QUEUE din mediu bate .env
	if p.addr != addr {
		t.Fatalf("adresa: %s, așteptat %s din .env", p.addr, addr)
	}
	if !strings.Contains(p.output(), `"keys":["BROKER_ADDR","BROKER_DATA_DIR"]`) {
		t.Fatalf("log fără cheile din .env:\n%s", p.output())
	}
	if _, err := os.Stat(filepath.Join(dir, "din-env", "broker.journal")); err != nil {
		t.Fatalf("BROKER_DATA_DIR din .env nu a fost folosit: %v", err)
	}
	c := client(t, p.addr)
	sub, _ := c.Subscribe(context.Background(), &brokerpb.SubscribeRequest{ReceiverId: "r", Topics: []string{"news"}})
	sub.Header()
	p.cmd.Process.Signal(syscall.SIGTERM)
	p.wait(t)

	// MAX_QUEUE=2 (din mediu): după repornire, din 3 mesaje rămân 2
	p = run(t, dir, "MAX_QUEUE=2")
	c = client(t, p.addr)
	for i := 0; i < 3; i++ {
		if _, err := c.Publish(context.Background(), &brokerpb.PublishRequest{Message: common.NewMessage("", "s", "news", fmt.Sprint(i))}); err != nil {
			t.Fatal(err)
		}
	}
	if !strings.Contains(p.output(), `"event":"queue.overflow"`) {
		t.Fatalf("MAX_QUEUE din mediu nu a fost aplicat:\n%s", p.output())
	}
}

// #10: kill -9 (fără nicio șansă de curățenie). Mesajele confirmate senderului sunt deja pe
// disc (fsync), deci noul proces le livrează receiverului.
func TestQueuedMessagesSurviveKill9(t *testing.T) {
	dir, addr := t.TempDir(), freeAddr(t)
	env := []string{"BROKER_ADDR=" + addr, "BROKER_DATA_DIR=" + filepath.Join(dir, "data")}
	p := run(t, dir, env...)
	c := client(t, addr)
	ctx, cancel := context.WithCancel(context.Background())
	sub, _ := c.Subscribe(ctx, &brokerpb.SubscribeRequest{ReceiverId: "r", Topics: []string{"news"}})
	sub.Header()
	cancel() // receiverul e offline

	var sent []string
	for i := 1; i <= 3; i++ {
		m := common.NewMessage("", "s", "news", fmt.Sprintf("înainte de kill -9, %d", i))
		if _, err := c.Publish(context.Background(), &brokerpb.PublishRequest{Message: m}); err != nil {
			t.Fatal(err)
		}
		sent = append(sent, m.MessageId)
	}
	p.cmd.Process.Signal(syscall.SIGKILL)
	if err := p.wait(t); err == nil || !strings.Contains(err.Error(), "killed") {
		t.Fatalf("procesul trebuia omorât: %v", err)
	}

	p = run(t, dir, env...)
	if !strings.Contains(p.output(), `"queued":3`) {
		t.Fatalf("starea nu a fost refăcută:\n%s", p.output())
	}
	c = client(t, addr)
	stream, _ := c.Subscribe(context.Background(), &brokerpb.SubscribeRequest{ReceiverId: "r", Topics: []string{"news"}})
	for i, want := range sent {
		m, err := stream.Recv()
		if err != nil || m.MessageId != want {
			t.Fatalf("mesajul %d după kill -9: %v %v", i+1, m, err)
		}
		t.Logf("recuperat: %s", m.Payload)
	}
}
