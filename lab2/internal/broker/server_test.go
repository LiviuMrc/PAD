package broker

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	reflectionpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/grpc/status"

	"lab2/common"
	"lab2/gen/brokerpb"
)

// #1: health e SERVING cât timp brokerul rulează, iar la oprire watcherii văd NOT_SERVING
// înainte să se închidă conexiunea. Stream-urile Subscribe se închid cu UNAVAILABLE,
// iar apelurile noi sunt refuzate.
func TestHealthGoesNotServingOnShutdown(t *testing.T) {
	e := start(t, ServerConfig{DrainDelay: 300 * time.Millisecond})
	hc := healthpb.NewHealthClient(e.conn)
	check, err := hc.Check(context.Background(), &healthpb.HealthCheckRequest{Service: serviceName})
	if err != nil || check.Status != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("Check: %v %v", check, err)
	}
	watch, err := hc.Watch(context.Background(), &healthpb.HealthCheckRequest{Service: serviceName})
	if err != nil {
		t.Fatal(err)
	}
	if r, _ := watch.Recv(); r.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("watch inițial: %v", r.GetStatus())
	}
	s := subscribe(t, e.conn, "r", "news")

	stopped := make(chan struct{})
	go func() { e.stop(); close(stopped) }()

	r, err := watch.Recv()
	if err != nil || r.Status != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("watch la oprire: %v %v, așteptat NOT_SERVING", r.GetStatus(), err)
	}
	// în DrainDelay, Check răspunde și el NOT_SERVING
	if c, err := hc.Check(context.Background(), &healthpb.HealthCheckRequest{Service: serviceName}); err != nil || c.Status != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("Check în timpul opririi: %v %v", c.GetStatus(), err)
	}
	if _, err := s.stream.Recv(); status.Code(err) != codes.Unavailable {
		t.Fatalf("Subscribe la oprire: %v, așteptat Unavailable", err)
	}
	<-stopped
	if _, err := brokerpb.NewBrokerClient(e.conn).Publish(context.Background(),
		&brokerpb.PublishRequest{Message: common.NewMessage("", "s", "news", "x")}); status.Code(err) != codes.Unavailable {
		t.Fatalf("Publish după oprire: %v, așteptat Unavailable", err)
	}
}

// Un Publish aflat în lucru când începe oprirea își primește răspunsul.
func TestShutdownWaitsForInflightPublish(t *testing.T) {
	slow := grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		time.Sleep(200 * time.Millisecond)
		return h(ctx, req)
	})
	e := start(t, ServerConfig{ExtraOptions: []grpc.ServerOption{slow}})
	res := make(chan error, 1)
	go func() {
		_, err := brokerpb.NewBrokerClient(e.conn).Publish(context.Background(),
			&brokerpb.PublishRequest{Message: common.NewMessage("", "s", "news", "în curs")})
		res <- err
	}()
	time.Sleep(50 * time.Millisecond)
	e.stop()
	if err := <-res; err != nil {
		t.Fatalf("Publish în curs la oprire: %v", err)
	}
}

// #6: reflection e înregistrat de NewServer (aceeași funcție ca în main): un client
// fără .proto descoperă serviciile, ca grpcurl list.
func TestReflectionListsServices(t *testing.T) {
	e := start(t, ServerConfig{})
	stream, err := reflectionpb.NewServerReflectionClient(e.conn).ServerReflectionInfo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&reflectionpb.ServerReflectionRequest{MessageRequest: &reflectionpb.ServerReflectionRequest_ListServices{}}); err != nil {
		t.Fatal(err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, s := range resp.GetListServicesResponse().GetService() {
		names = append(names, s.Name)
	}
	for _, want := range []string{"broker.v1.Broker", "grpc.health.v1.Health"} {
		if !slices.Contains(names, want) {
			t.Fatalf("servicii: %v, lipsește %s", names, want)
		}
	}
	t.Logf("servicii: %v", names)
}

func TestTokenAuthentication(t *testing.T) {
	e := start(t, ServerConfig{Token: "secret"})
	anon := dial(t, e.addr, common.ClientOptions{})
	wrong := dial(t, e.addr, common.ClientOptions{Token: "altceva"})
	for name, conn := range map[string]*grpc.ClientConn{"fără token": anon, "token greșit": wrong} {
		_, err := brokerpb.NewBrokerClient(conn).Publish(context.Background(),
			&brokerpb.PublishRequest{Message: common.NewMessage("", "s", "news", "x")})
		if status.Code(err) != codes.Unauthenticated {
			t.Errorf("%s, Publish: %v, așteptat Unauthenticated", name, err)
		}
		stream, _ := brokerpb.NewBrokerClient(conn).Subscribe(context.Background(), &brokerpb.SubscribeRequest{ReceiverId: "r", Topics: []string{"news"}})
		if _, err := stream.Recv(); status.Code(err) != codes.Unauthenticated {
			t.Errorf("%s, Subscribe: %v, așteptat Unauthenticated", name, err)
		}
	}
	// sondele de health nu au token
	if c, err := healthpb.NewHealthClient(anon).Check(context.Background(), &healthpb.HealthCheckRequest{}); err != nil || c.Status != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("health fără token: %v %v", c, err)
	}
	s := subscribe(t, e.conn, "r", "news") // e.conn are tokenul corect
	sent := publish(t, e.conn, "news", "autentificat")
	if got := s.recv(t); got.MessageId != sent.MessageId {
		t.Fatal("mesaj greșit")
	}
}

func TestTLS(t *testing.T) {
	dir := t.TempDir()
	ca, cert, key := writeTestCerts(t, dir)
	otherCA, _, _ := writeTestCerts(t, t.TempDir())

	e := startOn(t, "127.0.0.1:0", ServerConfig{TLSCert: cert, TLSKey: key, Token: "secret"},
		common.ClientOptions{CAFile: ca, Token: "secret"})
	s := subscribe(t, e.conn, "r", "news")
	sent := publish(t, e.conn, "news", "criptat")
	if got := s.recv(t); got.MessageId != sent.MessageId {
		t.Fatal("mesaj greșit prin TLS")
	}

	for name, o := range map[string]common.ClientOptions{
		"fără TLS": {Token: "secret"},
		"alt CA":   {CAFile: otherCA, Token: "secret"},
	} {
		conn := dial(t, e.addr, o)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := brokerpb.NewBrokerClient(conn).Publish(ctx, &brokerpb.PublishRequest{Message: common.NewMessage("", "s", "news", "x")})
		cancel()
		if status.Code(err) != codes.Unavailable {
			t.Errorf("%s: %v, așteptat Unavailable (handshake eșuat)", name, err)
		} else {
			t.Logf("%s -> %s", name, status.Convert(err).Message())
		}
	}
}

// #3: cu codec-ul json, mesajele circulă pe fir ca JSON (verificat pe octeții reali,
// printr-un proxy TCP), iar cu codec-ul implicit ca protobuf binar.
func TestJSONCodecOnTheWire(t *testing.T) {
	contentTypes := &logBuf{} // Content-Type văzut de server (header-ele HTTP/2 sunt comprimate HPACK pe fir)
	record := grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		fmt.Fprintf(contentTypes, "%s %v\n", info.FullMethod, md.Get("content-type"))
		return h(ctx, req)
	})
	e := start(t, ServerConfig{ExtraOptions: []grpc.ServerOption{record}})
	for _, codec := range []string{"json", "proto"} {
		wire := &logBuf{}
		proxy := tcpProxy(t, e.addr, wire)
		conn := dial(t, proxy, common.ClientOptions{Codec: codec})

		s := subscribe(t, conn, "r-"+codec, "news")
		sent := publish(t, conn, "news", "pe-fir-"+codec)
		got := s.recv(t)
		if got.MessageId != sent.MessageId || got.Payload != sent.Payload {
			t.Fatalf("%s: mesaj diferit: %+v", codec, got)
		}
		ack(t, conn, "r-"+codec, got)

		bytesOnWire := wire.String()
		hasJSON := strings.Contains(bytesOnWire, `"payload":"pe-fir-`+codec+`"`)
		hasCT := strings.Contains(contentTypes.String(), "Publish [application/grpc+json]")
		t.Logf("%s: JSON pe fir=%v, content-type grpc+json=%v", codec, hasJSON, hasCT)
		contentTypes.buf.Reset()
		if codec == "json" && (!hasJSON || !hasCT) {
			t.Fatal("codec json: mesajul nu a circulat ca JSON")
		}
		if codec == "proto" && (hasJSON || hasCT || !strings.Contains(bytesOnWire, "pe-fir-proto")) {
			t.Fatal("codec proto: mesajul nu a circulat ca protobuf binar")
		}
	}
}

// tcpProxy redirecționează conexiunile către target și copiază în wire tot ce circulă,
// în ambele sensuri.
func tcpProxy(t *testing.T, target string, wire io.Writer) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			s, err := net.Dial("tcp", target)
			if err != nil {
				c.Close()
				return
			}
			go func() { io.Copy(io.MultiWriter(s, wire), c); s.Close() }()
			go func() { io.Copy(io.MultiWriter(c, wire), s); c.Close() }()
		}
	}()
	return ln.Addr().String()
}

// writeTestCerts creează un CA și un certificat de server pentru 127.0.0.1/localhost.
func writeTestCerts(t *testing.T, dir string) (caFile, certFile, keyFile string) {
	t.Helper()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "lab2 test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	srvKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	srvTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "broker"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)},
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	srvDER, err := x509.CreateCertificate(rand.Reader, srvTmpl, caTmpl, &srvKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, _ := x509.MarshalECPrivateKey(srvKey)

	write := func(name, typ string, der []byte) string {
		p := filepath.Join(dir, name)
		var buf bytes.Buffer
		pem.Encode(&buf, &pem.Block{Type: typ, Bytes: der})
		if err := os.WriteFile(p, buf.Bytes(), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	return write("ca.pem", "CERTIFICATE", caDER), write("server.pem", "CERTIFICATE", srvDER), write("server-key.pem", "EC PRIVATE KEY", keyDER)
}
