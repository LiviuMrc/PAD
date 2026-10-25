package broker

import (
	"context"
	"crypto/subtle"
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"lab2/gen/brokerpb"
)

// ServerConfig descrie tot ce pornește broker/main.go. Testele folosesc aceeași structură.
type ServerConfig struct {
	Broker  Config
	DataDir string // director pentru jurnal; gol = fără persistență
	Fsync   bool   // fsync după fiecare înregistrare din jurnal

	Token           string // dacă e setat, fiecare apel Broker trebuie să aibă "authorization: Bearer <token>"
	TLSCert, TLSKey string // ambele = TLS; niciunul = conexiune necriptată

	DrainDelay      time.Duration // cât timp e raportat NOT_SERVING înainte de închiderea conexiunilor
	ShutdownTimeout time.Duration // cât se așteaptă apelurile unary în curs la oprire

	ExtraOptions []grpc.ServerOption // pentru teste (de exemplu interceptori care întârzie apeluri)
}

// Server leagă brokerul de serverul gRPC, de serviciul health și de reflection.
type Server struct {
	Broker *Broker

	cfg      ServerConfig
	grpc     *grpc.Server
	health   *health.Server
	journal  *Journal
	log      *slog.Logger
	inflight atomic.Int64
	draining atomic.Bool
	once     sync.Once
	stopped  chan struct{}
}

var serviceName = brokerpb.Broker_ServiceDesc.ServiceName

func NewServer(cfg ServerConfig, log *slog.Logger) (*Server, error) {
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 5 * time.Second
	}
	s := &Server{cfg: cfg, log: log, stopped: make(chan struct{})}

	if cfg.DataDir != "" {
		j, err := OpenJournal(cfg.DataDir, cfg.Fsync)
		if err != nil {
			return nil, err
		}
		s.journal = j
		cfg.Broker.Journal = j
	}
	b, err := New(cfg.Broker, log)
	if err != nil {
		s.journal.Close()
		return nil, err
	}
	if s.journal != nil && s.journal.Trimmed() > 0 {
		log.Warn("ultima înregistrare din jurnal era incompletă și a fost tăiată", "event", "journal.replay",
			"bytes", s.journal.Trimmed())
	}
	s.Broker = b

	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(s.trackUnary, s.authUnary),
		grpc.ChainStreamInterceptor(s.gateStream, s.authStream),
	}
	if cfg.TLSCert != "" || cfg.TLSKey != "" {
		if cfg.TLSCert == "" || cfg.TLSKey == "" {
			return nil, errors.New("TLS: trebuie date atât certificatul, cât și cheia")
		}
		creds, err := credentials.NewServerTLSFromFile(cfg.TLSCert, cfg.TLSKey)
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.Creds(creds))
	}
	s.grpc = grpc.NewServer(append(opts, cfg.ExtraOptions...)...)
	brokerpb.RegisterBrokerServer(s.grpc, b)

	// health check standard gRPC: NOT_SERVING până când serverul acceptă trafic și din
	// momentul în care începe oprirea; SERVING doar între Serve și Shutdown
	s.health = health.NewServer()
	s.health.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	s.health.SetServingStatus(serviceName, healthpb.HealthCheckResponse_NOT_SERVING)
	healthpb.RegisterHealthServer(s.grpc, s.health)

	// reflection: unelte precum grpcurl pot descoperi serviciile fără fișierul .proto
	reflection.Register(s.grpc)
	return s, nil
}

// Serve acceptă conexiuni pe ln până la Shutdown.
func (s *Server) Serve(ln net.Listener) error {
	s.health.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	s.health.SetServingStatus(serviceName, healthpb.HealthCheckResponse_SERVING)
	s.log.Info("broker pornit", "event", "broker.started", "addr", ln.Addr().String(),
		"tls", s.cfg.TLSCert != "", "auth", s.cfg.Token != "", "journal", s.journalPath())
	if err := s.grpc.Serve(ln); err != nil {
		return err
	}
	<-s.stopped // grpc.Serve se întoarce imediat după Stop; așteptăm și închiderea jurnalului
	return nil
}

// Shutdown oprește brokerul în ordine:
//  1. health devine NOT_SERVING (watcherii și load balancerele îl văd pe durata DrainDelay);
//  2. apelurile noi primesc UNAVAILABLE, iar cele unary deja în curs se termină normal
//     (cel mult ShutdownTimeout), ca un Publish deja primit să fie salvat și confirmat;
//  3. brokerul se închide: stream-urile Subscribe se termină cu UNAVAILABLE;
//  4. se închid conexiunile și jurnalul.
func (s *Server) Shutdown() {
	s.once.Do(s.shutdown)
}

func (s *Server) shutdown() {
	s.log.Info("oprire: health NOT_SERVING", "event", "broker.stopping")
	s.health.Shutdown()
	time.Sleep(s.cfg.DrainDelay)
	s.draining.Store(true)
	deadline := time.Now().Add(s.cfg.ShutdownTimeout)
	for s.inflight.Load() > 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if n := s.inflight.Load(); n > 0 {
		s.log.Warn("oprire forțată: apeluri încă în curs", "event", "broker.stopping", "inflight", n)
	}
	s.Broker.Close()
	s.grpc.Stop()
	if err := s.journal.Close(); err != nil {
		s.log.Error("închiderea jurnalului eșuată", "event", "journal.close", "error", err.Error())
	}
	close(s.stopped)
}

func (s *Server) journalPath() string {
	if s.journal == nil {
		return ""
	}
	return s.journal.Path()
}

var errDraining = status.Error(codes.Unavailable, "brokerul se oprește")

func isHealth(method string) bool { return strings.HasPrefix(method, "/grpc.health.v1.Health/") }

// trackUnary numără apelurile în curs; în timpul opririi refuză apelurile noi
// (în afară de health, care trebuie să poată raporta NOT_SERVING).
func (s *Server) trackUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	if isHealth(info.FullMethod) {
		return h(ctx, req)
	}
	s.inflight.Add(1)
	defer s.inflight.Add(-1)
	if s.draining.Load() {
		return nil, errDraining
	}
	return h(ctx, req)
}

func (s *Server) gateStream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
	if s.draining.Load() && !isHealth(info.FullMethod) {
		return errDraining
	}
	return h(srv, ss)
}

func (s *Server) authUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	if err := s.authorize(ctx, info.FullMethod); err != nil {
		return nil, err
	}
	return h(ctx, req)
}

func (s *Server) authStream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
	if err := s.authorize(ss.Context(), info.FullMethod); err != nil {
		return err
	}
	return h(srv, ss)
}

// authorize cere tokenul pentru tot în afară de health (sondele nu au token).
func (s *Server) authorize(ctx context.Context, method string) error {
	if s.cfg.Token == "" || isHealth(method) {
		return nil
	}
	md, _ := metadata.FromIncomingContext(ctx)
	for _, v := range md.Get("authorization") {
		if subtle.ConstantTimeCompare([]byte(v), []byte("Bearer "+s.cfg.Token)) == 1 {
			return nil
		}
	}
	s.log.Warn("apel neautentificat", "event", "auth", "result", "rejected", "method", method)
	return status.Error(codes.Unauthenticated, "token lipsă sau greșit")
}
