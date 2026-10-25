package common

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// ClientOptions descrie cum se conectează sender-ul și receiverul la broker.
type ClientOptions struct {
	CAFile string // certificatul CA pentru TLS; gol = conexiune necriptată
	Token  string // trimis ca "authorization: Bearer <token>"; gol = fără autentificare
	Codec  string // "proto" (implicit) sau "json"
}

// ClientOptionsFromEnv citește BROKER_TLS_CA, BROKER_TOKEN și BROKER_CODEC.
func ClientOptionsFromEnv() ClientOptions {
	return ClientOptions{
		CAFile: Env("BROKER_TLS_CA", ""),
		Token:  Env("BROKER_TOKEN", ""),
		Codec:  Env("BROKER_CODEC", "proto"),
	}
}

// Dial creează conexiunea gRPC către broker conform opțiunilor.
func Dial(addr string, o ClientOptions) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption
	if o.CAFile != "" {
		creds, err := credentials.NewClientTLSFromFile(o.CAFile, "")
		if err != nil {
			return nil, fmt.Errorf("certificat CA: %w", err)
		}
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	if o.Token != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(bearer{token: o.Token, tls: o.CAFile != ""}))
	}
	switch o.Codec {
	case "", "proto":
	case JSONCodecName:
		opts = append(opts, grpc.WithDefaultCallOptions(grpc.CallContentSubtype(JSONCodecName)))
	default:
		return nil, fmt.Errorf("codec necunoscut: %q (proto sau json)", o.Codec)
	}
	return grpc.NewClient(addr, opts...)
}

// bearer adaugă tokenul la fiecare apel.
type bearer struct {
	token string
	tls   bool
}

func (b bearer) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + b.token}, nil
}

// Fără TLS tokenul circulă necriptat. E permis (pentru localhost), dar nu e recomandat.
func (b bearer) RequireTransportSecurity() bool { return b.tls }

// WaitReady așteaptă cel mult timeout ca conexiunea să fie gata. Întâi anulează backoff-ul
// intern de reconectare al gRPC, deci fiecare reîncercare a aplicației chiar contactează
// brokerul (altfel, după un eșec, apelurile eșuează imediat cât timp durează backoff-ul).
// Întoarce UNAVAILABLE dacă brokerul nu răspunde în timp.
func WaitReady(ctx context.Context, conn *grpc.ClientConn, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn.ResetConnectBackoff()
	conn.Connect()
	for {
		s := conn.GetState()
		if s == connectivity.Ready {
			return nil
		}
		if !conn.WaitForStateChange(ctx, s) {
			return status.Errorf(codes.Unavailable, "brokerul nu răspunde în %v (conexiune %s)", timeout, s)
		}
	}
}
