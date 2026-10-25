// Receiver: apelează RPC-ul Subscribe, primește mesajele prin stream și confirmă fiecare
// mesaj procesat cu Ack. Un mesaj neconfirmat rămâne în coada brokerului și e retrimis la
// reconectare (at-least-once); retrimiterile deja procesate sunt recunoscute după messageId.
// Dacă pierde conexiunea, se reconectează de cel mult RECONNECT_MAX ori, la interval fix.
//
//	go run ./receiver -id receiver-1 -topics news
//	go run ./receiver -id receiver-2 -topics news,alerts
//	go run ./receiver -id receiver-2 -unsubscribe        șterge abonarea și coada din broker
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"lab2/common"
	"lab2/gen/brokerpb"
)

func main() {
	log := common.Setup("receiver")
	o := common.ClientOptionsFromEnv()
	addr := flag.String("addr", common.Env("BROKER_ADDR", "127.0.0.1:50061"), "adresa brokerului")
	id := flag.String("id", "receiver-1", "identificator stabil (cheia cozii din broker)")
	topics := flag.String("topics", "news", "topicuri separate prin virgulă")
	unsubscribe := flag.Bool("unsubscribe", false, "șterge abonarea și coada receiverului, apoi iese")
	flag.StringVar(&o.Codec, "codec", o.Codec, "reprezentarea pe fir: proto sau json")
	flag.StringVar(&o.CAFile, "tls-ca", o.CAFile, "certificatul CA (activează TLS)")
	r := receiver{out: os.Stdout}
	flag.IntVar(&r.maxRetries, "retries", common.EnvInt("RECONNECT_MAX", 5), "număr maxim de reconectări")
	flag.DurationVar(&r.connectTimeout, "connect-timeout", time.Duration(common.EnvInt("CONNECT_TIMEOUT_SEC", 2))*time.Second, "cât așteaptă o încercare de conectare")
	flag.DurationVar(&r.interval, "interval", time.Duration(common.EnvInt("RECONNECT_INTERVAL_SEC", 2))*time.Second, "interval între reconectări")
	flag.Parse()
	r.log = log.With("receiver", *id)

	conn, err := common.Dial(*addr, o)
	if err != nil {
		r.log.Error("nu mă pot conecta", "event", "connect", "result", "failed", "error", err.Error())
		os.Exit(1)
	}
	defer conn.Close()
	r.conn, r.client = conn, brokerpb.NewBrokerClient(conn)
	r.req = &brokerpb.SubscribeRequest{ReceiverId: *id}
	for _, t := range strings.Split(*topics, ",") {
		r.req.Topics = append(r.req.Topics, strings.TrimSpace(t))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *unsubscribe {
		err = r.unsubscribe(ctx)
	} else {
		err = r.run(ctx)
	}
	if err != nil {
		os.Exit(1)
	}
}

// errGaveUp: brokerul nu a răspuns după numărul maxim de reconectări.
var errGaveUp = errors.New("renunț la reconectare")

type receiver struct {
	conn           *grpc.ClientConn
	client         brokerpb.BrokerClient
	req            *brokerpb.SubscribeRequest
	maxRetries     int
	connectTimeout time.Duration // cât așteaptă o încercare ca brokerul să răspundă
	interval       time.Duration // pauza dintre încercări
	log            *slog.Logger
	out            io.Writer
	seen           *common.Recent[struct{}] // messageId-urile deja procesate
}

// run ține abonarea până la ctx.Done (nil) sau o eroare definitivă.
func (r *receiver) run(ctx context.Context) error {
	if r.seen == nil {
		r.seen = common.NewRecent[struct{}](10000)
	}
	attempts := 0
	for {
		subscribed, err := r.session(ctx)
		if ctx.Err() != nil {
			r.log.Info("receiver oprit", "event", "receiver.stopped")
			return nil
		}
		switch status.Code(err) {
		case codes.InvalidArgument, codes.Unauthenticated:
			// cererea e greșită sau lipsește tokenul: retry-ul nu ar schimba nimic
			r.log.Error("abonare respinsă", "event", "subscribe", "result", "rejected",
				"code", status.Code(err).String(), "error", status.Convert(err).Message())
			return err
		case codes.Canceled:
			// alt proces s-a abonat cu același id și a preluat coada, sau receiverul a fost dezabonat
			r.log.Error("sesiune închisă de broker", "event", "subscribe", "result", "closed",
				"error", status.Convert(err).Message())
			return err
		}
		if subscribed {
			attempts = 0 // sesiunea a mers, deci contorul de reconectări pornește iar de la zero
		}
		attempts++
		if attempts > r.maxRetries {
			r.log.Error("renunț la reconectare", "event", "reconnect", "result", "gave_up", "attempts", r.maxRetries)
			return errGaveUp
		}
		r.log.Warn("conexiune pierdută, reîncerc", "event", "reconnect", "attempt", attempts,
			"max", r.maxRetries, "in", r.interval.String(), "code", status.Code(err).String())
		select {
		case <-time.After(r.interval):
		case <-ctx.Done():
			r.log.Info("receiver oprit", "event", "receiver.stopped")
			return nil
		}
	}
}

// session face un Subscribe, citește mesajele și le confirmă până se închide stream-ul.
// subscribed = true dacă brokerul a confirmat abonarea.
func (r *receiver) session(ctx context.Context) (subscribed bool, err error) {
	r.req.CorrelationId = common.NewID()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := common.WaitReady(ctx, r.conn, r.connectTimeout); err != nil {
		return false, err
	}
	stream, err := r.client.Subscribe(ctx, r.req)
	if err != nil {
		return false, err
	}
	// header-ul "subscribed" = confirmarea abonării. Dacă brokerul respinge cererea,
	// header-ul lipsește, iar eroarea (de ex. INVALID_ARGUMENT) vine la Recv.
	md, err := stream.Header()
	if err != nil {
		return false, err
	}
	if len(md.Get("subscribed")) == 0 {
		_, err := stream.Recv()
		return false, err
	}
	r.log.Info("abonare confirmată", "event", "subscribe", "result", "ok", "topics", r.req.Topics,
		"correlationId", r.req.CorrelationId)

	for {
		m, err := stream.Recv()
		if err != nil {
			return true, err
		}
		if _, dup := r.seen.Get(m.MessageId); dup {
			r.log.Info("mesaj retrimis, deja procesat", "event", "receive", "result", "duplicate",
				"correlationId", m.CorrelationId, "messageId", m.MessageId)
		} else {
			fmt.Fprintf(r.out, ">>> [grpc] %s de la %s: %s\n", m.Topic, m.Sender, m.Payload)
			r.seen.Add(m.MessageId, struct{}{})
			r.log.Info("mesaj primit", "event", "receive", "result", "ok",
				"correlationId", m.CorrelationId, "messageId", m.MessageId, "topic", m.Topic)
		}
		// confirmarea vine după procesare: dacă receiverul cade înainte, mesajul se retrimite
		actx, acancel := context.WithTimeout(ctx, 3*time.Second)
		_, err = r.client.Ack(actx, &brokerpb.AckRequest{ReceiverId: r.req.ReceiverId, MessageId: m.MessageId})
		acancel()
		if err != nil {
			r.log.Warn("confirmare eșuată, mesajul va fi retrimis", "event", "ack", "result", "failed",
				"messageId", m.MessageId, "code", status.Code(err).String())
			return true, err
		}
	}
}

func (r *receiver) unsubscribe(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	reply, err := r.client.Unsubscribe(ctx, &brokerpb.UnsubscribeRequest{ReceiverId: r.req.ReceiverId, CorrelationId: common.NewID()})
	if err != nil {
		r.log.Error("dezabonare eșuată", "event", "unsubscribe", "result", "failed",
			"code", status.Code(err).String(), "error", status.Convert(err).Message())
		return err
	}
	r.log.Info("dezabonat", "event", "unsubscribe", "result", "ok", "dropped", reply.Dropped)
	return nil
}
