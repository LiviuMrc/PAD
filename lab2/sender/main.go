// Sender: apelează RPC-ul Publish și primește confirmarea (PublishReply) sau o eroare gRPC.
// La UNAVAILABLE sau DEADLINE_EXCEEDED reîncearcă de cel mult -retries ori cu același
// messageId; brokerul deduplică, deci mesajul nu ajunge de două ori.
//
//	go run ./sender -topic news -payload "salut"            un singur mesaj
//	go run ./sender -topic news                             citește mesajele din stdin, câte unul pe linie
//	go run ./sender -topic news -payload x -schema 2.0      demo validare: INVALID_ARGUMENT
//	go run ./sender -topic news -payload x -json            afișează mesajul și în JSON
//	go run ./sender -topic news -payload x -codec json      trimite mesajul pe fir ca JSON
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/prototext"

	"lab2/common"
	"lab2/gen/brokerpb"
)

func main() {
	log := common.Setup("sender")
	o := common.ClientOptionsFromEnv()
	addr := flag.String("addr", common.Env("BROKER_ADDR", "127.0.0.1:50061"), "adresa brokerului")
	id := flag.String("id", "sender-1", "identificatorul senderului")
	topic := flag.String("topic", "news", "topicul")
	payload := flag.String("payload", "", "conținutul mesajului (gol = citește din stdin)")
	schema := flag.String("schema", common.SchemaVersion, "schemaVersion trimis (altă valoare = demo validare)")
	showJSON := flag.Bool("json", false, "afișează mesajul și în format text proto și JSON")
	flag.StringVar(&o.Codec, "codec", o.Codec, "reprezentarea pe fir: proto sau json")
	flag.StringVar(&o.CAFile, "tls-ca", o.CAFile, "certificatul CA (activează TLS)")
	p := publisher{log: log}
	flag.DurationVar(&p.timeout, "timeout", time.Duration(common.EnvInt("PUBLISH_TIMEOUT_SEC", 3))*time.Second, "deadline pentru fiecare încercare")
	flag.IntVar(&p.retries, "retries", common.EnvInt("PUBLISH_RETRIES", 3), "reîncercări la UNAVAILABLE / DEADLINE_EXCEEDED")
	flag.DurationVar(&p.interval, "retry-interval", time.Duration(common.EnvInt("PUBLISH_RETRY_INTERVAL_MS", 500))*time.Millisecond, "pauza dintre reîncercări")
	flag.Parse()

	conn, err := common.Dial(*addr, o)
	if err != nil {
		log.Error("nu mă pot conecta", "event", "connect", "result", "failed", "error", err.Error())
		os.Exit(1)
	}
	defer conn.Close()
	p.conn, p.client = conn, brokerpb.NewBrokerClient(conn)

	send := func(text string) bool {
		m := common.NewMessage("", *id, *topic, text)
		m.SchemaVersion = *schema
		if *showJSON {
			j, _ := protojson.Marshal(m)
			fmt.Printf("proto: %s\nJSON : %s\n", prototext.Format(m), j)
		}
		_, err := p.publish(m)
		return err == nil
	}

	if *payload != "" {
		if !send(*payload) {
			os.Exit(1)
		}
		return
	}

	fmt.Fprintf(os.Stderr, "Scrie mesaje pentru topicul %q (Ctrl+D pentru ieșire):\n", *topic)
	in := bufio.NewScanner(os.Stdin)
	for in.Scan() {
		if line := in.Text(); line != "" {
			send(line)
		}
	}
}

type publisher struct {
	conn     *grpc.ClientConn
	client   brokerpb.BrokerClient
	timeout  time.Duration // deadline explicit pe fiecare încercare: apelul nu așteaptă la nesfârșit
	retries  int
	interval time.Duration
	log      *slog.Logger
}

// publish trimite mesajul și reîncearcă, cu același messageId, doar la erorile după care
// mesajul poate să fi ajuns sau nu: UNAVAILABLE (broker oprit) și DEADLINE_EXCEEDED.
func (p publisher) publish(m *brokerpb.Message) (*brokerpb.PublishReply, error) {
	for attempt := 1; ; attempt++ {
		start := time.Now()
		err := common.WaitReady(context.Background(), p.conn, p.timeout)
		var reply *brokerpb.PublishReply
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
			reply, err = p.client.Publish(ctx, &brokerpb.PublishRequest{Message: m})
			cancel()
		}
		if err == nil {
			p.log.Info("publicare confirmată", "event", "publish", "result", "ack",
				"correlationId", reply.CorrelationId, "messageId", reply.MessageId, "topic", m.Topic,
				"subscribers", reply.Subscribers, "duplicate", reply.Duplicate, "attempt", attempt,
				"duration_ms", time.Since(start).Milliseconds())
			return reply, nil
		}
		st := status.Convert(err)
		retryable := st.Code() == codes.Unavailable || st.Code() == codes.DeadlineExceeded
		if !retryable || attempt > p.retries {
			p.log.Warn("publicare eșuată", "event", "publish", "result", "error", "code", st.Code().String(),
				"correlationId", m.CorrelationId, "messageId", m.MessageId, "error", st.Message(),
				"attempt", attempt, "duration_ms", time.Since(start).Milliseconds())
			return nil, err
		}
		p.log.Warn("publicare eșuată, reîncerc cu același messageId", "event", "publish", "result", "retry",
			"code", st.Code().String(), "correlationId", m.CorrelationId, "messageId", m.MessageId,
			"attempt", attempt, "max", p.retries, "in", p.interval.String())
		time.Sleep(p.interval)
	}
}
