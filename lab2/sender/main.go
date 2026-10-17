// Sender: apelează RPC-ul Publish și primește confirmarea (PublishReply) sau o eroare gRPC.
//
//	go run ./sender -topic news -payload "salut"            un singur mesaj
//	go run ./sender -topic news                             citește mesajele din stdin, câte unul pe linie
//	go run ./sender -topic news -payload x -schema 2.0      demo validare: INVALID_ARGUMENT
//	go run ./sender -topic news -payload x -json            afișează mesajul și în JSON
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/prototext"

	"lab2/common"
	"lab2/gen/brokerpb"
)

func main() {
	addr := flag.String("addr", common.Env("BROKER_ADDR", "127.0.0.1:50061"), "adresa brokerului")
	id := flag.String("id", "sender-1", "identificatorul senderului")
	topic := flag.String("topic", "news", "topicul")
	payload := flag.String("payload", "", "conținutul mesajului (gol = citește din stdin)")
	schema := flag.String("schema", common.SchemaVersion, "schemaVersion trimis (altă valoare = demo validare)")
	showJSON := flag.Bool("json", false, "afișează mesajul și în format text proto și JSON")
	timeout := flag.Duration("timeout", time.Duration(common.EnvInt("PUBLISH_TIMEOUT_SEC", 3))*time.Second, "deadline pentru Publish")
	flag.Parse()

	log := common.NewLogger("sender")
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Error("adresă invalidă", "event", "connect", "result", "failed", "error", err.Error())
		os.Exit(1)
	}
	defer conn.Close()
	client := brokerpb.NewBrokerClient(conn)

	publish := func(text string) bool {
		m := common.NewMessage("", *id, *topic, text)
		m.SchemaVersion = *schema
		if *showJSON {
			j, _ := protojson.Marshal(m)
			fmt.Printf("proto: %s\nJSON : %s\n", prototext.Format(m), j)
		}

		// deadline explicit: apelul nu așteaptă la nesfârșit
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		defer cancel()
		start := time.Now()
		reply, err := client.Publish(ctx, &brokerpb.PublishRequest{Message: m})
		if err != nil {
			st := status.Convert(err)
			log.Warn("publicare eșuată", "event", "publish", "result", "error", "code", st.Code().String(),
				"correlationId", m.CorrelationId, "messageId", m.MessageId, "error", st.Message(),
				"duration_ms", time.Since(start).Milliseconds())
			return false
		}
		log.Info("publicare confirmată", "event", "publish", "result", "ack",
			"correlationId", reply.CorrelationId, "messageId", reply.MessageId, "topic", *topic,
			"subscribers", reply.Subscribers, "duration_ms", time.Since(start).Milliseconds())
		return true
	}

	if *payload != "" {
		if !publish(*payload) {
			os.Exit(1)
		}
		return
	}

	fmt.Fprintf(os.Stderr, "Scrie mesaje pentru topicul %q (Ctrl+D pentru ieșire):\n", *topic)
	in := bufio.NewScanner(os.Stdin)
	for in.Scan() {
		if line := in.Text(); line != "" {
			publish(line)
		}
	}
}
