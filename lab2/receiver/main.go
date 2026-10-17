// Receiver: apelează RPC-ul Subscribe și primește mesajele prin stream.
// Dacă pierde conexiunea, se reconectează de cel mult RECONNECT_MAX ori, la interval fix.
// La reconectare, brokerul îi trimite mesajele strânse în coadă cât a fost offline.
//
//	go run ./receiver -id receiver-1 -topics news
//	go run ./receiver -id receiver-2 -topics news,alerts
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"lab2/common"
	"lab2/gen/brokerpb"
)

func main() {
	addr := flag.String("addr", common.Env("BROKER_ADDR", "127.0.0.1:50061"), "adresa brokerului")
	id := flag.String("id", "receiver-1", "identificator stabil (cheia cozii din broker)")
	topics := flag.String("topics", "news", "topicuri separate prin virgulă")
	maxRetries := flag.Int("retries", common.EnvInt("RECONNECT_MAX", 5), "număr maxim de reconectări")
	interval := flag.Duration("interval", time.Duration(common.EnvInt("RECONNECT_INTERVAL_SEC", 2))*time.Second, "interval între reconectări")
	flag.Parse()

	log := common.NewLogger("receiver").With("receiver", *id)
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Error("adresă invalidă", "event", "connect", "result", "failed", "error", err.Error())
		os.Exit(1)
	}
	defer conn.Close()
	client := brokerpb.NewBrokerClient(conn)

	req := &brokerpb.SubscribeRequest{ReceiverId: *id}
	for _, t := range strings.Split(*topics, ",") {
		req.Topics = append(req.Topics, strings.TrimSpace(t))
	}
	attempts := 0
	for {
		subscribed, err := session(client, req, log)
		if status.Code(err) == codes.InvalidArgument {
			// cererea e greșită: retry-ul nu ar schimba nimic
			log.Error("abonare respinsă", "event", "subscribe", "result", "rejected", "error", status.Convert(err).Message())
			os.Exit(1)
		}
		if status.Code(err) == codes.Canceled {
			// alt proces s-a abonat cu același id și a preluat coada
			log.Error("sesiune preluată de alt receiver cu același id", "event", "subscribe", "result", "replaced")
			os.Exit(1)
		}
		if subscribed {
			attempts = 0 // sesiunea a mers, deci contorul de reconectări pornește iar de la zero
		}
		attempts++
		if attempts > *maxRetries {
			log.Error("renunț la reconectare", "event", "reconnect", "result", "gave_up", "attempts", *maxRetries)
			os.Exit(1)
		}
		log.Warn("conexiune pierdută, reîncerc", "event", "reconnect", "attempt", attempts,
			"max", *maxRetries, "in", interval.String(), "code", status.Code(err).String())
		time.Sleep(*interval)
	}
}

// session face un Subscribe și citește mesaje până se închide stream-ul.
// subscribed = true dacă brokerul a confirmat abonarea.
func session(client brokerpb.BrokerClient, req *brokerpb.SubscribeRequest, log *slog.Logger) (subscribed bool, err error) {
	req.CorrelationId = common.NewID()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := client.Subscribe(ctx, req)
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
	log.Info("abonare confirmată", "event", "subscribe", "result", "ok", "topics", req.Topics,
		"correlationId", req.CorrelationId)

	for {
		m, err := stream.Recv()
		if err != nil {
			return true, err
		}
		fmt.Printf(">>> [grpc] %s de la %s: %s\n", m.Topic, m.Sender, m.Payload)
		log.Info("mesaj primit", "event", "receive", "result", "ok",
			"correlationId", m.CorrelationId, "messageId", m.MessageId, "topic", m.Topic)
	}
}
