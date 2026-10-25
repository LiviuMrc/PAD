// dlq: inspectează și administrează coada dead-letter a brokerului.
//
//	go run ./dlq                                  listează toate mesajele din DLQ
//	go run ./dlq -receiver receiver-2             doar cele ale lui receiver-2
//	go run ./dlq -redrive -receiver receiver-2    le mută înapoi în coada lui (trebuie să fie abonat)
//	go run ./dlq -redrive -receiver receiver-2 -ids <id1>,<id2>
//	go run ./dlq -purge [-receiver receiver-2] [-ids ...]   le șterge definitiv
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc/status"

	"lab2/common"
	"lab2/gen/brokerpb"
)

func main() {
	log := common.Setup("dlq")
	o := common.ClientOptionsFromEnv()
	addr := flag.String("addr", common.Env("BROKER_ADDR", "127.0.0.1:50061"), "adresa brokerului")
	receiver := flag.String("receiver", "", "receiverul (gol = toate; obligatoriu la -redrive)")
	ids := flag.String("ids", "", "messageId-uri separate prin virgulă (gol = toate)")
	redrive := flag.Bool("redrive", false, "mută mesajele înapoi în coada receiverului")
	purge := flag.Bool("purge", false, "șterge definitiv mesajele din DLQ")
	limit := flag.Int("limit", 0, "câte mesaje să afișeze (0 = toate)")
	flag.StringVar(&o.CAFile, "tls-ca", o.CAFile, "certificatul CA (activează TLS)")
	flag.Parse()

	conn, err := common.Dial(*addr, o)
	if err != nil {
		log.Error("nu mă pot conecta", "event", "connect", "result", "failed", "error", err.Error())
		os.Exit(1)
	}
	defer conn.Close()
	var idList []string
	for _, id := range strings.Split(*ids, ",") {
		if id = strings.TrimSpace(id); id != "" {
			idList = append(idList, id)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := run(ctx, brokerpb.NewBrokerClient(conn), os.Stdout, *receiver, idList, *redrive, *purge, *limit); err != nil {
		st := status.Convert(err)
		log.Error("operație DLQ eșuată", "event", "dlq", "result", "failed", "code", st.Code().String(), "error", st.Message())
		os.Exit(1)
	}
}

func run(ctx context.Context, c brokerpb.BrokerClient, out io.Writer, receiver string, ids []string, redrive, purge bool, limit int) error {
	switch {
	case redrive && purge:
		return fmt.Errorf("alege -redrive sau -purge, nu amândouă")
	case redrive:
		r, err := c.RedriveDeadLetters(ctx, &brokerpb.RedriveRequest{ReceiverId: receiver, MessageIds: ids})
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "retrimise în coada lui %s: %d (rămase în DLQ, topic neabonat: %d)\n", receiver, r.Redriven, r.Skipped)
		return nil
	case purge:
		r, err := c.PurgeDeadLetters(ctx, &brokerpb.PurgeDeadLettersRequest{ReceiverId: receiver, MessageIds: ids})
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "șterse din DLQ: %d\n", r.Purged)
		return nil
	}
	r, err := c.ListDeadLetters(ctx, &brokerpb.ListDeadLettersRequest{ReceiverId: receiver, Limit: int32(limit)})
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "DLQ: %d mesaje\n", r.Total)
	for _, d := range r.DeadLetters {
		fmt.Fprintf(out, "%s  %-16s  %-12s  %s  %s: %s\n", d.DeadAt, d.Reason, d.ReceiverId,
			d.Message.MessageId, d.Message.Topic, d.Message.Payload)
	}
	return nil
}
