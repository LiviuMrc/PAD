package common

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"time"
)

// Transport este stratul de transport abstract: brokerul, senderul și receiverul
// lucrează cu mesaje, nu cu socket-uri. Există o implementare TCP și una UDP.
type Transport interface {
	Send(m *Message) error
	Receive() (*Message, error)
	Close() error
}

// InvalidMessageError înseamnă că s-a primit o linie, dar ea nu respectă
// contractul. Conexiunea rămâne utilizabilă.
type InvalidMessageError struct {
	Reason string
}

func (e *InvalidMessageError) Error() string { return "mesaj invalid: " + e.Reason }

const (
	maxFrame     = 64 * 1024
	writeTimeout = 2 * time.Second
)

// TCPTransport trimite un mesaj XML pe linie (delimitator "\n").
type TCPTransport struct {
	conn   net.Conn
	reader *bufio.Reader
}

func NewTCPTransport(conn net.Conn) *TCPTransport {
	return &TCPTransport{conn: conn, reader: bufio.NewReaderSize(conn, maxFrame)}
}

// DialTCP se conectează la broker cu un timeout explicit.
func DialTCP(addr string) (*TCPTransport, error) {
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return nil, err
	}
	return NewTCPTransport(conn), nil
}

func (t *TCPTransport) Send(m *Message) error {
	data, err := m.ToXML()
	if err != nil {
		return err
	}
	return t.SendRaw(data)
}

// SendRaw trimite octeți arbitrari. Senderul îl folosește ca să arate
// că brokerul respinge un XML invalid.
func (t *TCPTransport) SendRaw(data []byte) error {
	_ = t.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	_, err := t.conn.Write(append(data, '\n'))
	return err
}

// Receive citește o linie, o parsează și o validează. Dacă linia nu respectă
// contractul, întoarce *InvalidMessageError. Orice altă eroare înseamnă
// că s-a închis conexiunea.
func (t *TCPTransport) Receive() (*Message, error) {
	line, err := t.reader.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		return nil, fmt.Errorf("mesaj mai mare de %d octeți", maxFrame)
	}
	if err != nil {
		return nil, err
	}
	return decode(bytes.TrimRight(line, "\r\n"))
}

func (t *TCPTransport) Close() error { return t.conn.Close() }

// SetReadTimeout pune un deadline pe următoarele citiri (0 = fără deadline).
func (t *TCPTransport) SetReadTimeout(d time.Duration) {
	if d == 0 {
		_ = t.conn.SetReadDeadline(time.Time{})
		return
	}
	_ = t.conn.SetReadDeadline(time.Now().Add(d))
}

func (t *TCPTransport) RemoteAddr() string { return t.conn.RemoteAddr().String() }

// UDPTransport este folosit doar pentru multicast: brokerul trimite o datagramă
// către grup, iar receiverele abonate la grup o citesc.
type UDPTransport struct {
	conn  *net.UDPConn
	group *net.UDPAddr // destinația pentru Send (doar la sender-ul multicast)
}

// NewMulticastSender deschide un socket UDP care trimite către grupul multicast.
func NewMulticastSender(groupAddr string) (*UDPTransport, error) {
	group, err := net.ResolveUDPAddr("udp4", groupAddr)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialUDP("udp4", nil, group)
	if err != nil {
		return nil, err
	}
	return &UDPTransport{conn: conn, group: group}, nil
}

// NewMulticastReceiver intră în grupul multicast și ascultă pe portul lui.
func NewMulticastReceiver(groupAddr string) (*UDPTransport, error) {
	group, err := net.ResolveUDPAddr("udp4", groupAddr)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenMulticastUDP("udp4", nil, group)
	if err != nil {
		return nil, err
	}
	return &UDPTransport{conn: conn}, nil
}

func (u *UDPTransport) Send(m *Message) error {
	data, err := m.ToXML()
	if err != nil {
		return err
	}
	if len(data) > 1400 { // ca să încapă într-o singură datagramă fără fragmentare
		return fmt.Errorf("mesaj prea mare pentru o datagramă UDP (%d octeți)", len(data))
	}
	_, err = u.conn.Write(data)
	return err
}

func (u *UDPTransport) Receive() (*Message, error) {
	buf := make([]byte, maxFrame)
	n, _, err := u.conn.ReadFromUDP(buf)
	if err != nil {
		return nil, err
	}
	return decode(buf[:n])
}

func (u *UDPTransport) Close() error { return u.conn.Close() }

func decode(data []byte) (*Message, error) {
	m, err := ParseXML(data)
	if err != nil {
		return nil, &InvalidMessageError{Reason: err.Error()}
	}
	if err := m.Validate(); err != nil {
		return m, &InvalidMessageError{Reason: err.Error()}
	}
	return m, nil
}
