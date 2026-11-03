// Package halfopen is a TCP proxy for tests whose open connections can be
// made half-open: after Freeze, bytes on them are read and dropped in both
// directions and nothing is closed, so each end still holds an established
// connection that never delivers again. Connections accepted after the freeze
// forward normally, as a reconnect over a healthy path would.
package halfopen

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
)

// Proxy forwards connections on a loopback address to a target address.
type Proxy struct {
	lis      net.Listener
	target   string
	accepted atomic.Int32
	done     chan struct{}

	mu    sync.Mutex
	pairs []*pair
}

type pair struct {
	client, upstream net.Conn
	frozen           atomic.Bool
}

func (p *pair) close() {
	p.client.Close()
	p.upstream.Close()
}

// Start listens on a loopback port and forwards each connection to target.
func Start(target string) (*Proxy, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}
	p := &Proxy{lis: lis, target: target, done: make(chan struct{})}
	go p.acceptLoop()
	return p, nil
}

// Addr is where clients connect.
func (p *Proxy) Addr() string { return p.lis.Addr().String() }

// Accepted counts the connections accepted so far.
func (p *Proxy) Accepted() int { return int(p.accepted.Load()) }

// Freeze makes every connection open now half-open.
func (p *Proxy) Freeze() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, pr := range p.pairs {
		pr.frozen.Store(true)
	}
}

// Close stops accepting and closes every connection, frozen ones included.
func (p *Proxy) Close() error {
	err := p.lis.Close()
	<-p.done
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, pr := range p.pairs {
		pr.close()
	}
	return err
}

func (p *Proxy) acceptLoop() {
	defer close(p.done)
	for {
		c, err := p.lis.Accept()
		if err != nil {
			return // closed
		}
		p.accepted.Add(1)
		up, err := net.Dial("tcp", p.target)
		if err != nil {
			c.Close()
			continue
		}
		pr := &pair{client: c, upstream: up}
		p.mu.Lock()
		p.pairs = append(p.pairs, pr)
		p.mu.Unlock()
		go pump(pr, c, up)
		go pump(pr, up, c)
	}
}

// pump copies from one side to the other until a side closes. A normal close
// is passed on; a frozen connection closes nothing.
func pump(pr *pair, from, to net.Conn) {
	buf := make([]byte, 16<<10)
	for {
		n, err := from.Read(buf)
		if n > 0 && !pr.frozen.Load() {
			if _, werr := to.Write(buf[:n]); werr != nil {
				err = werr
			}
		}
		if err != nil {
			break
		}
	}
	if !pr.frozen.Load() {
		pr.close()
	}
}
