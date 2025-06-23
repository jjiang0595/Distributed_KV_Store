package cluster

import (
	"errors"
	"log"
	"net"
	"sync"
	"sync/atomic"
)

type MockListener struct {
	addr      net.Addr
	connCh    chan net.Conn
	closeOnce sync.Once
	isClosed  atomic.Bool
	closeSig  chan struct{}
}

func NewMockListener(addr string) *MockListener {
	mockAddr, err := net.ResolveTCPAddr("tcp", addr)
	if mockAddr == nil || err != nil {
		log.Fatalf("Error: Invalid Address %s: %v", addr, err)
	}
	return &MockListener{
		addr:     mockAddr,
		closeSig: make(chan struct{}),
	}
}

func (m *MockListener) Accept() (net.Conn, error) {
	if m.isClosed.Load() {
		return nil, errors.New("listener is closed")
	}
	select {
	case conn := <-m.connCh:
		return conn, nil
	case <-m.closeSig:
		return nil, net.ErrClosed
	}
}

func (m *MockListener) Close() error {
	m.closeOnce.Do(func() {
		m.isClosed.Store(true)
		close(m.closeSig)
	})
	return nil
}

func (m *MockListener) Addr() net.Addr {
	return m.addr
}
