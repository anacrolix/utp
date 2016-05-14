package utp

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAcceptOnDestroyedSocket(t *testing.T) {
	pc, err := net.ListenPacket("udp", "localhost:0")
	require.NoError(t, err)
	s, err := NewSocketFromPacketConn(pc)
	require.NoError(t, err)
	go pc.Close()
	_, err = s.Accept()
	t.Log(err)
}
