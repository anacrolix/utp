# utp
[![GoDoc](https://godoc.org/github.com/anacrolix/utp?status.svg)](https://godoc.org/github.com/anacrolix/utp)

Package utp implements uTP, the micro transport protocol as used with Bittorrent. It opts for simplicity and reliability over strict adherence to the (poor) spec. It allows using the underlying OS-level transport despite dispatching uTP on top to allow for example, shared socket use with DHT. Additionally, multiple uTP connections can share the same OS socket, to truly realize uTP's claim to be light on system and network switching resources.
