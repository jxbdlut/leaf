package gate

import (
	"net"
)

type Agent interface {
	Replay(msg interface{}, seq uint32)
	Send(msg interface{})
	SendRcv(msg interface{}) (interface{}, error)
	WriteMsg(msg interface{}, cbChan chan interface{}, seq uint32)
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
	Close()
	Destroy()
	UserData() interface{}
	SetUserData(data interface{})
}
