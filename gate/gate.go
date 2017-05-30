package gate

import (
	"errors"
	"github.com/jxbdlut/leaf/chanrpc"
	"github.com/jxbdlut/leaf/log"
	"github.com/jxbdlut/leaf/network"
	"github.com/jxbdlut/leaf/util"
	"net"
	"reflect"
	"sync/atomic"
	"time"
)

type Gate struct {
	MaxConnNum      int
	PendingWriteNum int
	MaxMsgLen       uint32
	Processor       network.Processor
	AgentChanRPC    *chanrpc.Server

	// websocket
	WSAddr      string
	HTTPTimeout time.Duration
	CertFile    string
	KeyFile     string

	// tcp
	TCPAddr      string
	LenMsgLen    int
	LittleEndian bool
}

func (gate *Gate) Run(closeSig chan bool) {
	var wsServer *network.WSServer
	if gate.WSAddr != "" {
		wsServer = new(network.WSServer)
		wsServer.Addr = gate.WSAddr
		wsServer.MaxConnNum = gate.MaxConnNum
		wsServer.PendingWriteNum = gate.PendingWriteNum
		wsServer.MaxMsgLen = gate.MaxMsgLen
		wsServer.HTTPTimeout = gate.HTTPTimeout
		wsServer.CertFile = gate.CertFile
		wsServer.KeyFile = gate.KeyFile
		wsServer.NewAgent = func(conn *network.WSConn) network.Agent {
			a := &agent{conn: conn, gate: gate}
			if gate.AgentChanRPC != nil {
				gate.AgentChanRPC.Go("NewAgent", a)
			}
			return a
		}
	}

	var tcpServer *network.TCPServer
	if gate.TCPAddr != "" {
		tcpServer = new(network.TCPServer)
		tcpServer.Addr = gate.TCPAddr
		tcpServer.MaxConnNum = gate.MaxConnNum
		tcpServer.PendingWriteNum = gate.PendingWriteNum
		tcpServer.LenMsgLen = gate.LenMsgLen
		tcpServer.MaxMsgLen = gate.MaxMsgLen
		tcpServer.LittleEndian = gate.LittleEndian
		tcpServer.NewAgent = func(conn *network.TCPConn) network.Agent {
			a := &agent{conn: conn, gate: gate, timeout: 10 * time.Second}
			a.cbChan = new(util.Map)
			if gate.AgentChanRPC != nil {
				gate.AgentChanRPC.Go("NewAgent", a)
			}
			return a
		}
	}

	if wsServer != nil {
		wsServer.Start()
	}
	if tcpServer != nil {
		tcpServer.Start()
	}
	<-closeSig
	if wsServer != nil {
		wsServer.Close()
	}
	if tcpServer != nil {
		tcpServer.Close()
	}
}

func (gate *Gate) OnDestroy() {}

type agent struct {
	conn     network.Conn
	gate     *Gate
	timeout  time.Duration
	userData interface{}
	seq      uint32
	cbChan   *util.Map
}

func (a *agent) Run() {
	for {
		data, err := a.conn.ReadMsg()
		if err != nil {
			log.Debug("read message: %v", err)
			break
		}

		if a.gate.Processor != nil {
			msg, seq, err := a.gate.Processor.Unmarshal(data)
			if err != nil {
				log.Debug("unmarshal message error: %v", err)
				break
			}
			// cbChan
			if cbChan := a.cbChan.Get(seq); cbChan != nil {
				cbChan.(chan interface{}) <- msg
				a.DelTimeOut(seq)
			} else {
				err = a.gate.Processor.Route(msg, seq, a)
				if err != nil {
					log.Debug("route message error: %v", err)
					break
				}
			}
		}
	}
}

func (a *agent) DelTimeOut(seq uint32) bool {
	if a.cbChan.Get(seq) != nil {
		a.cbChan.Del(seq)
		return true
	}
	return false
}

func (a *agent) OnClose() {
	if a.gate.AgentChanRPC != nil {
		err := a.gate.AgentChanRPC.Call0("CloseAgent", a)
		if err != nil {
			log.Error("chanrpc error: %v", err)
		}
	}
}

func (a *agent) Replay(msg interface{}, seq uint32) {
	a.WriteMsg(msg, nil, seq)
}

func (a *agent) Send(msg interface{}) {
	atomic.AddUint32(&a.seq, 1)
	a.WriteMsg(msg, nil, a.seq)
}

func (a *agent) SendRcv(msg interface{}) (interface{}, error) {
	cbChan := make(chan interface{})
	atomic.AddUint32(&a.seq, 1)
	a.WriteMsg(msg, cbChan, a.seq)
	select {
	case msg := <-cbChan:
		return msg, nil
	case <-time.After(a.timeout):
		a.DelTimeOut(a.seq)
		return nil, errors.New("time out")
	}
}

func (a *agent) WriteMsg(msg interface{}, cbChan chan interface{}, seq uint32) {
	if a.gate.Processor != nil {
		if cbChan != nil {
			a.cbChan.Set(seq, cbChan)
		}
		data, err := a.gate.Processor.Marshal(msg, seq)
		if err != nil {
			log.Error("marshal message %v error: %v", reflect.TypeOf(msg), err)
			return
		}
		err = a.conn.WriteMsg(data...)
		if err != nil {
			log.Error("write message %v error: %v", reflect.TypeOf(msg), err)
			return
		}
	}
}

func (a *agent) LocalAddr() net.Addr {
	return a.conn.LocalAddr()
}

func (a *agent) RemoteAddr() net.Addr {
	return a.conn.RemoteAddr()
}

func (a *agent) Close() {
	a.conn.Close()
}

func (a *agent) Destroy() {
	a.conn.Destroy()
}

func (a *agent) UserData() interface{} {
	return a.userData
}

func (a *agent) SetUserData(data interface{}) {
	a.userData = data
}
