package json

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jxbdlut/leaf/chanrpc"
	"github.com/jxbdlut/leaf/log"
	"reflect"
)

type Processor struct {
	msgInfo map[string]*MsgInfo
	msgSeq  uint32
	cbChan       map[uint32]chan interface{}
}

type MsgInfo struct {
	msgType       reflect.Type
	msgRouter     *chanrpc.Server
	msgHandler    MsgHandler
	msgRawHandler MsgHandler
}

type MsgHandler func([]interface{})

type MsgRaw struct {
	msgID      string
	msgRawData json.RawMessage
}

func NewProcessor() *Processor {
	p := new(Processor)
	p.msgInfo = make(map[string]*MsgInfo)
	p.msgSeq = 0
	return p
}

// It's dangerous to call the method on routing or marshaling (unmarshaling)
func (p *Processor) Register(msg interface{}) string {
	msgType := reflect.TypeOf(msg)
	if msgType == nil || msgType.Kind() != reflect.Ptr {
		log.Fatal("json message pointer required")
	}
	msgID := msgType.Elem().Name()
	if msgID == "" {
		log.Fatal("unnamed json message")
	}
	if _, ok := p.msgInfo[msgID]; ok {
		log.Fatal("message %v is already registered", msgID)
	}

	i := new(MsgInfo)
	i.msgType = msgType
	p.msgInfo[msgID] = i
	return msgID
}

// It's dangerous to call the method on routing or marshaling (unmarshaling)
func (p *Processor) SetRouter(msg interface{}, msgRouter *chanrpc.Server) {
	msgType := reflect.TypeOf(msg)
	if msgType == nil || msgType.Kind() != reflect.Ptr {
		log.Fatal("json message pointer required")
	}
	msgID := msgType.Elem().Name()
	i, ok := p.msgInfo[msgID]
	if !ok {
		log.Fatal("message %v not registered", msgID)
	}

	i.msgRouter = msgRouter
}

// It's dangerous to call the method on routing or marshaling (unmarshaling)
func (p *Processor) SetHandler(msg interface{}, msgHandler MsgHandler) {
	msgType := reflect.TypeOf(msg)
	if msgType == nil || msgType.Kind() != reflect.Ptr {
		log.Fatal("json message pointer required")
	}
	msgID := msgType.Elem().Name()
	i, ok := p.msgInfo[msgID]
	if !ok {
		log.Fatal("message %v not registered", msgID)
	}

	i.msgHandler = msgHandler
}

// It's dangerous to call the method on routing or marshaling (unmarshaling)
func (p *Processor) SetRawHandler(msgID string, msgRawHandler MsgHandler) {
	i, ok := p.msgInfo[msgID]
	if !ok {
		log.Fatal("message %v not registered", msgID)
	}

	i.msgRawHandler = msgRawHandler
}

// goroutine safe
func (p *Processor) Route(msg interface{}, seq uint32, userData interface{}) error {
	// cbChan
	if cbChan, ok := p.cbChan[seq]; ok {
		cbChan <- msg
		return nil
	}

	// raw
	if msgRaw, ok := msg.(MsgRaw); ok {
		i, ok := p.msgInfo[msgRaw.msgID]
		if !ok {
			return fmt.Errorf("message %v not registered", msgRaw.msgID)
		}
		if i.msgRawHandler != nil {
			i.msgRawHandler([]interface{}{msgRaw.msgID, msgRaw.msgRawData, userData})
		}
		return nil
	}

	// json
	msgType := reflect.TypeOf(msg)
	if msgType == nil || msgType.Kind() != reflect.Ptr {
		return errors.New("json message pointer required")
	}
	msgID := msgType.Elem().Name()
	i, ok := p.msgInfo[msgID]
	if !ok {
		return fmt.Errorf("message %v not registered", msgID)
	}
	if i.msgHandler != nil {
		i.msgHandler([]interface{}{msg, userData})
	}
	if i.msgRouter != nil {
		i.msgRouter.Go(msgType, msg, userData)
	}
	return nil
}

// goroutine safe
func (p *Processor) Unmarshal(data []byte) (interface{}, uint32, error) {
	var m map[string]json.RawMessage
	err := json.Unmarshal(data, &m)
	if err != nil {
		return nil, 0, err
	}
	if len(m) != 1 {
		return nil, 0, errors.New("invalid json data")
	}

	for key, value := range m {
		var seq uint32 = 0
		if key == "seq" {
			seq = key
			continue
		}
		msgID := key
		data := value
		i, ok := p.msgInfo[msgID]
		if !ok {
			return nil, 0, fmt.Errorf("message %v not registered", msgID)
		}

		// msg
		if i.msgRawHandler != nil {
			return MsgRaw{msgID, data}, seq, nil
		} else {
			msg := reflect.New(i.msgType.Elem()).Interface()
			return msg, seq, json.Unmarshal(data, msg)
		}
	}

	panic("bug")
}

// goroutine safe
func (p *Processor) Marshal(msg interface{}) ([][]byte, error) {
	msgType := reflect.TypeOf(msg)
	if msgType == nil || msgType.Kind() != reflect.Ptr {
		return nil, errors.New("json message pointer required")
	}
	msgID := msgType.Elem().Name()
	if _, ok := p.msgInfo[msgID]; !ok {
		return nil, fmt.Errorf("message %v not registered", msgID)
	}
	// seq
	p.msgSeq++

	// data
	m := map[string]interface{}{msgID: msg, "seq": p.msgSeq}
	data, err := json.Marshal(m)
	return [][]byte{data}, err
}
