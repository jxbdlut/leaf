package protobuf

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/jxbdlut/leaf/chanrpc"
	"github.com/jxbdlut/leaf/log"
	"math"
	"reflect"
)

// -----------------------------
// | id | seq | protobuf message |
// -----------------------------
type Processor struct {
	littleEndian bool
	msgInfo      []*MsgInfo
	msgID        map[reflect.Type]uint16
}

type MsgInfo struct {
	msgType       reflect.Type
	msgRouter     *chanrpc.Server
	msgHandler    MsgHandler
	msgRawHandler MsgHandler
}

type MsgHandler func([]interface{})

type MsgRaw struct {
	msgID      uint16
	msgRawData []byte
}

func NewProcessor() *Processor {
	p := new(Processor)
	p.littleEndian = false
	p.msgID = make(map[reflect.Type]uint16)
	return p
}

// It's dangerous to call the method on routing or marshaling (unmarshaling)
func (p *Processor) SetByteOrder(littleEndian bool) {
	p.littleEndian = littleEndian
}

// It's dangerous to call the method on routing or marshaling (unmarshaling)
func (p *Processor) Register(msg proto.Message) uint16 {
	msgType := reflect.TypeOf(msg)
	if msgType == nil || msgType.Kind() != reflect.Ptr {
		log.Fatal("protobuf message pointer required")
	}
	if _, ok := p.msgID[msgType]; ok {
		log.Fatal("message %s is already registered", msgType)
	}
	if len(p.msgInfo) >= math.MaxUint16 {
		log.Fatal("too many protobuf messages (max = %v)", math.MaxUint16)
	}

	i := new(MsgInfo)
	i.msgType = msgType
	p.msgInfo = append(p.msgInfo, i)
	id := uint16(len(p.msgInfo) - 1)
	p.msgID[msgType] = id
	return id
}

// It's dangerous to call the method on routing or marshaling (unmarshaling)
func (p *Processor) SetRouter(msg proto.Message, msgRouter *chanrpc.Server) {
	msgType := reflect.TypeOf(msg)
	id, ok := p.msgID[msgType]
	if !ok {
		log.Fatal("message %s not registered", msgType)
	}

	p.msgInfo[id].msgRouter = msgRouter
}

// It's dangerous to call the method on routing or marshaling (unmarshaling)
func (p *Processor) SetHandler(msg proto.Message, msgHandler MsgHandler) {
	msgType := reflect.TypeOf(msg)
	id, ok := p.msgID[msgType]
	if !ok {
		log.Fatal("message %s not registered", msgType)
	}

	p.msgInfo[id].msgHandler = msgHandler
}

// It's dangerous to call the method on routing or marshaling (unmarshaling)
func (p *Processor) SetRawHandler(id uint16, msgRawHandler MsgHandler) {
	if id >= uint16(len(p.msgInfo)) {
		log.Fatal("message id %v not registered", id)
	}

	p.msgInfo[id].msgRawHandler = msgRawHandler
}

// goroutine safe
func (p *Processor) Route(msg interface{}, seq uint32, userData interface{}) error {
	// raw
	if msgRaw, ok := msg.(MsgRaw); ok {
		if msgRaw.msgID >= uint16(len(p.msgInfo)) {
			return fmt.Errorf("message id %v not registered", msgRaw.msgID)
		}
		i := p.msgInfo[msgRaw.msgID]
		if i.msgRawHandler != nil {
			i.msgRawHandler([]interface{}{msgRaw.msgID, msgRaw.msgRawData, userData})
		}
		return nil
	}

	// protobuf
	msgType := reflect.TypeOf(msg)
	id, ok := p.msgID[msgType]
	if !ok {
		return fmt.Errorf("message %s not registered", msgType)
	}
	i := p.msgInfo[id]
	if i.msgHandler != nil {
		i.msgHandler([]interface{}{msg, userData, seq})
	}
	if i.msgRouter != nil {
		i.msgRouter.Go(msgType, msg, userData, seq)
	}
	return nil
}

// goroutine safe
func (p *Processor) Unmarshal(data []byte) (interface{}, uint32, error) {
	if len(data) < 6 {
		return nil, 0, errors.New("protobuf data too short")
	}

	// id
	var id uint16
	if p.littleEndian {
		id = binary.LittleEndian.Uint16(data)
	} else {
		id = binary.BigEndian.Uint16(data)
	}
	if id >= uint16(len(p.msgInfo)) {
		return nil, 0, fmt.Errorf("message id %v not registered", id)
	}

	// seq
	var seq uint32
	if p.littleEndian {
		seq = binary.LittleEndian.Uint32(data[2:])
	} else {
		seq = binary.BigEndian.Uint32(data[2:])
	}

	// msg
	i := p.msgInfo[id]
	if i.msgRawHandler != nil {
		return MsgRaw{id, data[6:]}, seq, nil
	} else {
		msg := reflect.New(i.msgType.Elem()).Interface()
		return msg, seq, proto.UnmarshalMerge(data[6:], msg.(proto.Message))
	}
}

// goroutine safe
func (p *Processor) Marshal(msg interface{}, seq uint32) ([][]byte, error) {
	msgType := reflect.TypeOf(msg)

	// id
	_id, ok := p.msgID[msgType]
	if !ok {
		err := fmt.Errorf("message %s not registered", msgType)
		return nil, err
	}

	id := make([]byte, 2)
	if p.littleEndian {
		binary.LittleEndian.PutUint16(id, _id)
	} else {
		binary.BigEndian.PutUint16(id, _id)
	}

	// seq
	seqByte := make([]byte, 4)
	if p.littleEndian {
		binary.LittleEndian.PutUint32(seqByte, seq)
	} else {
		binary.BigEndian.PutUint32(seqByte, seq)
	}

	// data
	data, err := proto.Marshal(msg.(proto.Message))
	return [][]byte{id, seqByte, data}, err
}

// goroutine safe
func (p *Processor) Range(f func(id uint16, t reflect.Type)) {
	for id, i := range p.msgInfo {
		f(uint16(id), i.msgType)
	}
}
