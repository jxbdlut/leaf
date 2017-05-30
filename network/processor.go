package network

type Processor interface {
	// must goroutine safe
	Route(msg interface{}, seq uint32, userData interface{}) error
	// must goroutine safe
	Unmarshal(data []byte) (interface{}, uint32, error)
	// must goroutine safe
	Marshal(msg interface{}, seq uint32) ([][]byte, error)
}
