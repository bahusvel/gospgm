package gospgm

import (
	"encoding/binary"
	"log"
	"net"
)

type Source struct {
	addr         *net.UDPAddr
	conn         *net.UDPConn
	replayBuffer []dataPacket
	bufferSize   int
	maxBufSize   int
	seqNum       uint32
}

func NewSource(addr string, maxBufSize int) *Source {
	src := Source{maxBufSize: maxBufSize}
	var err error

	src.addr, err = net.ResolveUDPAddr("udp", addr)
	if err != nil {
		log.Fatal(err)
	}

	src.conn, err = net.DialUDP("udp", nil, src.addr)
	if err != nil {
		log.Fatal(err)
	}

	return &src
}

func encodeDataPacket(packet dataPacket) []byte {
	buff := make([]byte, 4)
	binary.BigEndian.PutUint32(buff, packet.seqNum)
	return append(buff, packet.data...)
}

func (this *Source) Broadcast(message []byte) error {
	packet := dataPacket{seqNum: this.seqNum, data: message}
	this.seqNum++

	data := encodeDataPacket(packet)

	for this.bufferSize+len(message) > this.maxBufSize {
		minusSize := len(this.replayBuffer[0].data)
		this.replayBuffer = this.replayBuffer[1:]
		this.bufferSize -= minusSize
	}

	this.replayBuffer = append(this.replayBuffer, packet)
	this.bufferSize += len(message)

	_, err := this.conn.Write(data)
	if err != nil {
		return err
	}
	return nil
}

type dataPacket struct {
	seqNum uint32
	data   []byte
}

type sourceState struct {
	addr      *net.UDPAddr
	seqNum    uint32
	unordered map[uint32]dataPacket
}

type Receiver struct {
	addr    *net.UDPAddr
	conn    *net.UDPConn
	sources map[string]*sourceState
}

func NewReceiver(addr string) *Receiver {
	rec := Receiver{}
	var err error

	rec.addr, err = net.ResolveUDPAddr("udp", addr)
	if err != nil {
		log.Fatal(err)
	}

	rec.conn, err = net.ListenMulticastUDP("udp", nil, rec.addr)
	if err != nil {
		log.Fatal(err)
	}

	err = rec.conn.SetReadBuffer(8192)
	if err != nil {
		log.Fatal(err)
	}

	return &rec
}

func decodeDataPacket(data []byte) dataPacket {
	packet := dataPacket{}
	packet.seqNum = binary.BigEndian.Uint32(data)
	packet.data = data[4:]
	return packet
}

func (this *Receiver) ReceiveMessage() ([]byte, error) {
	for _, v := range this.sources {
		if p, ok := v.unordered[v.seqNum+1]; ok {
			v.seqNum = p.seqNum
			delete(v.unordered, p.seqNum)
			return p.data, nil
		}
	}
READ:
	buff := make([]byte, 8192)
	n, src, err := this.conn.ReadFromUDP(buff)
	if err != nil {
		return []byte{}, err
	}
	packet := decodeDataPacket(buff[:n])

	state, ok := this.sources[src.String()]
	if !ok {
		state = &sourceState{addr: src, seqNum: packet.seqNum, unordered: map[uint32]dataPacket{}}
		this.sources[src.String()] = state
		return packet.data, nil
	}

	if state.seqNum != packet.seqNum-1 {
		// out of sequence packet
		state.unordered[packet.seqNum] = packet
		goto READ
	}

	state.seqNum = packet.seqNum
	return packet.data, nil
}
