package godb

import "encoding/binary"

const HeaderSize = 5

// State 定义了记录的状态信息，由一个字节表示
// 高4位表示记录类型 (StateType)，低4位表示记录状态 (StateRecord)
type State uint8

// StateType 定义，使用高4位
const (
	TypeNormal   State = 0x00 // 0000 0000 - 普通记录
	TypeBucket   State = 0x10 // 0001 0000 - 桶记录
	TypeKV       State = 0x20 // 0010 0000 - 键值记录
	TypeReserved State = 0x30 // 0011 0000 - 保留类型
	TypeMask     State = 0xF0 // 1111 0000 - 类型掩码
)

// StateRecord 定义，使用低4位
const (
	StateNormal   State = 0x00 // 0000 0000 - 正常状态
	StateDeleted  State = 0x01 // 0000 0001 - 已删除
	StateInvalid  State = 0x02 // 0000 0010 - 无效状态
	StateReserved State = 0x03 // 0000 0011 - 保留状态
	StateMask     State = 0x0F // 0000 1111 - 状态掩码
)

func CombineState(typ State, state State) State {
	return (typ & TypeMask) | (state & StateMask)
}

/*
  - Header format:

+----------+---------------+
|state(1B) | EntrySize(4B) |
+----------+---------------+
*/
type Header [HeaderSize]byte

func (h Header) isValid() bool {
	t := h.StateType()
	return t == TypeBucket || t == TypeKV
}

func (h Header) EntrySize() uint32 {
	return binary.LittleEndian.Uint32(h[1:])
}

func (h *Header) SetEntrySize(size uint32) {
	binary.LittleEndian.PutUint32(h[1:], size)
}
func (h *Header) EncodeState(t State, s State) {
	h[0] = byte((t & TypeMask) | (s & StateMask))
}

func (h *Header) SetType(t State) {
	h[0] = byte((t & TypeMask) | (State(h[0]) & StateMask))
}

func (h *Header) SetRecord(s State) {
	h[0] = byte((State(h[0]) & TypeMask) | (s & StateMask))
}

func (h Header) State() State {
	return State(h[0])
}

func (h Header) StateType() State {
	return h.State() & TypeMask // 取高4位作为记录类型
}

func (h Header) StateRecord() State {
	return h.State() & StateMask // 取低4位作为记录状态
}

// 内联常用操作
func (h Header) IsBucket() bool {
	return (State(h[0]) & TypeMask) == TypeBucket
}

func (h Header) IsKV() bool {
	return (State(h[0]) & TypeMask) == TypeKV
}

func (h Header) IsNormal() bool {
	return h.StateRecord() == StateNormal
}

func (h Header) IsDeleted() bool {
	return h.StateRecord() == StateDeleted
}

func (h Header) IsExpired() bool {
	return h.StateRecord() == StateInvalid
}
