package rdb

import (
	"fmt"
	"math"
	"time"
)

type FileEncoder struct {
	writer       *FileWriter
	countPos     int64
	count        int64
	countWithExp int64
	backlenBuf   []byte
	redisVersion string
	begin        bool
}

func NewFileEncoder(path string, redisVersion string) (*FileEncoder, error) {
	w, err := newFileWriter(path)
	if redisVersion == "" {
		return nil, fmt.Errorf("missing Redis version")
	}
	if err != nil {
		return nil, err
	}
	return &FileEncoder{
		redisVersion: redisVersion,
		writer:       w,
		backlenBuf:   make([]byte, 5),
		begin:        false,
	}, nil
}

func (s *FileEncoder) Begin() error {
	if _, err := s.writer.Write([]byte("REDIS")); err != nil {
		return err
	}
	if _, err := s.writer.Write([]byte(fmt.Sprintf("%04d", Version))); err != nil {
		return err
	}
	if err := s.writeAuxField("redis-ver", s.redisVersion); err != nil {
		return err
	}
	if err := s.writeAuxField("redis-bits", "64"); err != nil {
		return err
	}
	if err := s.writeAuxField("ctime", fmt.Sprintf("%d", time.Now().Unix())); err != nil {
		return err
	}
	if err := s.selectDB(0); err != nil {
		return err
	}
	resizeDbPos, err := s.writer.Pos()
	if err != nil {
		return err
	}
	s.countPos = resizeDbPos
	if err := s.writeResizeDB(0, 0); err != nil {
		return err
	}
	return nil
}

func (s *FileEncoder) WriteStringEntry(key string, value string, expiry time.Time) error {
	if s.begin {
		return fmt.Errorf("cannot write; a collection is already being written. Call Close on the existing collection first")
	}
	if err := s.writeExpiry(expiry); err != nil {
		return err
	}
	if err := s.writer.WriteByte(byte(TypeString)); err != nil {
		return err
	}
	if err := s.writeString(key); err != nil {
		return err
	}
	if err := s.writeString(value); err != nil {
		return err
	}
	s.count++
	return nil
}

func (s *FileEncoder) BeginHash(key string, expiry time.Time) (*HashEncoder, error) {
	if s.begin {
		return nil, fmt.Errorf("cannot begin; a collection is already being written. Call Close on the existing collection first")
	}
	s.begin = true
	if err := s.writeExpiry(expiry); err != nil {
		return nil, err
	}
	err := s.writeTypeAndKey(TypeHash, key)
	if err != nil {
		return nil, err
	}
	s.count++
	return NewHashEncoder(s)
}

func (s *FileEncoder) BeginHashWithMetadata(key string, expiry time.Time) (*HashMetadataEncoder, error) {
	if s.begin {
		return nil, fmt.Errorf("cannot begin; a collection is already being written. Call Close on the existing collection first")
	}
	s.begin = true
	if err := s.writeExpiry(expiry); err != nil {
		return nil, err
	}
	err := s.writeTypeAndKey(TypeHashMetadata, key)
	if err != nil {
		return nil, err
	}
	s.count++
	return NewHashMetadataEncoder(s)
}

func (s *FileEncoder) BeginStream(key string, expiry time.Time) (*StreamEncoder, error) {
	if s.begin {
		return nil, fmt.Errorf("cannot begin; a collection is already being written. Call Close on the existing collection first")
	}
	s.begin = true
	if err := s.writeExpiry(expiry); err != nil {
		return nil, err
	}
	err := s.writeTypeAndKey(TypeStreamListpacks, key)
	if err != nil {
		return nil, err
	}
	s.count++
	return NewStreamEncoder(s)
}

func (s *FileEncoder) BeginList(key string, expiry time.Time) (*ListEncoder, error) {
	if s.begin {
		return nil, fmt.Errorf("cannot begin; a collection is already being written. Call Close on the existing collection first")
	}
	s.begin = true
	if err := s.writeExpiry(expiry); err != nil {
		return nil, err
	}
	err := s.writeTypeAndKey(TypeList, key)
	if err != nil {
		return nil, err
	}
	s.count++
	return NewListEncoder(s)
}

func (s *FileEncoder) BeginSet(key string, expiry time.Time) (*SetEncoder, error) {
	if s.begin {
		return nil, fmt.Errorf("cannot begin; a collection is already being written. Call Close on the existing collection first")
	}
	s.begin = true
	if err := s.writeExpiry(expiry); err != nil {
		return nil, err
	}
	err := s.writeTypeAndKey(TypeSet, key)
	if err != nil {
		return nil, err
	}
	s.count++
	return NewSetEncoder(s)
}

func (s *FileEncoder) BeginSortedSet(key string, expiry time.Time) (*SortedSetEncoder, error) {
	if s.begin {
		return nil, fmt.Errorf("cannot begin; a collection is already being written. Call Close on the existing collection first")
	}
	s.begin = true
	if err := s.writeExpiry(expiry); err != nil {
		return nil, err
	}
	err := s.writeTypeAndKey(TypeZset2, key)
	if err != nil {
		return nil, err
	}
	s.count++
	return NewSortedSetEncoder(s)
}

func (s *FileEncoder) WriteJSON(key string, json string, expiry time.Time) error {
	if s.begin {
		return fmt.Errorf("cannot write; a collection is already being written. Call Close on the existing collection first")
	}
	if err := s.writeExpiry(expiry); err != nil {
		return err
	}
	err := s.writeTypeAndKey(TypeModule2, key)
	if err != nil {
		return err
	}
	err = s.writeModuleId(jsonModuleID, jsonModuleV3)
	if err != nil {
		return err
	}

	err = s.writeModuleString(json)
	if err != nil {
		return err
	}

	err = s.writeModuleEOF()
	if err != nil {
		return err
	}
	s.count++
	return nil
}

func (s *FileEncoder) Close() error {
	err := s.writeEOF()
	if err != nil {
		return err
	}
	err = s.writer.WriteUint64(0)
	if err != nil {
		return err
	}
	err = s.writer.Flush()
	if err != nil {
		return err
	}
	_, err = s.writer.SeekPos(s.countPos)
	if err != nil {
		return err
	}
	err = s.writeResizeDB(int(s.count), int(s.countWithExp))
	if err != nil {
		return err
	}
	err = s.writer.Flush()
	return err
}

func (s *FileEncoder) writeAuxField(key, value string) error {
	if err := s.writer.WriteByte(byte(typeOpCodeAux)); err != nil {
		return err
	}
	if err := s.writeString(key); err != nil {
		return err
	}
	err := s.writeString(value)
	return err
}

func (s *FileEncoder) selectDB(dbNumber int) error {
	if err := s.writer.WriteByte(byte(typeOpCodeSelectDB)); err != nil {
		return err
	}
	if err := s.writer.WriteLength(uint64(dbNumber)); err != nil {
		return err
	}
	return nil
}

func (s *FileEncoder) writeResizeDB(dbSize, expiryDBSize int) error {
	if err := s.writer.WriteByte(byte(typeOpCodeResizeDB)); err != nil {
		return err
	}
	if err := s.writer.WriteLengthUint64(uint64(dbSize)); err != nil {
		return err
	}
	if err := s.writer.WriteLengthUint64(uint64(expiryDBSize)); err != nil {
		return err
	}
	return nil
}

func (s *FileEncoder) writeEOF() error {
	err := s.writer.WriteByte(byte(typeOpCodeEOF))
	return err
}

func (s *FileEncoder) writeTypeAndKey(t Type, key string) error {
	if err := s.writer.WriteByte(byte(t)); err != nil {
		return err
	}
	if err := s.writeString(key); err != nil {
		return err
	}
	return nil
}

func (s *FileEncoder) writeModuleId(id, version uint64) error {
	moduleID := id & 0xFFFFFFFFFFFFFC00
	moduleID |= version & 0x000000000000003FF
	return s.writer.WriteLength(moduleID)
}

func (s *FileEncoder) writeModuleString(value string) error {
	err := s.writer.WriteLength(moduleOpCodeString)
	if err != nil {
		return err
	}

	return s.writeString(value)
}

func (s *FileEncoder) writeModuleEOF() error {
	return s.writer.WriteLength(moduleOpCodeEOF)
}

func (s *FileEncoder) writeExpiry(expiry time.Time) error {
	if expiry.IsZero() {
		return nil
	}
	if err := s.writer.WriteByte(byte(typeOpCodeExpireTimeMS)); err != nil {
		return err
	}
	msTimestamp := uint64(time.Until(expiry).Milliseconds())
	if err := s.writer.WriteUint64(msTimestamp); err != nil {
		return err
	}
	s.countWithExp++
	return nil
}

func (s *FileEncoder) writeListpackStrEntry(value string) (uint32, error) {
	// we always write 32 bit long strings for simplicity
	err := s.writer.WriteUint8(listpackEnc32bitStrLen)
	if err != nil {
		return 0, err
	}

	bytes := stringToBytes(value)
	// length of <lpentry-data>
	err = s.writer.WriteUint32(uint32(len(bytes)))
	if err != nil {
		return 0, err
	}

	// <lpentry-data>
	_, err = s.writer.Write(bytes)
	if err != nil {
		return 0, err
	}

	// <backlen>
	backLen := 5 + len(bytes)
	var backLenLen uint32
	if backLen <= 127 {
		backLenLen = 1
		s.backlenBuf[0] = byte(backLen)
		_, err = s.writer.Write(s.backlenBuf[:1])
	} else if backLen < 16383 {
		backLenLen = 2
		s.backlenBuf[0] = byte(backLen >> 7)
		s.backlenBuf[1] = byte((backLen & 127) | 128)
		_, err = s.writer.Write(s.backlenBuf[:2])
	} else if backLen < 2097151 {
		backLenLen = 3
		s.backlenBuf[0] = byte(backLen >> 14)
		s.backlenBuf[1] = byte(((backLen >> 7) & 127) | 128)
		s.backlenBuf[2] = byte((backLen & 127) | 128)
		_, err = s.writer.Write(s.backlenBuf[:3])
	} else if backLen < 268435455 {
		backLenLen = 4
		s.backlenBuf[0] = byte(backLen >> 21)
		s.backlenBuf[1] = byte(((backLen >> 14) & 127) | 128)
		s.backlenBuf[2] = byte(((backLen >> 7) & 127) | 128)
		s.backlenBuf[3] = byte((backLen & 127) | 128)
		_, err = s.writer.Write(s.backlenBuf[:4])
	} else {
		backLenLen = 5
		s.backlenBuf[0] = byte(backLen >> 28)
		s.backlenBuf[1] = byte(((backLen >> 21) & 127) | 128)
		s.backlenBuf[2] = byte(((backLen >> 14) & 127) | 128)
		s.backlenBuf[3] = byte(((backLen >> 7) & 127) | 128)
		s.backlenBuf[4] = byte((backLen & 127) | 128)
		_, err = s.writer.Write(s.backlenBuf[:5])
	}

	if err != nil {
		return 0, err
	}

	// encoding + entry data len + entry data + back len
	return 1 + 4 + uint32(len(bytes)) + backLenLen, nil
}

func (s *FileEncoder) writeListpackIntEntry(value int64) (uint32, error) {
	var encoding, encodingLen uint8
	if math.MinInt16 <= value && value <= math.MaxInt16 {
		encoding = listpackEncInt16
		encodingLen = 2
	} else if math.MinInt32 <= value && value <= math.MaxInt32 {
		encoding = listpackEncInt32
		encodingLen = 4
	} else {
		encoding = listpackEncInt64
		encodingLen = 8
	}

	err := s.writer.WriteUint8(encoding)
	if err != nil {
		return 0, err
	}

	switch encodingLen {
	case 2:
		err = s.writer.WriteUint16(uint16(value))
	case 4:
		err = s.writer.WriteUint32(uint32(value))
	case 8:
		err = s.writer.WriteUint64(uint64(value))
	}

	if err != nil {
		return 0, err
	}

	// +1 for the first byte, specifying the encoding
	err = s.writer.WriteUint8(1 + encodingLen)
	if err != nil {
		return 0, err
	}

	// encoding + encoding len + back len
	// back len is 1 because encoding is always 1 byte and encoding
	// len is at most 8, which makes back len less than 127, so 1
	// byte is enough to represent it.
	return uint32(1 + encodingLen + 1), nil
}

func (s *FileEncoder) writeString(value string) error {
	err := s.writer.WriteLength(uint64(len(value)))
	if err != nil {
		return err
	}
	_, err = s.writer.Write([]byte(value))
	return err
}
