package rdb

import (
	"fmt"
	"math"
	"time"
)

type CollectionEncoder interface {
	WriteZeroLength() error

	WriteFieldStrStr(key string, value string) error

	WriteFieldStrStrWithExpiry(key string, value string, expiry time.Time) error

	WriteFieldStr(field string) error

	WriteFieldStrFloat64(field string, value float64) error

	WriteFieldUint64Str(index uint64, value string) error

	Close() error
}

type baseCollectionEncoder struct {
	encoder   *FileEncoder
	length    int64
	lengthPos int64
}

func (s *baseCollectionEncoder) WriteZeroLength() error {
	startPos, _ := s.encoder.writer.Pos()
	err := s.encoder.writer.WriteLengthUint64(0)
	if err != nil {
		return err
	}
	s.length = 0
	s.lengthPos = startPos
	return nil
}

func (s *baseCollectionEncoder) Close() error {
	finalPos, err := s.encoder.writer.Pos()
	if err != nil {
		return err
	}
	_, err = s.encoder.writer.SeekPos(s.lengthPos)
	if err != nil {
		return err
	}
	err = s.encoder.writer.WriteLengthUint64(uint64(s.length))
	if err != nil {
		return err
	}
	_, err = s.encoder.writer.SeekPos(finalPos)
	s.encoder.begin = false
	return err
}

func (s *baseCollectionEncoder) WriteFieldStrStr(key string, value string) error {
	panic("implement me")
}

func (s *baseCollectionEncoder) WriteFieldStr(field string) error {
	panic("implement me")
}

func (s *baseCollectionEncoder) WriteFieldStrFloat64(field string, value float64) error {
	panic("implement me")
}

func (s *baseCollectionEncoder) WriteFieldStrStrWithExpiry(key string, value string, expiry time.Time) error {
	panic("implement me")
}

func (s *baseCollectionEncoder) WriteFieldUint64Str(index uint64, value string) error {
	panic("implement me")
}

type ListEncoder struct {
	baseCollectionEncoder
}

func NewListEncoder(e *FileEncoder) (*ListEncoder, error) {
	encoder := &ListEncoder{}
	encoder.encoder = e
	err := encoder.WriteZeroLength()
	return encoder, err
}

func (s *ListEncoder) WriteFieldStr(val string) error {
	err := s.encoder.writeString(val)
	if err != nil {
		return err
	}
	s.length++
	return nil
}

type SetEncoder struct {
	baseCollectionEncoder
}

func NewSetEncoder(e *FileEncoder) (*SetEncoder, error) {
	encoder := &SetEncoder{}
	encoder.encoder = e
	err := encoder.WriteZeroLength()
	if err != nil {
		return nil, err
	}
	return encoder, nil
}

func (s *SetEncoder) WriteFieldStr(field string) error {
	err := s.encoder.writeString(field)
	if err != nil {
		return err
	}
	s.length++
	return nil
}

type SortedSetEncoder struct {
	baseCollectionEncoder
}

func NewSortedSetEncoder(e *FileEncoder) (*SortedSetEncoder, error) {
	encoder := &SortedSetEncoder{}
	encoder.encoder = e
	err := encoder.WriteZeroLength()
	if err != nil {
		return nil, err
	}
	return encoder, nil
}

func (s *SortedSetEncoder) WriteFieldStrFloat64(key string, value float64) error {
	err := s.encoder.writeString(key)
	if err != nil {
		return err
	}
	score := math.Float64bits(value)
	err = s.encoder.writer.WriteUint64(score)
	if err != nil {
		return err
	}
	s.length++
	return nil
}

type HashEncoder struct {
	baseCollectionEncoder
}

func NewHashEncoder(e *FileEncoder) (*HashEncoder, error) {
	encoder := &HashEncoder{}
	encoder.encoder = e
	err := encoder.WriteZeroLength()
	if err != nil {
		return nil, err
	}
	return encoder, nil
}

func (s *HashEncoder) WriteFieldStrStr(key string, value string) error {
	err := s.encoder.writeString(key)
	if err != nil {
		return err
	}
	err = s.encoder.writeString(value)
	if err != nil {
		return err
	}
	s.length++
	return nil
}

type ArrayEncoder struct {
	baseCollectionEncoder
}

// NewArrayEncoder creates an encoder for an array whose insert cursor is
// insertIndex, which is the index the last ARINSERT wrote to. The next
// insertion goes to insertIndex + 1. It must be set to ArrayInsertIndexNone
// for the arrays that have no cursor.
//
// Redis rejects the empty arrays, so at least one element must be written
// before the returned encoder is closed.
func NewArrayEncoder(e *FileEncoder, insertIndex uint64) (*ArrayEncoder, error) {
	encoder := &ArrayEncoder{}
	encoder.encoder = e
	err := encoder.WriteZeroLength()
	if err != nil {
		return nil, err
	}
	// The cursor is stored right after the length, and its size depends on
	// whether the array has one, so unlike the length, it cannot be written
	// back on Close.
	err = encoder.writeInsertIndex(insertIndex)
	if err != nil {
		return nil, err
	}
	return encoder, nil
}

// Close writes the final length of the array back, and fails for the empty
// arrays, which Redis rejects while reading them.
func (s *ArrayEncoder) Close() error {
	if s.length == 0 {
		return errEmptyArray
	}

	return s.baseCollectionEncoder.Close()
}

func (s *ArrayEncoder) writeInsertIndex(insertIndex uint64) error {
	// The cursor cannot be stored unconditionally, as the length encoding
	// cannot represent the value used to mark the missing cursors.
	if insertIndex == ArrayInsertIndexNone {
		return s.encoder.writer.WriteLength(0)
	}

	if err := s.encoder.writer.WriteLength(1); err != nil {
		return err
	}

	return s.encoder.writer.WriteLength(insertIndex)
}

func (s *ArrayEncoder) WriteFieldUint64Str(index uint64, value string) error {
	if index > arrayMaxIndex {
		return fmt.Errorf("invalid array index %d", index)
	}

	if err := s.encoder.writer.WriteLength(index); err != nil {
		return err
	}

	tag, ival, fval := encodeArrayValue(value)
	if err := s.encoder.writer.WriteLength(tag); err != nil {
		return err
	}

	var err error
	switch tag {
	case arrayTagInt:
		err = s.encoder.writer.WriteUint64(uint64(ival))
	case arrayTagFloat:
		err = s.encoder.writer.WriteUint64(math.Float64bits(fval))
	default:
		err = s.encoder.writeString(value)
	}

	if err != nil {
		return err
	}

	s.length++
	return nil
}

type HashMetadataEncoder struct {
	baseCollectionEncoder
}

func NewHashMetadataEncoder(e *FileEncoder) (*HashMetadataEncoder, error) {
	encoder := &HashMetadataEncoder{}
	encoder.encoder = e
	// Redis optimizes storage by placing the minimum expiration timestamp at the start
	// and then writing only the diff for fields.
	// Since we don't know the minimum expiration timestamp, we write a dummy value here.
	// All the expiration timestamps written with fields will be absolute.
	err := e.writer.WriteUint64(0)
	if err != nil {
		return nil, err
	}
	err = encoder.WriteZeroLength()
	if err != nil {
		return nil, err
	}
	return encoder, nil
}

func (s *HashMetadataEncoder) WriteFieldStrStrWithExpiry(key string, value string, expiry time.Time) error {
	ms := int64(0)
	if !expiry.IsZero() {
		ms = expiry.UnixMilli() + 1
	}
	err := s.encoder.writer.WriteLength(uint64(ms))
	if err != nil {
		return err
	}
	err = s.encoder.writeString(key)
	if err != nil {
		return err
	}
	err = s.encoder.writeString(value)
	if err != nil {
		return err
	}
	s.length++
	return nil
}
