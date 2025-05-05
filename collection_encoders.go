package rdb

import (
	"math"
	"time"
)

type CollectionEncoder interface {
	WriteZeroLength() error

	WriteFieldStrStr(key string, value string) error

	WriteFieldStrStrWithExpiry(key string, value string, expiry *time.Time) error

	WriteFieldStr(field string) error

	WriteFieldStrFloat64(field string, value float64) error

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

func (s *baseCollectionEncoder) WriteFieldStrStrWithExpiry(key string, value string, expiry *time.Time) error {
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
