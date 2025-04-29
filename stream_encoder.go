package rdb

import (
	"encoding/binary"
)

type StreamEncoder struct {
	encoder     *FileEncoder
	entryLength int64
	lengthPos   int64
	masterIDBuf []byte
}

func NewStreamEncoder(e *FileEncoder) (*StreamEncoder, error) {
	s := &StreamEncoder{
		encoder:     e,
		masterIDBuf: make([]byte, 16),
	}
	startPos, _ := s.encoder.writer.Pos()
	err := s.encoder.writer.WriteLengthUint64(0)
	if err != nil {
		return nil, err
	}
	s.entryLength = 0
	s.lengthPos = startPos
	return s, nil
}

func (s *StreamEncoder) WriteEntry(entry StreamEntry) error {
	// master id as two big endian 64 bit numbers
	binary.BigEndian.PutUint64(s.masterIDBuf[:8], entry.ID.Millis)
	binary.BigEndian.PutUint64(s.masterIDBuf[8:], entry.ID.Seq)

	s.encoder.writeString(bytesToString(s.masterIDBuf))

	// we need to write the following entry as a listpack string,
	// which is prefixed by a length. we don't know the length of
	// the listpack here, so we write a dummy 64 bit len here. at
	// the end of the writing the listpack, we will come back to
	// this position and write the actual len.
	strLenPos, err := s.encoder.writer.Pos()
	if err != nil {
		return err
	}
	s.encoder.writer.WriteLengthUint64(0)

	// similar to the str len, we don't know total bytes the
	// listpack has or the number of listpack entries before hand.
	// we will write dummy values here, and come back to this position
	// to writer the actual values.
	lpBytesPos, err := s.encoder.writer.Pos()
	if err != nil {
		return err
	}
	var lpBytes, lpCount uint32

	// lpbytes
	err = s.encoder.writer.WriteUint32(0)
	if err != nil {
		return err
	}

	// lplen
	err = s.encoder.writer.WriteUint16(0)
	if err != nil {
		return err
	}

	// count - always 1 because we write each entry as a seperate listpack
	size, err := s.encoder.writeListpackIntEntry(1)
	if err != nil {
		return err
	}
	lpBytes += size
	lpCount++

	// deleted - always 0 because we don't write deleted entries.
	size, err = s.encoder.writeListpackIntEntry(0)
	if err != nil {
		return err
	}
	lpBytes += size
	lpCount++

	// num fields
	// each entry.value has the following form:
	// <field><value>....<field><value>
	size, err = s.encoder.writeListpackIntEntry(int64(len(entry.Value) / 2))
	if err != nil {
		return err
	}
	lpBytes += size
	lpCount++

	// field names
	for i := 0; i < len(entry.Value); i += 2 {
		size, err = s.encoder.writeListpackStrEntry(entry.Value[i])
		if err != nil {
			return err
		}
		lpBytes += size
		lpCount++
	}

	// 0 at the end of the master entry
	size, err = s.encoder.writeListpackIntEntry(0)
	if err != nil {
		return err
	}
	lpBytes += size
	lpCount++

	// flags - always 2 to signal that the field names are the same
	// with master entry encoded above
	size, err = s.encoder.writeListpackIntEntry(2)
	if err != nil {
		return err
	}
	lpBytes += size
	lpCount++

	// id millis delta - always 0 becuase we only have one entry, which
	// is the one encoded as the master entry.
	size, err = s.encoder.writeListpackIntEntry(0)
	if err != nil {
		return err
	}
	lpBytes += size
	lpCount++

	// id seq delta - always 0 becuase we only have one entry, which
	// is the one encoded as the master entry.
	size, err = s.encoder.writeListpackIntEntry(0)
	if err != nil {
		return err
	}
	lpBytes += size
	lpCount++

	// values
	// each entry.value has the following form:
	// <field><value>....<field><value>
	for i := 1; i < len(entry.Value); i += 2 {
		size, err = s.encoder.writeListpackStrEntry(entry.Value[i])
		if err != nil {
			return err
		}
		lpBytes += size
		lpCount++
	}

	// lp entry count for the field - since we have the same fields with the
	// master entry, we only write field names (len = len(entry.value)/2). The
	// other lp entries are: this entry + <millis-delta> + <seq-delta>
	size, err = s.encoder.writeListpackIntEntry(int64(3 + len(entry.Value)/2))
	if err != nil {
		return err
	}
	lpBytes += size
	lpCount++

	// lpend
	err = s.encoder.writer.WriteUint8(listpackEnd)
	if err != nil {
		return err
	}

	lpBytes += 4 + 2 + 1 // lpbytes + lpcount + lpend

	// remember the current position and write the actual
	// lpbytes and lpcount
	pos, err := s.encoder.writer.Pos()
	if err != nil {
		return err
	}
	_, err = s.encoder.writer.SeekPos(lpBytesPos)
	if err != nil {
		return err
	}

	err = s.encoder.writer.WriteUint32(lpBytes)
	if err != nil {
		return err
	}

	if lpCount >= uint32(listpackLenBig) {
		lpCount = uint32(listpackLenBig)
	}

	err = s.encoder.writer.WriteUint16(uint16(lpCount))
	if err != nil {
		return err
	}

	// write the actual str len of the listpack
	_, err = s.encoder.writer.SeekPos(strLenPos)
	if err != nil {
		return err
	}
	err = s.encoder.writer.WriteLengthUint64(uint64(lpBytes))
	if err != nil {
		return err
	}

	// go back to actual position
	_, err = s.encoder.writer.SeekPos(pos)
	s.entryLength++
	return err
}

func (s *StreamEncoder) WriteMetadata(length uint64, lastID StreamID) error {
	err := s.encoder.writer.WriteLength(length)
	if err != nil {
		return err
	}

	err = s.encoder.writer.WriteLength(lastID.Millis)
	if err != nil {
		return err
	}

	return s.encoder.writer.WriteLength(lastID.Seq)
}

func (s *StreamEncoder) WriteGroups(groups []StreamConsumerGroup) error {
	err := s.encoder.writer.WriteLength(uint64(len(groups)))
	if err != nil {
		return err
	}

	for _, group := range groups {
		err = s.encoder.writeString(group.Name)
		if err != nil {
			return err
		}

		err = s.encoder.writer.WriteLength(group.LastID.Millis)
		if err != nil {
			return err
		}

		err = s.encoder.writer.WriteLength(group.LastID.Seq)
		if err != nil {
			return err
		}

		globalPEL := make(map[StreamID]*StreamPendingEntry)
		for _, consumer := range group.Consumers {
			for _, pe := range consumer.PendingEntries {
				globalPEL[pe.Entry.ID] = pe
			}
		}

		err = s.encoder.writer.WriteLength(uint64(len(globalPEL)))
		if err != nil {
			return err
		}

		for _, pe := range globalPEL {
			err = s.encoder.writer.WriteUint64BE(pe.Entry.ID.Millis)
			if err != nil {
				return err
			}

			err = s.encoder.writer.WriteUint64BE(pe.Entry.ID.Seq)
			if err != nil {
				return err
			}

			err = s.encoder.writer.WriteUint64BE(uint64(pe.DeliveryTime))
			if err != nil {
				return err
			}

			err = s.encoder.writer.WriteLength(pe.DeliveryCount)
			if err != nil {
				return err
			}
		}

		err = s.encoder.writer.WriteLength(uint64(len(group.Consumers)))
		if err != nil {
			return err
		}

		for _, consumer := range group.Consumers {
			err = s.encoder.writeString(consumer.Name)
			if err != nil {
				return err
			}

			err = s.encoder.writer.WriteUint64(uint64(consumer.SeenTime))
			if err != nil {
				return err
			}

			err = s.encoder.writer.WriteLength(uint64(len(consumer.PendingEntries)))
			if err != nil {
				return err
			}

			for _, pe := range consumer.PendingEntries {
				err = s.encoder.writer.WriteUint64BE(pe.Entry.ID.Millis)
				if err != nil {
					return err
				}

				err = s.encoder.writer.WriteUint64BE(pe.Entry.ID.Seq)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (s *StreamEncoder) Close() error {
	finalPos, err := s.encoder.writer.Pos()
	if err != nil {
		return err
	}
	_, err = s.encoder.writer.SeekPos(s.lengthPos)
	if err != nil {
		return err
	}
	err = s.encoder.writer.WriteLengthUint64(uint64(s.entryLength))
	if err != nil {
		return err
	}
	_, err = s.encoder.writer.SeekPos(finalPos)
	return err
}
