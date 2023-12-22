package rdb

import (
	"encoding/binary"
)

type StreamWriter struct {
	writer *Writer
}

// Instead of delta encoding entries, we write each entry as a
// seperate listpack, to make the implementation simpler.
func (sw *StreamWriter) WriteEntries(entries []StreamEntry) error {
	err := sw.writer.writeLen(uint64(len(entries)))
	if err != nil {
		return err
	}

	var masterIDBuf [16]byte
	var backLenBuf [5]byte
	for _, entry := range entries {
		// master id as two big endian 64 bit numbers
		binary.BigEndian.PutUint64(masterIDBuf[:8], entry.ID.Millis)
		binary.BigEndian.PutUint64(masterIDBuf[8:], entry.ID.Seq)

		sw.writer.WriteString(bytesToString(masterIDBuf[:]))

		// we need to write the following entry as a listpack string,
		// which is prefixed by a length. we don't know the length of
		// the listpack here, so we write a dummy 64 bit len here. at
		// the end of the writing the listpack, we will come back to
		// this position and write the actual len.
		strLenPos := sw.writer.pos
		sw.writer.writeLenUint64(0)

		// similar to the str len, we don't know total bytes the
		// listpack has or the number of listpack entries before hand.
		// we will write dummy values here, and come back to this position
		// to writer the actual values.
		lpBytesPos := sw.writer.pos
		var lpBytes, lpCount uint32

		// lpbytes
		err := sw.writer.writeUint32(0)
		if err != nil {
			return err
		}

		// lplen
		err = sw.writer.writeUint16(0)
		if err != nil {
			return err
		}

		// count - always 1 because we write each entry as a seperate listpack
		size, err := sw.writer.writeListpackIntEntry(1)
		if err != nil {
			return err
		}
		lpBytes += size
		lpCount++

		// deleted - always 0 because we don't write deleted entries.
		size, err = sw.writer.writeListpackIntEntry(0)
		if err != nil {
			return err
		}
		lpBytes += size
		lpCount++

		// num fields
		// each entry.value has the following form:
		// <field><value>....<field><value>
		size, err = sw.writer.writeListpackIntEntry(int64(len(entry.Value) / 2))
		if err != nil {
			return err
		}
		lpBytes += size
		lpCount++

		// field names
		for i := 0; i < len(entry.Value); i += 2 {
			size, err = sw.writer.writeListpackEntry(entry.Value[i], backLenBuf)
			if err != nil {
				return err
			}
			lpBytes += size
			lpCount++
		}

		// 0 at the end of the master entry
		size, err = sw.writer.writeListpackIntEntry(0)
		if err != nil {
			return err
		}
		lpBytes += size
		lpCount++

		// flags - always 2 to signal that the field names are the same
		// with master entry encoded above
		size, err = sw.writer.writeListpackIntEntry(2)
		if err != nil {
			return err
		}
		lpBytes += size
		lpCount++

		// id millis delta - always 0 becuase we only have one entry, which
		// is the one encoded as the master entry.
		size, err = sw.writer.writeListpackIntEntry(0)
		if err != nil {
			return err
		}
		lpBytes += size
		lpCount++

		// id seq delta - always 0 becuase we only have one entry, which
		// is the one encoded as the master entry.
		size, err = sw.writer.writeListpackIntEntry(0)
		if err != nil {
			return err
		}
		lpBytes += size
		lpCount++

		// values
		// each entry.value has the following form:
		// <field><value>....<field><value>
		for i := 1; i < len(entry.Value); i += 2 {
			size, err = sw.writer.writeListpackEntry(entry.Value[i], backLenBuf)
			if err != nil {
				return err
			}
			lpBytes += size
			lpCount++
		}

		// lp entry count for the field - since we have the same fields with the
		// master entry, we only write field names (len = len(entry.value)/2). The
		// other lp entries are: this entry + <millis-delta> + <seq-delta>
		size, err = sw.writer.writeListpackIntEntry(int64(3 + len(entry.Value)/2))
		if err != nil {
			return err
		}
		lpBytes += size
		lpCount++

		// lpend
		err = sw.writer.writeUint8(listpackEnd)
		if err != nil {
			return err
		}

		lpBytes += 4 + 2 + 1 // lpbytes + lpcount + lpend

		// remember the current position and write the actual
		// lpbytes and lpcount
		pos := sw.writer.pos
		sw.writer.pos = lpBytesPos

		err = sw.writer.writeUint32(lpBytes)
		if err != nil {
			return err
		}

		if lpCount >= uint32(listpackLenBig) {
			lpCount = uint32(listpackLenBig)
		}

		err = sw.writer.writeUint16(uint16(lpCount))
		if err != nil {
			return err
		}

		// write the actual str len of the listpack
		sw.writer.pos = strLenPos
		err = sw.writer.writeLenUint64(uint64(lpBytes))
		if err != nil {
			return err
		}

		// go back to actual position
		sw.writer.pos = pos
	}

	return nil
}

func (sw *StreamWriter) WriteMetadata(length uint64, lastID StreamID) error {
	err := sw.writer.writeLen(length)
	if err != nil {
		return err
	}

	err = sw.writer.writeLen(lastID.Millis)
	if err != nil {
		return err
	}

	return sw.writer.writeLen(lastID.Seq)
}

func (sw *StreamWriter) WriteConsumerGroups(groups []StreamConsumerGroup) error {
	err := sw.writer.writeLen(uint64(len(groups)))
	if err != nil {
		return err
	}

	for _, group := range groups {
		err = sw.writer.WriteString(group.Name)
		if err != nil {
			return err
		}

		err = sw.writer.writeLen(group.LastID.Millis)
		if err != nil {
			return err
		}

		err = sw.writer.writeLen(group.LastID.Seq)
		if err != nil {
			return err
		}

		globalPEL := make(map[StreamID]*StreamPendingEntry)
		for _, consumer := range group.Consumers {
			for _, pe := range consumer.PendingEntries {
				globalPEL[pe.Entry.ID] = pe
			}
		}

		err = sw.writer.writeLen(uint64(len(globalPEL)))
		if err != nil {
			return err
		}

		for _, pe := range globalPEL {
			err = sw.writer.writeUint64BE(pe.Entry.ID.Millis)
			if err != nil {
				return err
			}

			err = sw.writer.writeUint64BE(pe.Entry.ID.Seq)
			if err != nil {
				return err
			}

			err = sw.writer.writeUint64(uint64(pe.DeliveryTime))
			if err != nil {
				return err
			}

			err = sw.writer.writeLen(pe.DeliveryCount)
			if err != nil {
				return err
			}
		}

		err = sw.writer.writeLen(uint64(len(group.Consumers)))
		if err != nil {
			return err
		}

		for _, consumer := range group.Consumers {
			err = sw.writer.WriteString(consumer.Name)
			if err != nil {
				return err
			}

			err = sw.writer.writeUint64(uint64(consumer.SeenTime))
			if err != nil {
				return err
			}

			err = sw.writer.writeLen(uint64(len(consumer.PendingEntries)))
			if err != nil {
				return err
			}

			for _, pe := range consumer.PendingEntries {
				err = sw.writer.writeUint64BE(pe.Entry.ID.Millis)
				if err != nil {
					return err
				}

				err = sw.writer.writeUint64BE(pe.Entry.ID.Seq)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}
