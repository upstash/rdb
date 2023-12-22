package rdb

import (
	"errors"
	"strconv"
)

type Stream struct {
	LastID  StreamID
	Entries []StreamEntry
	Length  uint64
	Groups  []StreamConsumerGroup
}

type StreamEntry struct {
	ID    StreamID
	Value []string
}

type StreamID struct {
	Millis uint64
	Seq    uint64
}

type StreamConsumerGroup struct {
	Name        string
	LastID      StreamID
	EntriesRead int64
	Consumers   []StreamConsumer
}

type StreamConsumer struct {
	Name           string
	SeenTime       int64
	ActiveTime     int64
	PendingEntries []*StreamPendingEntry
}

type StreamPendingEntry struct {
	Entry         StreamEntry
	DeliveryTime  int64
	DeliveryCount uint64
}

func (r *valueReader) readStreamListpacks0(
	t Type,
	entryCB func(StreamEntry) error,
	groupCB func(StreamConsumerGroup) error,
) (uint64, error) {
	entriesView, err := r.buf.View(r.buf.Pos())
	if err != nil {
		return 0, err
	}
	defer entriesView.Close()

	// first pass over entries, we read all into cb
	err = r.readStreamEntries(entryCB)
	if err != nil {
		return 0, err
	}

	_, _, err = r.readLen() // length
	if err != nil {
		return 0, err
	}

	_, _, err = r.readLen() // last id millis
	if err != nil {
		return 0, err
	}

	_, _, err = r.readLen() // last id seq
	if err != nil {
		return 0, err
	}

	if t >= TypeStreamListpacks2 {
		_, _, err = r.readLen() // first id millis
		if err != nil {
			return 0, err
		}

		_, _, err = r.readLen() // first id seq
		if err != nil {
			return 0, err
		}

		_, _, err = r.readLen() // max deleted entry id millis
		if err != nil {
			return 0, err
		}

		_, _, err = r.readLen() // max deleted entry id seq
		if err != nil {
			return 0, err
		}

		_, _, err = r.readLen() // entries added
		if err != nil {
			return 0, err
		}
	}

	groupsView, err := r.buf.View(r.buf.Pos())
	if err != nil {
		return 0, err
	}
	defer groupsView.Close()

	// first pass over group, we detect all the pending entries in the stream
	pendingEntries := make(map[StreamID][]string)
	err = r.readStreamConsumerGroups(t, func(group StreamConsumerGroup) error {
		for _, c := range group.Consumers {
			for _, pe := range c.PendingEntries {
				pendingEntries[pe.Entry.ID] = nil // value will be set later
			}
		}

		return nil
	})
	if err != nil {
		return 0, err
	}

	entriesViewReader := valueReader{
		buf:           entriesView,
		maxLz77StrLen: r.maxLz77StrLen,
	}

	var read uint64
	// second pass over entries, we read the values of the pending entries we collected above
	// and calculate the number of entries read.
	err = entriesViewReader.readStreamEntries(func(entry StreamEntry) error {
		read++
		if _, ok := pendingEntries[entry.ID]; ok {
			pendingEntries[entry.ID] = entry.Value
		}

		return nil
	})
	if err != nil {
		return 0, err
	}

	groupsViewReader := valueReader{
		buf:           groupsView,
		maxLz77StrLen: r.maxLz77StrLen,
	}

	// second pass over entries, we read all into cb, after setting the pending entry values
	err = groupsViewReader.readStreamConsumerGroups(t, func(group StreamConsumerGroup) error {
		for _, c := range group.Consumers {
			for _, pe := range c.PendingEntries {
				pe.Entry.Value = pendingEntries[pe.Entry.ID]
				if pe.Entry.Value == nil {
					return errors.New("illegal state: an entry is in PEL but there is no corresponding entry in stream")
				}
			}
		}

		return groupCB(group)
	})

	if err != nil {
		return 0, err
	}

	return read, nil
}

func (r *valueReader) readStreamEntries(cb func(StreamEntry) error) error {
	lpCount, _, err := r.readLen()
	if err != nil {
		return err
	}

	for i := uint64(0); i < lpCount; i++ {
		masterIDS, err := r.ReadString()
		if err != nil {
			return err
		}

		masterIDReader := valueReader{
			buf:           newMemoryBackedBuffer(stringToBytes(masterIDS)),
			maxLz77StrLen: r.maxLz77StrLen,
		}

		masterIDMillis, err := masterIDReader.readUint64BE()
		if err != nil {
			return err
		}

		masterIDSeq, err := masterIDReader.readUint64BE()
		if err != nil {
			return err
		}

		masterID := StreamID{
			Millis: masterIDMillis,
			Seq:    masterIDSeq,
		}

		lp, err := r.ReadString()
		if err != nil {
			return err
		}

		lpReader := valueReader{
			buf:           newMemoryBackedBuffer(stringToBytes(lp)),
			maxLz77StrLen: r.maxLz77StrLen,
		}

		// <lpbytes><lplen>
		err = lpReader.skip(6)
		if err != nil {
			return err
		}

		countS, err := lpReader.readListpackEntry()
		if err != nil {
			return err
		}
		count, err := strconv.Atoi(countS)
		if err != nil {
			return err
		}

		deletedS, err := lpReader.readListpackEntry()
		if err != nil {
			return err
		}
		deleted, err := strconv.Atoi(deletedS)
		if err != nil {
			return err
		}

		numFieldsS, err := lpReader.readListpackEntry()
		if err != nil {
			return err
		}
		numFields, err := strconv.Atoi(numFieldsS)
		if err != nil {
			return err
		}

		masterFieldNames := make([]string, 0)
		for i := 0; i < numFields; i++ {
			name, err := lpReader.readListpackEntry()
			if err != nil {
				return err
			}

			masterFieldNames = append(masterFieldNames, name)
		}

		_, err = lpReader.readListpackEntry() // 0 at the end of the master entry
		if err != nil {
			return err
		}

		total := count + deleted
		for j := 0; j < total; j++ {
			fields := make([]string, 0)

			flagS, err := lpReader.readListpackEntry()
			if err != nil {
				return err
			}
			flag, err := strconv.Atoi(flagS)
			if err != nil {
				return err
			}

			millisDeltaS, err := lpReader.readListpackEntry()
			if err != nil {
				return err
			}
			millisDelta, err := strconv.Atoi(millisDeltaS)
			if err != nil {
				return err
			}

			seqDeltaS, err := lpReader.readListpackEntry()
			if err != nil {
				return err
			}
			seqDelta, err := strconv.Atoi(seqDeltaS)
			if err != nil {
				return err
			}

			var millis, seq uint64
			if millisDelta < 0 {
				millis = masterID.Millis - uint64(-millisDelta)
			} else {
				millis = masterID.Millis + uint64(millisDelta)
			}

			if seqDelta < 0 {
				seq = masterID.Seq - uint64(-seqDelta)
			} else {
				seq = masterID.Seq + uint64(seqDelta)
			}

			id := StreamID{
				Millis: millis,
				Seq:    seq,
			}

			delete := flag&streamItemFlagDeleted != 0
			if flag&streamItemFlagSameFields != 0 {
				for i := 0; i < numFields; i++ {
					value, err := lpReader.readListpackEntry()
					if err != nil {
						return err
					}

					fields = append(fields, masterFieldNames[i], value)
				}

				if !delete {
					entry := StreamEntry{
						ID:    id,
						Value: fields,
					}
					cb(entry)
				}
			} else {
				numFieldsS, err := lpReader.readListpackEntry()
				if err != nil {
					return err
				}
				numFields, err := strconv.Atoi(numFieldsS)
				if err != nil {
					return err
				}

				for i := 0; i < numFields; i++ {
					field, err := lpReader.readListpackEntry()
					if err != nil {
						return err
					}

					value, err := lpReader.readListpackEntry()
					if err != nil {
						return err
					}

					fields = append(fields, field, value)
				}

				if !delete {
					entry := StreamEntry{
						ID:    id,
						Value: fields,
					}
					cb(entry)
				}
			}

			_, err = lpReader.readListpackEntry() // lp entry count
			if err != nil {
				return err
			}
		}

		lpEnd, err := lpReader.readUint8()
		if err != nil {
			return err
		}

		if lpEnd != listpackEnd {
			return errLPUnexpectedEnd
		}
	}

	return nil
}

func (r *valueReader) readStreamConsumerGroups(t Type, cb func(StreamConsumerGroup) error) error {
	count, _, err := r.readLen()
	if err != nil {
		return err
	}

	for i := uint64(0); i < count; i++ {
		name, err := r.ReadString()
		if err != nil {
			return err
		}

		lastIDMillis, _, err := r.readLen()
		if err != nil {
			return err
		}

		lastIDSeq, _, err := r.readLen()
		if err != nil {
			return err
		}

		lastID := StreamID{
			Millis: lastIDMillis,
			Seq:    lastIDSeq,
		}

		var entriesRead int64
		if t >= TypeStreamListpacks2 {
			entriesRead0, _, err := r.readLen()
			if err != nil {
				return err
			}

			entriesRead = int64(entriesRead0)
		}

		globalPELLen, _, err := r.readLen()
		if err != nil {
			return err
		}

		pendingEntries := make(map[StreamID]StreamPendingEntry)
		for j := uint64(0); j < globalPELLen; j++ {
			entryIDMillis, err := r.readUint64BE()
			if err != nil {
				return err
			}

			entryIDSeq, err := r.readUint64BE()
			if err != nil {
				return err
			}

			entryID := StreamID{
				Millis: entryIDMillis,
				Seq:    entryIDSeq,
			}

			deliveryTime0, err := r.readUint64()
			if err != nil {
				return err
			}
			deliveryTime := int64(deliveryTime0)

			deliveryCount, _, err := r.readLen()
			if err != nil {
				return err
			}

			pendingEntries[entryID] = StreamPendingEntry{
				Entry:         StreamEntry{ID: entryID},
				DeliveryTime:  deliveryTime,
				DeliveryCount: deliveryCount,
			}
		}

		consumerCount, _, err := r.readLen()
		if err != nil {
			return err
		}

		consumers := make([]StreamConsumer, 0)
		for j := uint64(0); j < consumerCount; j++ {
			consumerName, err := r.ReadString()
			if err != nil {
				return err
			}

			seenTime0, err := r.readUint64()
			if err != nil {
				return err
			}
			seenTime := int64(seenTime0)

			var activeTime int64
			if t >= TypeStreamListpacks3 {
				activeTime0, err := r.readUint64()
				if err != nil {
					return err
				}

				activeTime = int64(activeTime0)
			}

			pelLen, _, err := r.readLen()
			if err != nil {
				return err
			}

			consumerPEL := make([]*StreamPendingEntry, 0)
			for k := uint64(0); k < pelLen; k++ {
				entryIDMillis, err := r.readUint64BE()
				if err != nil {
					return err
				}

				entryIDSeq, err := r.readUint64BE()
				if err != nil {
					return err
				}

				entryID := StreamID{
					Millis: entryIDMillis,
					Seq:    entryIDSeq,
				}

				pe := pendingEntries[entryID]
				consumerPEL = append(consumerPEL, &pe)
			}

			consumer := StreamConsumer{
				Name:           consumerName,
				SeenTime:       seenTime,
				ActiveTime:     activeTime,
				PendingEntries: consumerPEL,
			}
			consumers = append(consumers, consumer)
		}

		group := StreamConsumerGroup{
			Name:        name,
			LastID:      lastID,
			EntriesRead: entriesRead,
			Consumers:   consumers,
		}
		cb(group)
	}

	return nil
}
