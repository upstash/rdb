package rdb

type moduleWriter struct {
	writer *Writer
}

func (w *moduleWriter) WriteJSON(json string) error {
	err := w.writeModuleId(jsonModuleID, jsonModuleV3)
	if err != nil {
		return err
	}

	err = w.writeString(json)
	if err != nil {
		return err
	}

	return w.writeEOF()
}

func (w *moduleWriter) writeModuleId(id, version uint64) error {
	moduleID := id & 0xFFFFFFFFFFFFFC00
	moduleID |= version & 0x000000000000003FF
	return w.writer.writeLen(moduleID)
}

func (w *moduleWriter) writeString(value string) error {
	err := w.writer.writeLen(moduleOpCodeString)
	if err != nil {
		return err
	}

	return w.writer.WriteString(value)
}

func (w *moduleWriter) writeEOF() error {
	return w.writer.writeLen(moduleOpCodeEOF)
}
