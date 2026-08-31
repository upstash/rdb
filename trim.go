package rdb

import "os"

func TrimFile(path string) (int, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return 0, err
	}

	fileLen := int(info.Size())
	buf := newFileBackedBuffer(file, fileLen, minInt(fileLen, 1<<20))

	err = readFile(buf, nopHandler{}, uint64(defaultMaxEntrySize))
	if err != nil {
		return 0, err
	}

	end := buf.Pos()
	if end == fileLen {
		return 0, nil
	}

	err = file.Truncate(int64(end))
	if err != nil {
		return 0, err
	}

	err = file.Sync()
	if err != nil {
		return 0, err
	}

	return fileLen - end, nil
}
