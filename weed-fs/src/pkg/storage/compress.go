package storage

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"io/ioutil"
	"strings"
)

// is the extension or the mime-type compressable?
func IsGzippable(ext, mtype string) bool {
	switch ext {
	case ".zip", ".rar", ".jpg", ".jpeg", ".png", ".bz2", ".xz":
		return false
	}
	if strings.HasPrefix(mtype, "text/") || strings.HasPrefix(mtype, "application/") {
		return true
	}
	return false
}

// gzips bytes
func GzipData(input []byte) ([]byte, error) {
	buf := new(bytes.Buffer)
	w, err := gzip.NewWriterLevel(buf, flate.BestCompression)
	if err != nil {
		return nil, err
	}
	if _, err = w.Write(input); err != nil {
		println("error compressing data:", err)
		w.Close()
		return nil, err
	}
	if err = w.Close(); err != nil {
		println("error closing compressed data:", err)
		return nil, err
	}
	return buf.Bytes(), nil
}

// ungzips bytes
func UnGzipData(input []byte) ([]byte, error) {
	buf := bytes.NewBuffer(input)
	r, err := gzip.NewReader(buf)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	output, err := ioutil.ReadAll(r)
	if err != nil {
		println("error uncompressing data:", err)
		return nil, err
	}
	return output, nil
}
