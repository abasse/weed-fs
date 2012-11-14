package storage

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"net/http"
	"os"
	"pkg/util"
	"strconv"
	"strings"
)

const (
	PadLen     = 8
	HeaderSize = 17
	CksumLen   = 4
)

type Needle struct {
	Cookie      uint32 "random number to mitigate brute force lookups"
	Id          uint64 "needle id"
	Size        uint32 "Data size"
	Data        []byte "The actual file data"
	Checksum    CRC    "CRC32 to check integrity"
	ctsize      uint8  "content-type size"
	ContentType []byte "file content-type"
	Padding     []byte "Aligned to 8 bytes"
}

func NewNeedle(r *http.Request) (n *Needle, fname string, e error) {

	n = new(Needle)
	form, fe := r.MultipartReader()
	if fe != nil {
		fmt.Println("MultipartReader [ERROR]", fe)
		e = fe
		return
	}
	part, _ := form.NextPart()
	fname = part.FileName()
	ext := ""
	ctype := part.Header.Get("Content-Type")
	if len(n.ContentType) == 0 {
		dotIndex := strings.LastIndex(fname, ".")
		if dotIndex > 0 {
			ext = fname[dotIndex:]
			ctype = mime.TypeByExtension(ext)
		}
	}
	if len(ctype) > 255 {
		n.ContentType = []byte(ctype[:255])
		n.ctsize = 255
	} else {
		n.ContentType = []byte(ctype)
		n.ctsize = uint8(len(n.ContentType))
	}
	data, _ := ioutil.ReadAll(part)
	//log.Println("uploading file " + part.FileName())
	if IsGzippable(ext, ctype) {
		data = GzipData(data)
	}
	n.Data = data
	n.Checksum = NewCRC(data)

	commaSep := strings.LastIndex(r.URL.Path, ",")
	dotSep := strings.LastIndex(r.URL.Path, ".")
	fid := r.URL.Path[commaSep+1:]
	if dotSep > 0 {
		fid = r.URL.Path[commaSep+1 : dotSep]
	}

	e = n.ParsePath(fid)
	return
}
func (n *Needle) ParsePath(fid string) error {
	length := len(fid)
	if length <= 8 {
		if length > 0 {
			return fmt.Errorf("Invalid fid=%s, length=%d", fid, length)
		}
		return nil
	}
	delta := ""
	deltaIndex := strings.LastIndex(fid, "_")
	if deltaIndex > 0 {
		fid, delta = fid[0:deltaIndex], fid[deltaIndex+1:]
	}
	var e error
	if n.Id, n.Cookie, e = ParseKeyHash(fid); e != nil {
		return e
	}
	if delta != "" {
		d, e := strconv.ParseUint(delta, 10, 64)
		if e != nil {
			return e
		}
		n.Id += d
	}
	return nil
}

// appends needle to the writer, returns written data bytes and error
// The written data is header, data, checksum, content-type, and padding
func (n *Needle) Append(w io.Writer) (uint32, error) {
	var err error
	header := make([]byte, HeaderSize)
	util.Uint32toBytes(header[0:4], n.Cookie)
	util.Uint64toBytes(header[4:12], n.Id)
	n.Size = uint32(len(n.Data))
	util.Uint32toBytes(header[12:16], n.Size)
	n.ctsize = uint8(len(n.ContentType))
	header[16] = n.ctsize
	if _, err = w.Write(header); err != nil {
		return 0, err
	}
	if _, err = w.Write(n.Data); err != nil {
		return 0, err
	}
	util.Uint32toBytes(header[0:4], n.Checksum.Value())
	if _, err = w.Write(header[0:4]); err != nil {
		return 0, err
	}
	if n.ctsize > 0 {
		if _, err = w.Write(n.ContentType); err != nil {
			return 0, err
		}
	}
	rest := PadLen - ((n.Size + HeaderSize + uint32(n.ctsize) + 4) % PadLen)
	if rest > 0 {
		for i := uint32(0); i < rest; i++ {
			header[i] = 0
		}
		if _, err = w.Write(header[:rest]); err != nil {
			return 0, err
		}
	}
	return n.Size, nil
}

// reads needle with data, returns read data length and error
func (n *Needle) Read(r io.Reader, size uint32) (int, error) {
	bytes := make([]byte, HeaderSize+size+CksumLen)
	ret, e := io.ReadFull(r, bytes)
	if e != nil {
		return 0, e
	}
	n.Cookie = util.BytesToUint32(bytes[0:4])
	n.Id = util.BytesToUint64(bytes[4:12])
	n.Size = util.BytesToUint32(bytes[12:16])
	n.ctsize = bytes[16]
	n.Data = bytes[HeaderSize : HeaderSize+size]
	checksum := util.BytesToUint32(bytes[HeaderSize+size : HeaderSize+size+CksumLen])
	if checksum != NewCRC(n.Data).Value() {
		return 0, errors.New("CRC error! Data On Disk Corrupted!")
	}
	if n.ctsize > 0 {
		ctype := make([]byte, n.ctsize)
		s, e := io.ReadFull(r, ctype)
		if e != nil {
			return ret, fmt.Errorf("cannot read content-type: %s", e)
		}
		n.ContentType = ctype[:s]
		// ret += s
	}
	return ret, e
}

// returns filled Needle, rest (jump) size and error
func ReadNeedle(r *os.File) (*Needle, uint32, error) {
	n := new(Needle)
	bytes := make([]byte, HeaderSize)
	count, e := r.Read(bytes)
	if count <= 0 || e != nil {
		return nil, 0, e
	}
	n.Cookie = util.BytesToUint32(bytes[0:4])
	n.Id = util.BytesToUint64(bytes[4:12])
	n.Size = util.BytesToUint32(bytes[12:16])
	n.ctsize = bytes[16]
	rest := PadLen - ((n.Size + HeaderSize + uint32(n.ctsize) + CksumLen) % PadLen)
	return n, n.Size + CksumLen + uint32(n.ctsize) + rest, nil
}

// parses key and hash
func ParseKeyHash(key_hash_string string) (uint64, uint32, error) {
	key_hash_bytes, khe := hex.DecodeString(key_hash_string)
	key_hash_len := len(key_hash_bytes)
	if khe != nil || key_hash_len <= 4 {
		return 0, 0, fmt.Errorf("Invalid key_hash=%s length=%d error=%s",
			key_hash_string, key_hash_len, khe)
	}
	key := util.BytesToUint64(key_hash_bytes[0 : key_hash_len-4])
	hash := util.BytesToUint32(key_hash_bytes[key_hash_len-4 : key_hash_len])
	return key, hash, nil
}
