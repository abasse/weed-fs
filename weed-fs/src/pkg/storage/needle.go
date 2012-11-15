package storage

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"net/http"
	"net/textproto"
	"os"
	"pkg/util"
	"strconv"
	"strings"
)

const (
	PadLen     = 8  // padding length (volume phisical size is 2^32 * PadLen)
	HeaderSize = 24 // header size
	CksumLen   = 4  // checksum (CRC32) length
)

const (
	FlagGzipped = 1 << iota
)

type Needle struct {
	Cookie   uint32               "random number to mitigate brute force lookups"
	Id       uint64               "needle id"
	Size     uint32               "Data size"
	Data     []byte               "The actual file data"
	Checksum CRC                  "CRC32 to check integrity"
	infosize uint16               "size of headers"
	Flags    uint8                "flags (gzipped?)"
	reserved [5]byte              "reserved for future"
	Info     textproto.MIMEHeader "headers (HTTP, like Content-Type, X-Filename, Content-Disposition)"
	Padding  []byte               "Aligned to 8 bytes"
}

// returns a new needle read from the HTTP request
func NewNeedle(r *http.Request) (n *Needle, fname string, e error) {
	n = new(Needle)
	form, fe := r.MultipartReader()
	if fe != nil {
		fmt.Println("MultipartReader [ERROR]", fe)
		e = fe
		return
	}
	part, fe := form.NextPart()
	if fe != nil {
		e = fe
		return
	}
	fname = part.FileName()
	if p := strings.LastIndexAny(fname, `/\`); p > -1 {
		fname = fname[p+1:]
	}
	part.Header.Add("X-Filename", fname)
	ext := ""
	ctype := part.Header.Get("Content-Type")
	if ctype == "" {
		dotIndex := strings.LastIndex(fname, ".")
		if dotIndex > 0 {
			ext = fname[dotIndex:]
			ctype = mime.TypeByExtension(ext)
		}
	}
	data, fe := ioutil.ReadAll(part)
	if fe != nil {
		e = fe
		return
	}
	//log.Println("uploading file " + part.FileName())
	if IsGzippable(ext, ctype) {
		n.Flags |= FlagGzipped
		if data, e = GzipData(data); e != nil {
			return
		}

	}
	n.Data = data
	n.Checksum = NewCRC(data)
	n.Info = part.Header

	commaSep := strings.LastIndex(r.URL.Path, ",")
	dotSep := strings.LastIndex(r.URL.Path, ".")
	fid := r.URL.Path[commaSep+1:]
	if dotSep > 0 {
		fid = r.URL.Path[commaSep+1 : dotSep]
	}

	e = n.ParsePath(fid)
	return
}

// parses volumeid,fileid+key
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
	header[16] = n.Flags
	info, err := HeaderToBytes(n.Info)
	if err != nil {
		return 0, err
	}
	n.infosize = uint16(len(info))
	util.Uint16toBytes(header[17:19], n.infosize)
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
	if n.infosize > 0 {
		if _, err = w.Write(info); err != nil {
			return 0, err
		}
	}
	rest := PadLen - ((n.Size + HeaderSize + uint32(n.infosize) + 4) % PadLen)
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
	header := make([]byte, HeaderSize+size+CksumLen)
	ret, e := io.ReadFull(r, header)
	if e != nil {
		return 0, e
	}
	n.Cookie = util.BytesToUint32(header[0:4])
	n.Id = util.BytesToUint64(header[4:12])
	n.Size = util.BytesToUint32(header[12:16])
	n.Flags = header[16]
	n.infosize = util.BytesToUint16(header[17:19])
	n.Data = header[HeaderSize : HeaderSize+size]
	checksum := util.BytesToUint32(header[HeaderSize+size : HeaderSize+size+CksumLen])
	if checksum != NewCRC(n.Data).Value() {
		return 0, errors.New("CRC error! Data On Disk Corrupted!")
	}
	if n.infosize > 0 {
		info := make([]byte, n.infosize, n.infosize+2)
		if _, e = io.ReadFull(r, info); e != nil {
			return ret, fmt.Errorf("error reading %d bytes as info: %s", n.infosize, e)
		}
		info = info[:cap(info)]
		info[n.infosize], info[n.infosize+1] = '\r', '\n'
		mr := textproto.NewReader(bufio.NewReader(
			bytes.NewReader(info)))
		n.Info, e = mr.ReadMIMEHeader()
		if e != nil {
			return ret, fmt.Errorf("cannot read info (%s): %s", info, e)
		}
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
	n.Flags = bytes[16]
	n.infosize = util.BytesToUint16(bytes[17:19])
	rest := PadLen - ((n.Size + HeaderSize + uint32(n.infosize) + CksumLen) % PadLen)
	return n, n.Size + CksumLen + uint32(n.infosize) + rest, nil
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

// is the Data gzipped?
func (n Needle) IsGzipped() bool {
	return n.Flags&FlagGzipped > 0
}

// Allowed headers
var AllowedHeaders = map[string]bool{"Content-Type": true, "X-Filename": true}

func HeaderToBytes(header textproto.MIMEHeader) ([]byte, error) {
	hdrs := make(http.Header, len(AllowedHeaders))
	for k, v := range header {
		k = http.CanonicalHeaderKey(k)
		if _, ok := AllowedHeaders[k]; ok {
			hdrs[k] = v
		}
	}
	// FIXME: Content-Disposition
	hbuf := bytes.NewBuffer(nil)
	if err := hdrs.Write(hbuf); err != nil {
		return nil, err
	} else {
		if len(hbuf.Bytes()) >= 1<<16-1 {
			return hbuf.Bytes()[:1<<16-1], nil
		} else {
			return hbuf.Bytes(), nil
		}
	}
	return nil, errors.New("unreachable")
}
