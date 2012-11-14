package directory

import (
	"encoding/hex"
	"fmt"
	"pkg/storage"
	"pkg/util"
	"strings"
)

type FileId struct {
	VolumeId storage.VolumeId
	Key      uint64
	Hashcode uint32
}

func NewFileId(VolumeId storage.VolumeId, Key uint64, Hashcode uint32) *FileId {
	return &FileId{VolumeId: VolumeId, Key: Key, Hashcode: Hashcode}
}
func ParseFileId(fid string) (*FileId, error) {
	a := strings.Split(fid, ",")
	if len(a) != 2 {
		return nil, fmt.Errorf("Invalid fid=%s, split length=%d", fid, len(a))
	}
	vid_string, key_hash_string := a[0], a[1]
	volumeId, _ := storage.NewVolumeId(vid_string)
	key, hash, err := storage.ParseKeyHash(key_hash_string)
	if err != nil {
		return nil, err
	}
	return &FileId{VolumeId: volumeId, Key: key, Hashcode: hash}, nil
}
func (n *FileId) String() string {
	bytes := make([]byte, 12)
	util.Uint64toBytes(bytes[0:8], n.Key)
	util.Uint32toBytes(bytes[8:12], n.Hashcode)
	nonzero_index := 0
	for ; bytes[nonzero_index] == 0; nonzero_index++ {
	}
	return n.VolumeId.String() + "," + hex.EncodeToString(bytes[nonzero_index:])
}
