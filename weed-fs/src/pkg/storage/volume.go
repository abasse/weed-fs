package storage

import (
	"errors"
	"log"
	"os"
	"path"
	"sync"
)

const (
	SuperBlockSize = 8
)

type Volume struct {
	Id       VolumeId
	dir      string
	dataFile *os.File
	nm       *NeedleMap

	replicaType ReplicationType

	accessLock sync.Mutex
}

func NewVolume(dirname string, id VolumeId, replicationType ReplicationType) (v *Volume) {
	v = &Volume{dir: dirname, Id: id, replicaType: replicationType}
	v.load()
	return
}
func (v *Volume) load() {
	var e error
	fileName := path.Join(v.dir, v.Id.String())
	v.dataFile, e = os.OpenFile(fileName+".dat", os.O_RDWR|os.O_CREATE, 0644)
	if e != nil {
		log.Fatalf("New Volume [ERROR] %s\n", e)
	}
	if v.replicaType == CopyNil {
		v.readSuperBlock()
	} else {
		v.maybeWriteSuperBlock()
	}
	indexFile, ie := os.OpenFile(fileName+".idx", os.O_RDWR|os.O_CREATE, 0644)
	if ie != nil {
		log.Fatalf("Write Volume Index [ERROR] %s\n", ie)
	}
	v.nm = LoadNeedleMap(indexFile)
}
func (v *Volume) Size() int64 {
	stat, e := v.dataFile.Stat()
	if e == nil {
		return stat.Size()
	}
	return -1
}
func (v *Volume) Close() {
	v.nm.Close()
	v.dataFile.Close()
}
func (v *Volume) maybeWriteSuperBlock() {
	stat, _ := v.dataFile.Stat()
	if stat.Size() == 0 {
		header := make([]byte, SuperBlockSize)
		header[0] = 1
		header[1] = v.replicaType.Byte()
		v.dataFile.Write(header)
	}
}
func (v *Volume) readSuperBlock() {
	v.dataFile.Seek(0, 0)
	header := make([]byte, SuperBlockSize)
	if _, error := v.dataFile.Read(header); error == nil {
		v.replicaType, _ = NewReplicationTypeFromByte(header[1])
	}
}
func (v *Volume) NeedToReplicate() bool {
	return v.replicaType.GetCopyCount() > 1
}

func (v *Volume) write(n *Needle) (uint32, error) {
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	offset, _ := v.dataFile.Seek(0, 2)
	ret, err := n.Append(v.dataFile)
	if err != nil {
		return 0, err
	}
	nv, ok := v.nm.Get(n.Id)
	if !ok || int64(nv.Offset)*8 < offset {
		v.nm.Put(n.Id, uint32(offset/8), n.Size)
	}
	return ret, nil
}
func (v *Volume) delete(n *Needle) uint32 {
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	nv, ok := v.nm.Get(n.Id)
	//log.Println("key", n.Id, "volume offset", nv.Offset, "data_size", n.Size, "cached size", nv.Size)
	if ok {
		v.nm.Delete(n.Id)
		v.dataFile.Seek(int64(nv.Offset*8), 0)
		n.Append(v.dataFile)
		return nv.Size
	}
	return 0
}
func (v *Volume) read(n *Needle) (int, error) {
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	nv, ok := v.nm.Get(n.Id)
	if ok && nv.Offset > 0 {
		v.dataFile.Seek(int64(nv.Offset)*8, 0)
		return n.Read(v.dataFile, nv.Size)
	}
	return -1, errors.New("Not Found")
}

func (v *Volume) compact() error {
	v.accessLock.Lock()
	defer v.accessLock.Unlock()

	filePath := path.Join(v.dir, v.Id.String())
	return v.copyDataAndGenerateIndexFile(filePath+".dat", filePath+".cpd", filePath+".cpx")
}
func (v *Volume) commitCompact() (int, error) {
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	v.dataFile.Close()
	os.Rename(path.Join(v.dir, v.Id.String()+".cpd"), path.Join(v.dir, v.Id.String()+".dat"))
	os.Rename(path.Join(v.dir, v.Id.String()+".cpx"), path.Join(v.dir, v.Id.String()+".idx"))
	v.load()
	return 0, nil
}

func (v *Volume) copyDataAndGenerateIndexFile(srcName, dstName, idxName string) (err error) {
	src, err := os.OpenFile(srcName, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer src.Close()

	dst, err := os.OpenFile(dstName, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer dst.Close()

	idx, err := os.OpenFile(idxName, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer idx.Close()

	src.Seek(0, 0)
	header := make([]byte, SuperBlockSize)
	if _, error := src.Read(header); error == nil {
		dst.Write(header)
	}

	n, rest, err := ReadNeedle(src)
	if err != nil {
		return err
	}
	nm := NewNeedleMap(idx)
	old_offset := uint32(SuperBlockSize)
	new_offset := uint32(SuperBlockSize)
	for n != nil {
		nv, ok := v.nm.Get(n.Id)
		//log.Println("file size is", n.Size, "rest", rest)
		if !ok || nv.Offset*8 != old_offset {
			log.Println("expected offset should be", nv.Offset*8, "skipping", (rest - 16), "key", n.Id, "volume offset", old_offset, "data_size", n.Size, "rest", rest)
			src.Seek(int64(rest), 1)
		} else {
			if nv.Size > 0 {
				nm.Put(n.Id, new_offset/8, n.Size)
				bytes := make([]byte, n.Size+4)
				src.Read(bytes)
				n.Data = bytes[:n.Size]
				n.Checksum = NewCRC(n.Data)
				n.Append(dst)
				new_offset += rest + 16
				log.Println("saving key", n.Id, "volume offset", old_offset, "=>", new_offset, "data_size", n.Size, "rest", rest)
			}
			src.Seek(int64(rest-n.Size-4), 1)
		}
		old_offset += rest + 16
		if n, rest, err = ReadNeedle(src); err != nil {
			return err
		}
	}

	return nil
}
