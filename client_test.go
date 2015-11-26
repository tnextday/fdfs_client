package fdfs_client

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"os"
	"testing"
	"time"
)

var (
	trackers    = []string{"192.168.199.2"}
	trackerPort = 22122
	connPool    *ConnectionPool
)

func init() {
	var e error
	connPool, e = NewConnectionPool(
		trackers,
		trackerPort,
		10,
		150,
	)
	if e != nil {
		fmt.Println("NewConnectionPool error:", e)
		return
	}
}

type MemData struct {
	FillBytes   []byte
	Size        int64
	readOffset  int64
	writeOffset int64
}

func (td *MemData) Read(p []byte) (n int, err error) {
	if td.readOffset >= td.Size {
		fmt.Printf("MemData read offset %d, size %d\n", td.readOffset, td.Size)
		return 0, io.EOF
	}
	err = nil
	n = len(p)
	if (td.Size - td.readOffset) < int64(n) {
		n = int(td.Size - td.readOffset)
	}

	fbSz := int64(len(td.FillBytes))
	o1 := td.readOffset % fbSz
	l1 := int64(0)
	if o1 > 0 {
		l1 = fbSz - o1
	}
	rs := (int64(n) - l1) / fbSz
	l2 := int64(n) - rs*fbSz - l1
	//	fmt.Printf("ro=%d,n=%d,fbSz=%d,o1=%d,rs=%d,l2=%d\n", td.readOffset,n,fbSz, o1,rs,l2)
	i := int64(0)
	if o1 > 0 {
		copy(p[i:], td.FillBytes[o1:])
		i += (fbSz - o1)
	}
	for j := int64(0); j < rs; j++ {
		copy(p[i:], td.FillBytes)
		i += fbSz
	}
	if l2 > 0 {
		copy(p[i:], td.FillBytes[:l2])
		//		fmt.Printf("read last %d bytes\n%s", l2, dumpPrefixBytes(p[i:], int(l2)))
	}
	td.readOffset += int64(n)
	return
}

func dumpPrefixBytes(bs []byte, max int) string {
	if len(bs) > max {
		return hex.Dump(bs[:max])
	} else {
		return hex.Dump(bs)
	}
}

func (td *MemData) Write(p []byte) (n int, err error) {
	if td.writeOffset >= td.Size {
		fmt.Printf("MemData write offset %d, size %d\n", td.writeOffset, td.Size)
		return 0, io.EOF
	}
	err = nil
	n = len(p)
	if (td.Size - td.writeOffset) < int64(n) {
		n = int(td.Size - td.writeOffset)
	}
	fbSz := int64(len(td.FillBytes))
	o1 := td.writeOffset % fbSz
	l1 := int64(0)
	if o1 > 0 {
		l1 = fbSz - o1
	}
	rs := (int64(n) - l1) / fbSz
	l2 := int64(n) - rs*fbSz - l1
	//	fmt.Printf("wo=%d,n=%d,fbSz=%d,o1=%d,rs=%d,l2=%d\n", td.writeOffset,n,fbSz, o1,rs,l2)
	i := 0
	if o1 > 0 {
		if len(p[i:]) > len(td.FillBytes[o1:]) {
			if !bytes.HasPrefix(p[i:], td.FillBytes[o1:]) {
				return i, fmt.Errorf("Date check error0, offset %d\n%s", td.writeOffset, dumpPrefixBytes(p[i:], 32))
			}
		} else {
			if !bytes.HasPrefix(td.FillBytes[o1:], p[i:]) {
				return i, fmt.Errorf("Date check error0, offset %d\n%s", td.writeOffset, dumpPrefixBytes(p[i:], 32))
			}
		}
		i += int(fbSz - o1)
		td.writeOffset += (fbSz - o1)
	}
	for j := int64(0); j < rs; j++ {
		if !bytes.HasPrefix(p[i:], td.FillBytes) {
			return i, fmt.Errorf("Date check error1, offset %d\n%s", td.writeOffset, dumpPrefixBytes(p[i:], 32))
		}
		i += int(fbSz)
		td.writeOffset += fbSz
	}
	if l2 > 0 && !bytes.HasPrefix(p[i:], td.FillBytes[:l2]) {
		return i, fmt.Errorf("Date check err2, offset %d\n%s", td.writeOffset, dumpPrefixBytes(p[i:], 32))
	}
	td.writeOffset += l2
	return
}

func (td *MemData) ResetAll() {
	td.readOffset = 0
	td.writeOffset = 0
}

func (td *MemData) ResetRead() {
	td.readOffset = 0
}

func (td *MemData) ResetWrite() {
	td.writeOffset = 0
}

func TestMemData(t *testing.T) {
	bs := []byte("1234567890abcdef")
	rand.Seed(time.Now().Unix())
	for i := 0; i < 1; i++ {
		fb := bs[:1+rand.Int31n(16)]
		sz := 1 + rand.Int63n(0xFFFF)
		t.Logf("fbSize=%d, fileSize=%d\n", len(fb), sz)
		mtd := MemData{FillBytes: fb, Size: sz}
		n, e := io.Copy(&mtd, &mtd)
		if e != nil {
			t.Fatal(e)
		}
		if n != sz {
			t.Fatalf("size wrong, %d != %d\n", n, sz)
		}
	}
}

func TestUploadByFilename(t *testing.T) {
	fdfsClient := FdfsClient{ConnPool: connPool}
	remoteFileId, err := fdfsClient.UploadByFilename("README.md")
	if err != nil {
		t.Fatal("UploadByfilename error:", err.Error())
	}
	t.Log(remoteFileId)
	fdfsClient.DeleteFile(remoteFileId)
}

func TestUploadByBuffer(t *testing.T) {
	fdfsClient := FdfsClient{ConnPool: connPool}
	file, err := os.Open("testfile") // For read access.
	if err != nil {
		t.Fatal(err)
	}

	var fileSize int64 = 0
	if fileInfo, err := file.Stat(); err == nil {
		fileSize = fileInfo.Size()
	}
	fileBuffer := make([]byte, fileSize)
	_, err = file.Read(fileBuffer)
	if err != nil {
		t.Fatal(err)
	}

	remoteFileId, err := fdfsClient.UploadByBuffer(fileBuffer, "txt")
	if err != nil {
		t.Fatal("TestUploadByBuffer error:", err.Error())
	}

	t.Log(remoteFileId)
	fdfsClient.DeleteFile(remoteFileId)
}

func TestUploadSlaveByFilename(t *testing.T) {
	fdfsClient := FdfsClient{ConnPool: connPool}

	masterFileId, err := fdfsClient.UploadByFilename("README.md")
	if err != nil {
		t.Fatal("UploadByfilename error:", err.Error())
	}
	t.Log("masterFileId: ", masterFileId)

	slaveFileId, err := fdfsClient.UploadSlaveByFilename("testfile", masterFileId, "_test")
	if err != nil {
		t.Fatal("UploadSlaveByFilename error:", err.Error())
	}
	t.Log("slaveFileId: ", slaveFileId)

	err = fdfsClient.DeleteFile(masterFileId)
	if err != nil {
		t.Fatal("DeleteFile master error:", err.Error())
	}
	err = fdfsClient.DeleteFile(slaveFileId)
	if err != nil {
		t.Fatal("DeleteFile slave error:", err.Error())
	}
}

func TestDownloadToFile(t *testing.T) {
	fdfsClient := FdfsClient{ConnPool: connPool}

	remoteFileId, err := fdfsClient.UploadByFilename("README.md")
	if err != nil {
		t.Fatal("UploadByfilename error:", err.Error())
	}
	t.Log("Upload to:", remoteFileId)
	defer func() {
		e := fdfsClient.DeleteFile(remoteFileId)
		if e != nil {
			t.Fatal("DeleteFile error:", e.Error())
		}
	}()

	var (
		localFilename string = "download.txt"
	)
	sz, err := fdfsClient.DownloadToFile(remoteFileId, localFilename)
	if err != nil {
		t.Fatal("DownloadToFile error:", err.Error())
	}
	t.Log("DownloadToFile size: ", sz)
	os.Remove(localFilename)
}

func formatSize(sz int64) string {
	if sz < 1024*1024 {
		return fmt.Sprintf("%.2f Kb", float64(sz)/1024.0)
	} else if sz < 1024*1024*1024 {
		return fmt.Sprintf("%.2f Mb", float64(sz)/(1024*1024.0))
	} else if sz < 1024*1024*1024*1024 {
		return fmt.Sprintf("%.2f Gb", float64(sz)/(1024*1024*1024.0))
	} else {
		return fmt.Sprintf("%.2f Tb", float64(sz)/(1024*1024*1024*1024.0))
	}
}

func TestFileContent(t *testing.T) {
	fdfsClient := FdfsClient{ConnPool: connPool}
	bs := []byte("1234567890abcdef")
	rand.Seed(time.Now().Unix())
	for i := 0; i < 5; i++ {
		sz := 1 + rand.Int63n(0xFFFFF)
		mtd := MemData{FillBytes: bs, Size: sz}

		remoteFileId, e := fdfsClient.UploadByReader(&mtd, mtd.Size, "bin")
		t.Logf("Upload fileSize=%s(%d),\tid=%s\n", formatSize(sz), sz, remoteFileId)
		if e != nil {
			t.Fatal(e)
		}
//		remoteFileId := "apk1/M00/00/08/wKjHAlWbqeSARKSMoa4vGf2SnsM024.bin"
		sz, e = fdfsClient.DownloadEx(remoteFileId, &mtd, 0, 0)
		fdfsClient.DeleteFile(remoteFileId)
		if e != nil {
			t.Fatalf("download %s, err: %s", remoteFileId, e.Error())
		}
		if sz != mtd.Size {
			t.Fatalf("download %s, size error: %d", remoteFileId, sz)
		}
	}
}

func BenchmarkUpload(b *testing.B) {
	fdfsClient := FdfsClient{ConnPool: connPool}

	b.ResetTimer()

	mtd := MemData{FillBytes: []byte("1234567890abcdef"), Size: 1024 * 1024}
	for i := 0; i < b.N; i++ {
		mtd.ResetRead()
		remoteFileId, err := fdfsClient.UploadByReader(&mtd, mtd.Size, "png")
		if err != nil {
			fmt.Errorf("UploadByfilename error %s", err.Error())
		}
		err = fdfsClient.DeleteFile(remoteFileId)
		if err != nil {
			fmt.Errorf("DeleteFile error %s", err.Error())
		}
	}
}

func BenchmarkDownload(b *testing.B) {
	fdfsClient := FdfsClient{ConnPool: connPool}
	mtd := MemData{FillBytes: []byte("1234567890abcdef"), Size: 1024 * 1024}
	remoteFileId, err := fdfsClient.UploadByReader(&mtd, mtd.Size, "png")
	if err != nil {
		fmt.Errorf("UploadByReader error %s", err.Error())
	}
	defer fdfsClient.DeleteFile(remoteFileId)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		mtd.ResetWrite()
		_, err = fdfsClient.DownloadEx(remoteFileId, &mtd, 0, 0)
		if err != nil {
			fmt.Errorf("DownloadToFile error %s", err.Error())
		}
	}
}

// go test -run=none -bench=BenchmarkQueryStorageFetch -cpuprofile=cprof
// go tool pprof --text fdfs_client.test cprof
func BenchmarkQueryStorageFetch(b *testing.B) {
	fdfsClient := FdfsClient{ConnPool: connPool}
	mtd := MemData{FillBytes: []byte("1234567890abcdef"), Size: 1024 * 1024}
	remoteFileId, err := fdfsClient.UploadByReader(&mtd, mtd.Size, "png")
	if err != nil {
		fmt.Errorf("UploadByReader error %s", err.Error())
	}
	defer fdfsClient.DeleteFile(remoteFileId)
	fid, _ := NewFileIdFromStr(remoteFileId)
	b.ResetTimer()
	tc := TrackerClient{connPool}
	for i := 0; i < b.N; i++ {
		store, e := tc.QueryStorageFetch(fid)
		if e != nil || store.IpAddr == "" {
			b.Fatal("QueryStorageFetch error, ", e)
		}
	}
}
