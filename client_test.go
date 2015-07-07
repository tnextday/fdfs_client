package fdfs_client

import (
	"fmt"
	"os"
	"testing"
	"io"
	"encoding/hex"
	"bytes"
	"math/rand"
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

type MemTestData struct {
	FillBytes 	[]byte
	Size 		int64
	readOffset  int64
	writeOffset int64
}

func(td *MemTestData) Read(p []byte) (n int, err error){
	if td.readOffset >= td.Size{
		return 0, io.EOF
	}
	err = nil
	n = len(p)
	if (td.Size - td.readOffset) < int64(n){
		n = int(td.Size - td.readOffset)
	}

	fbSz := int64(len(td.FillBytes))
	o1 := td.readOffset % fbSz
	l1 := fbSz - o1
	rs := (int64(n) - l1) / fbSz
	l2 := int64(n) - rs * fbSz - l1
//	fmt.Printf("ro=%d,n=%d,fbSz=%d,o1=%d,rs=%d,l2=%d\n", td.readOffset,n,fbSz, o1,rs,l2)
	i := int64(0)
	if o1 > 0 {
		copy(p[i:], td.FillBytes[o1:])
		i += (fbSz - o1)
	}
	for j := int64(0); j < rs; j++{
		copy(p[i:], td.FillBytes)
		i += fbSz
	}
	if (l2 > 0){
		copy(p[i:], td.FillBytes[:l2])
//		fmt.Printf("read last %d bytes\n%s", l2, dumpPrefixBytes(p[i:], int(l2)))
	}
	td.readOffset += int64(n)
	return
}

func dumpPrefixBytes(bs []byte, max int) string {
	if len(bs) > max {
		return hex.Dump(bs[:max])
	}else{
		return hex.Dump(bs)
	}
}

func(td *MemTestData) Write(p []byte) (n int, err error){
	if td.writeOffset >= td.Size{
		return 0, io.EOF
	}
	err = nil
	n = len(p)
	if (td.Size - td.writeOffset) < int64(n){
		n = int(td.Size - td.writeOffset)
	}
	fbSz := int64(len(td.FillBytes))
	o1 := td.writeOffset % fbSz
	l1 := fbSz - o1
	rs := (int64(n) - l1) / fbSz
	l2 := int64(n) - rs * fbSz - l1
//	fmt.Printf("wo=%d,n=%d,fbSz=%d,o1=%d,rs=%d,l2=%d\n", td.writeOffset,n,fbSz, o1,rs,l2)
	i := 0
	if o1 > 0{
		if !bytes.HasPrefix(p[i:], td.FillBytes[o1:]) {
			return i, fmt.Errorf("Date chek error0, offset %d\n%s", td.writeOffset, dumpPrefixBytes(p[i:], 32))
		}
		i += int(fbSz - o1)
		td.writeOffset += (fbSz - o1)
	}
	for j := int64(0); j < rs; j++{
		if !bytes.HasPrefix(p[i:], td.FillBytes) {
			return i, fmt.Errorf("Date chek error1, offset %d\n%s", td.writeOffset, dumpPrefixBytes(p[i:], 32))
		}
		i += int(fbSz)
		td.writeOffset += fbSz
	}
	if l2 > 0 && !bytes.HasPrefix(p[i:], td.FillBytes[:l2]) {
		return i, fmt.Errorf("Date chek err2, offset %d\n%s", td.writeOffset, dumpPrefixBytes(p[i:], 32))
	}
	td.writeOffset += l2
	return
}

func(td *MemTestData) Reset(){
	td.readOffset = 0
	td.writeOffset = 0
}

func TestMemData(t *testing.T) {
	bs := []byte("1234567890abcdef")
	rand.Seed(time.Now().Unix())
	for i := 0; i < 10; i++{
		fb := bs[:1 + rand.Int31n(16)]
		sz := 1+rand.Int63n(0xFFFF)
		t.Logf("fbSize=%d, fileSize=%d\n", len(fb),sz)
		mtd := MemTestData{FillBytes:fb, Size:sz}
		n, e := io.Copy(&mtd, &mtd)
		if e != nil{
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

func BenchmarkUploadByBuffer(b *testing.B) {
	fdfsClient := FdfsClient{ConnPool: connPool}
	file, err := os.Open("testfile") // For read access.
	if err != nil {
		fmt.Errorf("%s", err.Error())
	}

	var fileSize int64 = 0
	if fileInfo, err := file.Stat(); err == nil {
		fileSize = fileInfo.Size()
	}
	fileBuffer := make([]byte, fileSize)
	_, err = file.Read(fileBuffer)
	if err != nil {
		fmt.Errorf("%s", err.Error())
	}

	b.StopTimer()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		remoteFileId, err := fdfsClient.UploadByBuffer(fileBuffer, "txt")
		if err != nil {
			fmt.Errorf("TestUploadByBuffer error %s", err.Error())
		}

		fdfsClient.DeleteFile(remoteFileId)
	}
}

func BenchmarkUploadByFilename(b *testing.B) {
	fdfsClient := FdfsClient{ConnPool: connPool}

	b.StopTimer()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		remoteFileId, err := fdfsClient.UploadByFilename("README.md")
		if err != nil {
			fmt.Errorf("UploadByfilename error %s", err.Error())
		}
		err = fdfsClient.DeleteFile(remoteFileId)
		if err != nil {
			fmt.Errorf("DeleteFile error %s", err.Error())
		}
	}
}

func BenchmarkDownloadToFile(b *testing.B) {
	fdfsClient := FdfsClient{ConnPool: connPool}

	remoteFileId, err := fdfsClient.UploadByFilename("README.md")
	defer fdfsClient.DeleteFile(remoteFileId)
	if err != nil {
		fmt.Errorf("UploadByfilename error %s", err.Error())
	}
	b.StopTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		var (
			localFilename string = "download.txt"
		)
		_, err = fdfsClient.DownloadToFile(remoteFileId, localFilename)
		if err != nil {
			fmt.Errorf("DownloadToFile error %s", err.Error())
		}
		os.Remove(localFilename)
		// fmt.Println(downloadResponse.RemoteFileId)
	}
}
