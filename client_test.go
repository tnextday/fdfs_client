package fdfs_client

import (
	"fmt"
	"os"
	"testing"
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
	t.Log(slaveFileId)

	fdfsClient.DeleteFile(masterFileId)
	fdfsClient.DeleteFile(slaveFileId)
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
