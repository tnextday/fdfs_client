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
	uploadResponse, err := fdfsClient.UploadByFilename("README.md")
	if err != nil {
		t.Errorf("UploadByfilename error %s", err.Error())
	}
	t.Log(uploadResponse.GroupName)
	t.Log(uploadResponse.RemoteFileId)
	fdfsClient.DeleteFile(uploadResponse.RemoteFileId)
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

	uploadResponse, err := fdfsClient.UploadByBuffer(fileBuffer, "txt")
	if err != nil {
		t.Errorf("TestUploadByBuffer error %s", err.Error())
	}

	t.Log(uploadResponse.GroupName)
	t.Log(uploadResponse.RemoteFileId)
	fdfsClient.DeleteFile(uploadResponse.RemoteFileId)
}

func TestUploadSlaveByFilename(t *testing.T) {
	fdfsClient := FdfsClient{ConnPool: connPool}

	uploadResponse, err := fdfsClient.UploadByFilename("README.md")
	if err != nil {
		t.Errorf("UploadByfilename error %s", err.Error())
	}
	t.Log(uploadResponse.GroupName)
	t.Log(uploadResponse.RemoteFileId)

	masterFileId := uploadResponse.RemoteFileId
	uploadResponse, err = fdfsClient.UploadSlaveByFilename("testfile", masterFileId, "_test")
	if err != nil {
		t.Errorf("UploadByfilename error %s", err.Error())
	}
	t.Log(uploadResponse.GroupName)
	t.Log(uploadResponse.RemoteFileId)

	fdfsClient.DeleteFile(masterFileId)
	fdfsClient.DeleteFile(uploadResponse.RemoteFileId)
}

func TestDownloadToFile(t *testing.T) {
	fdfsClient := FdfsClient{ConnPool: connPool}

	uploadResponse, err := fdfsClient.UploadByFilename("README.md")
	defer fdfsClient.DeleteFile(uploadResponse.RemoteFileId)
	if err != nil {
		t.Errorf("UploadByfilename error %s", err.Error())
	}
	t.Log(uploadResponse.GroupName)
	t.Log(uploadResponse.RemoteFileId)

	var (
		downloadResponse *DownloadFileResponse
		localFilename    string = "download.txt"
	)
	downloadResponse, err = fdfsClient.DownloadToFile(uploadResponse.RemoteFileId, localFilename, 0, 0)
	if err != nil {
		t.Errorf("DownloadToFile error %s", err.Error())
	}
	t.Log(downloadResponse.DownloadSize)
	t.Log(downloadResponse.RemoteFileId)
	os.Remove(localFilename)
}

func TestDownloadToBuffer(t *testing.T) {
	fdfsClient := FdfsClient{ConnPool: connPool}
	uploadResponse, err := fdfsClient.UploadByFilename("README.md")
	defer fdfsClient.DeleteFile(uploadResponse.RemoteFileId)
	if err != nil {
		t.Errorf("UploadByfilename error %s", err.Error())
	}
	t.Log(uploadResponse.GroupName)
	t.Log(uploadResponse.RemoteFileId)

	var (
		downloadResponse *DownloadFileResponse
	)
	downloadResponse, err = fdfsClient.DownloadToBuffer(uploadResponse.RemoteFileId, 0, 0)
	if err != nil {
		t.Errorf("DownloadToBuffer error %s", err.Error())
	}
	t.Log(downloadResponse.DownloadSize)
	t.Log(downloadResponse.RemoteFileId)
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
		uploadResponse, err := fdfsClient.UploadByBuffer(fileBuffer, "txt")
		if err != nil {
			fmt.Errorf("TestUploadByBuffer error %s", err.Error())
		}

		fdfsClient.DeleteFile(uploadResponse.RemoteFileId)
	}
}

func BenchmarkUploadByFilename(b *testing.B) {
	fdfsClient := FdfsClient{ConnPool: connPool}

	b.StopTimer()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		uploadResponse, err := fdfsClient.UploadByFilename("README.md")
		if err != nil {
			fmt.Errorf("UploadByfilename error %s", err.Error())
		}
		err = fdfsClient.DeleteFile(uploadResponse.RemoteFileId)
		if err != nil {
			fmt.Errorf("DeleteFile error %s", err.Error())
		}
	}
}

func BenchmarkDownloadToFile(b *testing.B) {
	fdfsClient := FdfsClient{ConnPool: connPool}

	uploadResponse, err := fdfsClient.UploadByFilename("README.md")
	defer fdfsClient.DeleteFile(uploadResponse.RemoteFileId)
	if err != nil {
		fmt.Errorf("UploadByfilename error %s", err.Error())
	}
	b.StopTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		var (
			localFilename string = "download.txt"
		)
		_, err = fdfsClient.DownloadToFile(uploadResponse.RemoteFileId, localFilename, 0, 0)
		if err != nil {
			fmt.Errorf("DownloadToFile error %s", err.Error())
		}
		os.Remove(localFilename)
		// fmt.Println(downloadResponse.RemoteFileId)
	}
}
