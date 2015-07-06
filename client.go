package fdfs_client

import (
	"errors"
	"os"

	"io"
)


type FdfsClient struct {
	ConnPool *ConnectionPool
//	timeout  int
}

func (this *FdfsClient) UploadByFilename(filename string) (*UploadFileResponse, error) {
	if _, err := os.Stat(filename); err != nil {
		return nil, errors.New(err.Error() + "(uploading)")
	}

	tc := TrackerClient{this.ConnPool}
	store, err := tc.QueryStorageStoreWithoutGroup()
	if err != nil {
		return nil, err
	}

	return store.UploadByFilename(filename)
}

func (this *FdfsClient) UploadByBuffer(fileBuffer []byte, fileExtName string) (*UploadFileResponse, error) {
	tc := TrackerClient{this.ConnPool}
	store, err := tc.QueryStorageStoreWithoutGroup()
	if err != nil {
		return nil, err
	}

	return store.UploadByBuffer(fileBuffer, fileExtName)
}

func (this *FdfsClient) UploadSlaveByFilename(filename, remoteFileId, prefixName string) (*UploadFileResponse, error) {
	if _, err := os.Stat(filename); err != nil {
		return nil, errors.New(err.Error() + "(uploading)")
	}

	tmp, err := splitRemoteFileId(remoteFileId)
	if err != nil || len(tmp) != 2 {
		return nil, err
	}
	groupName := tmp[0]
	remoteFilename := tmp[1]

	tc := TrackerClient{this.ConnPool}
	store, err := tc.QueryStorageStoreWithGroup(groupName)
	if err != nil {
		return nil, err
	}

	return store.UploadSlaveByFilename(filename, prefixName, remoteFilename)
}

func (this *FdfsClient) UploadSlaveByBuffer(fileBuffer []byte, remoteFileId, fileExtName string) (*UploadFileResponse, error) {
	tmp, err := splitRemoteFileId(remoteFileId)
	if err != nil || len(tmp) != 2 {
		return nil, err
	}
	groupName := tmp[0]
	remoteFilename := tmp[1]

	tc := TrackerClient{this.ConnPool}
	store, err := tc.QueryStorageStoreWithGroup(groupName)
	if err != nil {
		return nil, err
	}

	return store.UploadSlaveByBuffer(fileBuffer, remoteFilename, fileExtName)
}

func (this *FdfsClient) UploadAppenderByFilename(filename string) (*UploadFileResponse, error) {
	if _, err := os.Stat(filename); err != nil {
		return nil, errors.New(err.Error() + "(uploading)")
	}

	tc := TrackerClient{this.ConnPool}
	store, err := tc.QueryStorageStoreWithoutGroup()
	if err != nil {
		return nil, err
	}

	return store.UploadAppenderByFilename(filename)
}

func (this *FdfsClient) UploadAppenderByBuffer(fileBuffer []byte, fileExtName string) (*UploadFileResponse, error) {
	tc := TrackerClient{this.ConnPool}
	store, err := tc.QueryStorageStoreWithoutGroup()
	if err != nil {
		return nil, err
	}

	return store.UploadAppenderByBuffer(fileBuffer, fileExtName)
}

func (this *FdfsClient) DeleteFile(remoteFileId string) error {
	tmp, err := splitRemoteFileId(remoteFileId)
	if err != nil || len(tmp) != 2 {
		return err
	}
	remoteFilename := tmp[1]

	tc := TrackerClient{this.ConnPool}
	store, err := tc.QueryStorageUpdate(remoteFileId)
	if err != nil {
		return err
	}

	return store.DeleteFile(remoteFilename)
}

func (this *FdfsClient) DownloadToFile(remoteFileId string, localFilename string) (size int64, e error) {
	file, err := os.Create(localFilename)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	return this.DownloadEx(remoteFileId, file, 0, 0);
}

func (this *FdfsClient) DownloadEx(remoteFileId string, output io.Writer, offset int64, downloadSize int64) (size int64, e error) {
	tc := TrackerClient{this.ConnPool}
	store, err := tc.QueryStorageFetch(remoteFileId)
	if err != nil {
		return 0, err
	}
	return store.DownloadEx(remoteFileId, output, offset, downloadSize)
}