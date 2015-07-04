package fdfs_client

import (
	"errors"
	"os"

	"github.com/Sirupsen/logrus"
)

var (
	logger = logrus.New()
)

type FdfsClient struct {
	tracker     *Tracker
	trackerPool *ConnectionPool
	timeout     int
}

type Tracker struct {
	HostList []string
	Port     int
}

func init() {
	logger.Formatter = new(logrus.TextFormatter)
}

func NewFdfsClientByTracker(tracker *Tracker) (*FdfsClient, error) {
	trackerPool, err := NewConnectionPool(tracker.HostList, tracker.Port, 10, 150)
	if err != nil {
		return nil, err
	}

	return &FdfsClient{tracker: tracker, trackerPool: trackerPool}, nil
}

func (this *FdfsClient) UploadByFilename(filename string) (*UploadFileResponse, error) {
	if _, err := os.Stat(filename); err != nil {
		return nil, errors.New(err.Error() + "(uploading)")
	}

	tc := &TrackerClient{this.trackerPool}
	store, err := tc.QueryStorageStoreWithoutGroup()
	if err != nil {
		return nil, err
	}

	return store.UploadByFilename(filename)
}

func (this *FdfsClient) UploadByBuffer(fileBuffer []byte, fileExtName string) (*UploadFileResponse, error) {
	tc := &TrackerClient{this.trackerPool}
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

	tc := &TrackerClient{this.trackerPool}
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

	tc := &TrackerClient{this.trackerPool}
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

	tc := &TrackerClient{this.trackerPool}
	store, err := tc.QueryStorageStoreWithoutGroup()
	if err != nil {
		return nil, err
	}

	return store.UploadAppenderByFilename(filename)
}

func (this *FdfsClient) UploadAppenderByBuffer(fileBuffer []byte, fileExtName string) (*UploadFileResponse, error) {
	tc := &TrackerClient{this.trackerPool}
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
	groupName := tmp[0]
	remoteFilename := tmp[1]

	tc := &TrackerClient{this.trackerPool}
	store, err := tc.QueryStorageUpdate(groupName, remoteFilename)
	if err != nil {
		return err
	}

	return store.DeleteFile(remoteFilename)
}

func (this *FdfsClient) DownloadToFile(localFilename string, remoteFileId string, offset int64, downloadSize int64) (*DownloadFileResponse, error) {
	tmp, err := splitRemoteFileId(remoteFileId)
	if err != nil || len(tmp) != 2 {
		return nil, err
	}
	groupName := tmp[0]
	remoteFilename := tmp[1]

	tc := &TrackerClient{this.trackerPool}
	store, err := tc.QueryStorageFetch(groupName, remoteFilename)
	if err != nil {
		return nil, err
	}

	return store.DownloadToFile(localFilename, offset, downloadSize, remoteFilename)
}

func (this *FdfsClient) DownloadToBuffer(remoteFileId string, offset int64, downloadSize int64) (*DownloadFileResponse, error) {
	tmp, err := splitRemoteFileId(remoteFileId)
	if err != nil || len(tmp) != 2 {
		return nil, err
	}
	groupName := tmp[0]
	remoteFilename := tmp[1]

	tc := &TrackerClient{this.trackerPool}
	store, err := tc.QueryStorageFetch(groupName, remoteFilename)
	if err != nil {
		return nil, err
	}

	var fileBuffer []byte
	return store.DownloadToBuffer(fileBuffer, offset, downloadSize, remoteFilename)
}
