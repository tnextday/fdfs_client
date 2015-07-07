package fdfs_client

import (
	"errors"
	"io"
	"os"
)

type FdfsClient struct {
	ConnPool *ConnectionPool
	//	timeout  int
}

func (this *FdfsClient) UploadByFilename(filename string) (remoteFileId string, e error) {
	if _, err := os.Stat(filename); err != nil {
		return "", errors.New(err.Error() + "(uploading)")
	}

	tc := TrackerClient{this.ConnPool}
	store, err := tc.QueryStorageStoreWithoutGroup()
	if err != nil {
		return "", err
	}
	fid, e := store.UploadByFilename(filename)
	if e != nil {
		return "", e
	}
	return fid.GetFileIdStr(), nil
}

func (this *FdfsClient) UploadByBuffer(fileBuffer []byte, fileExtName string) (remoteFileId string, e error) {
	tc := TrackerClient{this.ConnPool}
	store, err := tc.QueryStorageStoreWithoutGroup()
	if err != nil {
		return "", err
	}
	fid, e := store.UploadByBuffer(fileBuffer, fileExtName)
	if e != nil {
		return "", e
	}
	return fid.GetFileIdStr(), nil
}

func (this *FdfsClient) UploadByReader(reader io.Reader, size int64, fileExtName string) (remoteFileId string, e error) {
	tc := TrackerClient{this.ConnPool}
	store, err := tc.QueryStorageStoreWithoutGroup()
	if err != nil {
		return "", err
	}
	fid, e := store.UploadByReader(reader, size, fileExtName)
	if e != nil {
		return "", e
	}
	return fid.GetFileIdStr(), nil
}

func (this *FdfsClient) UploadSlaveByFilename(filename, masterFileId, prefixName string) (remoteFileId string, e error) {
	if _, err := os.Stat(filename); err != nil {
		return "", errors.New(err.Error() + "(uploading)")
	}

	masterFid, err := NewFileIdFromStr(masterFileId)
	if err != nil {
		return "", err
	}

	tc := TrackerClient{this.ConnPool}
	store, err := tc.QueryStorageStoreWithGroup(masterFid.GroupName)
	if err != nil {
		return "", err
	}
	fid, e := store.UploadSlaveByFilename(filename, prefixName, masterFid.FileName)
	if e != nil {
		return "", e
	}
	return fid.GetFileIdStr(), nil
}

func (this *FdfsClient) UploadSlaveByBuffer(fileBuffer []byte, masterFileId, fileExtName string) (remoteFileId string, e error) {
	masterFid, err := NewFileIdFromStr(masterFileId)
	if err != nil {
		return "", err
	}

	tc := TrackerClient{this.ConnPool}
	store, err := tc.QueryStorageStoreWithGroup(masterFid.GroupName)
	if err != nil {
		return "", err
	}

	fid, e := store.UploadSlaveByBuffer(fileBuffer, masterFid.FileName, fileExtName)
	if e != nil {
		return "", e
	}
	return fid.GetFileIdStr(), nil
}

//func (this *FdfsClient) UploadAppenderByFilename(filename string) (string, error) {
//	if _, err := os.Stat(filename); err != nil {
//		return nil, errors.New(err.Error() + "(uploading)")
//	}
//
//	tc := TrackerClient{this.ConnPool}
//	store, err := tc.QueryStorageStoreWithoutGroup()
//	if err != nil {
//		return nil, err
//	}
//
//	return store.UploadAppenderByFilename(filename)
//}

//func (this *FdfsClient) UploadAppenderByBuffer(fileBuffer []byte, fileExtName string) (*FileId, error) {
//	tc := TrackerClient{this.ConnPool}
//	store, err := tc.QueryStorageStoreWithoutGroup()
//	if err != nil {
//		return nil, err
//	}
//
//	return store.UploadAppenderByBuffer(fileBuffer, fileExtName)
//}

func (this *FdfsClient) DeleteFile(remoteFileId string) error {
	fid, err := NewFileIdFromStr(remoteFileId)
	if err != nil {
		return err
	}

	tc := TrackerClient{this.ConnPool}
	store, err := tc.QueryStorageUpdate(fid)
	if err != nil {
		return err
	}

	return store.DeleteFile(fid.FileName)
}

func (this *FdfsClient) DownloadToFile(remoteFileId string, localFilename string) (size int64, e error) {
	file, err := os.Create(localFilename)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	return this.DownloadEx(remoteFileId, file, 0, 0)
}

func (this *FdfsClient) DownloadEx(remoteFileId string, output io.Writer, offset int64, downloadSize int64) (size int64, e error) {
	fid, err := NewFileIdFromStr(remoteFileId)
	if err != nil {
		return 0, err
	}
	tc := TrackerClient{this.ConnPool}
	store, err := tc.QueryStorageFetch(fid)
	if err != nil {
		return 0, err
	}

	return store.DownloadEx(fid.FileName, output, offset, downloadSize)
}
