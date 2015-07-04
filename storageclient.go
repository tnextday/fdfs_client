package fdfs_client

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path"
	"time"
)

type StorageClient struct {
	IpAddr string
	Port int
	GroupName      string
	StorePathIndex int
}

func (this *StorageClient) UploadByFilename(filename string) (*UploadFileResponse, error) {
	fileInfo, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}

	fileSize := fileInfo.Size()
	fileExtName := path.Ext(filename)

	return this.UploadFile(filename, int64(fileSize), FDFS_UPLOAD_BY_FILENAME,
		STORAGE_PROTO_CMD_UPLOAD_FILE, "", "", fileExtName)
}

func (this *StorageClient) UploadByBuffer(fileBuffer []byte, fileExtName string) (*UploadFileResponse, error) {
	bufferSize := len(fileBuffer)

	return this.UploadFile(fileBuffer, int64(bufferSize), FDFS_UPLOAD_BY_BUFFER,
		STORAGE_PROTO_CMD_UPLOAD_FILE, "", "", fileExtName)
}

func (this *StorageClient) UploadSlaveByFilename(filename string, prefixName string, remoteFileId string) (*UploadFileResponse, error) {
	fileInfo, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}

	fileSize := fileInfo.Size()
	fileExtName := path.Ext(filename)

	return this.UploadFile(filename, int64(fileSize), FDFS_UPLOAD_BY_FILENAME,
		STORAGE_PROTO_CMD_UPLOAD_SLAVE_FILE, remoteFileId, prefixName, fileExtName)
}

func (this *StorageClient) UploadSlaveByBuffer(fileBuffer []byte, remoteFileId string, fileExtName string) (*UploadFileResponse, error) {
	bufferSize := len(fileBuffer)

	return this.UploadFile(fileBuffer, int64(bufferSize), FDFS_UPLOAD_BY_BUFFER,
		STORAGE_PROTO_CMD_UPLOAD_SLAVE_FILE, "", remoteFileId, fileExtName)
}

func (this *StorageClient) UploadAppenderByFilename(filename string) (*UploadFileResponse, error) {
	fileInfo, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}

	fileSize := fileInfo.Size()
	fileExtName := path.Ext(filename)

	return this.UploadFile(filename, int64(fileSize), FDFS_UPLOAD_BY_FILENAME,
		STORAGE_PROTO_CMD_UPLOAD_APPENDER_FILE, "", "", fileExtName)
}

func (this *StorageClient) UploadAppenderByBuffer(fileBuffer []byte, fileExtName string) (*UploadFileResponse, error) {
	bufferSize := len(fileBuffer)

	return this.UploadFile(fileBuffer, int64(bufferSize), FDFS_UPLOAD_BY_BUFFER,
		STORAGE_PROTO_CMD_UPLOAD_APPENDER_FILE, "", "", fileExtName)
}

func (this *StorageClient) UploadFile(fileContent interface{}, fileSize int64, uploadType int,
	cmd int8, masterFilename string, prefixName string, fileExtName string) (*UploadFileResponse, error) {

	var (
		conn        net.Conn
		uploadSlave bool
		headerLen   int64 = 15
		reqBuf      []byte
		err         error
	)

	conn, err = this.makeConn()
	defer conn.Close()
	if err != nil {
		return nil, err
	}

	masterFilenameLen := int64(len(masterFilename))
	if len(this.GroupName) > 0 && len(masterFilename) > 0 {
		uploadSlave = true
		// #slave_fmt |-master_len(8)-file_size(8)-prefix_name(16)-file_ext_name(6)
		//       #           -master_name(master_filename_len)-|
		headerLen = int64(38) + masterFilenameLen
	}

	th := &trackerHeader{}
	th.pkgLen = headerLen
	th.pkgLen += int64(fileSize)
	th.cmd = cmd
	th.sendHeader(conn)

	if uploadSlave {
		req := &uploadSlaveFileRequest{}
		req.masterFilenameLen = masterFilenameLen
		req.fileSize = int64(fileSize)
		req.prefixName = prefixName
		req.fileExtName = fileExtName
		req.masterFilename = masterFilename
		reqBuf, err = req.marshal()
	} else {
		req := &uploadFileRequest{}
		req.storePathIndex = uint8(this.StorePathIndex)
		req.fileSize = int64(fileSize)
		req.fileExtName = fileExtName
		reqBuf, err = req.marshal()
	}
	if err != nil {
		logger.Warnf("uploadFileRequest.marshal error :%s", err.Error())
		return nil, err
	}
	conn.Write(reqBuf)

	switch uploadType {
	case FDFS_UPLOAD_BY_FILENAME:
		if filename, ok := fileContent.(string); ok {
			err = TcpSendFile(conn, filename)
		}
	case FDFS_DOWNLOAD_TO_BUFFER:
		if fileBuffer, ok := fileContent.([]byte); ok {
			_, err =  conn.Write(fileBuffer)
		}
	}
	if err != nil {
		logger.Warnf(err.Error())
		return nil, err
	}

	th.recvHeader(conn)
	if th.status != 0 {
		return nil, Errno{int(th.status)}
	}
	recvBuff, recvSize, err := TcpRecvResponse(conn, th.pkgLen)
	if recvSize <= int64(FDFS_GROUP_NAME_MAX_LEN) {
		errmsg := "[-] Error: Storage response length is not match, "
		errmsg += fmt.Sprintf("expect: %d, actual: %d", th.pkgLen, recvSize)
		logger.Warn(errmsg)
		return nil, errors.New(errmsg)
	}
	ur := &UploadFileResponse{}
	err = ur.unmarshal(recvBuff)
	if err != nil {
		errmsg := fmt.Sprintf("recvBuf can not unmarshal :%s", err.Error())
		logger.Warn(errmsg)
		return nil, errors.New(errmsg)
	}

	return ur, nil
}

func (this *StorageClient) DeleteFile(remoteFilename string) error {
	var (
		conn   net.Conn
		reqBuf []byte
		err    error
	)
	conn, err = this.makeConn()
	defer conn.Close()
	if err != nil {
		return err
	}

	th := &trackerHeader{}
	th.cmd = STORAGE_PROTO_CMD_DELETE_FILE
	fileNameLen := len(remoteFilename)
	th.pkgLen = int64(FDFS_GROUP_NAME_MAX_LEN + fileNameLen)
	th.sendHeader(conn)

	req := &deleteFileRequest{}
	req.groupName = this.GroupName
	req.remoteFilename = remoteFilename
	reqBuf, err = req.marshal()
	if err != nil {
		logger.Warnf("deleteFileRequest.marshal error :%s", err.Error())
		return err
	}
	conn.Write(reqBuf)

	th.recvHeader(conn)
	if th.status != 0 {
		return Errno{int(th.status)}
	}
	return nil
}

func (this *StorageClient) DownloadToFile(localFilename string, offset int64,
	downloadSize int64, remoteFilename string) (*DownloadFileResponse, error) {
	return this.DownloadFile(localFilename, offset, downloadSize, FDFS_DOWNLOAD_TO_FILE, remoteFilename)
}

func (this *StorageClient) DownloadToBuffer(fileBuffer []byte, offset int64,
	downloadSize int64, remoteFilename string) (*DownloadFileResponse, error) {
	return this.DownloadFile(fileBuffer, offset, downloadSize, FDFS_DOWNLOAD_TO_BUFFER, remoteFilename)
}

func (this *StorageClient) DownloadFile(fileContent interface{}, offset int64, downloadSize int64,
	downloadType int, remoteFilename string) (*DownloadFileResponse, error) {

	var (
		conn          net.Conn
		reqBuf        []byte
		localFilename string
		recvBuff      []byte
		recvSize      int64
		err           error
	)

	conn, err = this.makeConn()
	defer conn.Close()
	if err != nil {
		return nil, err
	}

	th := &trackerHeader{}
	th.cmd = STORAGE_PROTO_CMD_DOWNLOAD_FILE
	th.pkgLen = int64(FDFS_PROTO_PKG_LEN_SIZE*2 + FDFS_GROUP_NAME_MAX_LEN + len(remoteFilename))
	th.sendHeader(conn)

	req := &downloadFileRequest{}
	req.offset = offset
	req.downloadSize = downloadSize
	req.groupName = this.GroupName
	req.remoteFilename = remoteFilename
	reqBuf, err = req.marshal()
	if err != nil {
		logger.Warnf("downloadFileRequest.marshal error :%s", err.Error())
		return nil, err
	}
	conn.Write(reqBuf)

	th.recvHeader(conn)
	if th.status != 0 {
		return nil, Errno{int(th.status)}
	}

	switch downloadType {
	case FDFS_DOWNLOAD_TO_FILE:
		if localFilename, ok := fileContent.(string); ok {
			recvSize, err = TcpRecvFile(conn, localFilename, th.pkgLen)
		}
	case FDFS_DOWNLOAD_TO_BUFFER:
		if _, ok := fileContent.([]byte); ok {
			recvBuff, recvSize, err = TcpRecvResponse(conn, th.pkgLen)
		}
	}
	if err != nil {
		logger.Warnf(err.Error())
		return nil, err
	}
	if recvSize < downloadSize {
		errmsg := "[-] Error: Storage response length is not match, "
		errmsg += fmt.Sprintf("expect: %d, actual: %d", th.pkgLen, recvSize)
		logger.Warn(errmsg)
		return nil, errors.New(errmsg)
	}

	dr := &DownloadFileResponse{}
	dr.RemoteFileId = this.GroupName + "/" + remoteFilename
	if downloadType == FDFS_DOWNLOAD_TO_FILE {
		dr.Content = localFilename
	} else {
		dr.Content = recvBuff
	}
	dr.DownloadSize = recvSize
	return dr, nil
}


func (this *StorageClient) makeConn() (net.Conn, error) {
	addr := fmt.Sprintf("%s:%d", this.IpAddr, this.Port)
	return net.DialTimeout("tcp", addr, time.Minute)
}