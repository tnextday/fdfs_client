package fdfs_client

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"time"
)

type StorageClient struct {
	IpAddr         string
	Port           int
	GroupName      string
	StorePathIndex int
}

func (this *StorageClient) UploadByFilename(filename string) (*FileId, error) {
	fileInfo, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}

	fileSize := fileInfo.Size()
	fileExtName := getFileExt(filename)
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return this.UploadEx(file, fileSize,
		STORAGE_PROTO_CMD_UPLOAD_FILE, "", "", fileExtName)
}

func (this *StorageClient) UploadByBuffer(buf []byte, fileExtName string) (*FileId, error) {
	bufferSize := len(buf)
	bb := bytes.NewReader(buf)
	return this.UploadEx(bb, int64(bufferSize),
		STORAGE_PROTO_CMD_UPLOAD_FILE, "", "", fileExtName)
}

func (this *StorageClient) UploadByReader(reader io.Reader, size int64, fileExtName string) (*FileId, error) {
	return this.UploadEx(reader, size,
		STORAGE_PROTO_CMD_UPLOAD_FILE, "", "", fileExtName)
}

func (this *StorageClient) UploadSlaveByFilename(filename string, prefixName string, masterFileId string) (*FileId, error) {
	fileInfo, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}

	fileSize := fileInfo.Size()
	fileExtName := getFileExt(filename)
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return this.UploadEx(file, fileSize,
		STORAGE_PROTO_CMD_UPLOAD_SLAVE_FILE, masterFileId, prefixName, fileExtName)
}

func (this *StorageClient) UploadSlaveByBuffer(buf []byte, remoteFileId string, fileExtName string) (*FileId, error) {
	bufferSize := len(buf)
	bb := bytes.NewReader(buf)
	return this.UploadEx(bb, int64(bufferSize),
		STORAGE_PROTO_CMD_UPLOAD_SLAVE_FILE, "", remoteFileId, fileExtName)
}

//func (this *StorageClient) UploadAppenderByFilename(filename string, appenderFileId string) (*FileId, error) {
//	fileInfo, err := os.Stat(filename)
//	if err != nil {
//		return nil, err
//	}
//
//	fileSize := fileInfo.Size()
//	fileExtName := getFileExt(filename)
//	file, err := os.Open(filename)
//	if err != nil {
//		return nil, err
//	}
//	defer file.Close()
//
//	return this.UploadEx(file, fileSize,
//		STORAGE_PROTO_CMD_UPLOAD_APPENDER_FILE, "", "", fileExtName)
//}
//
//func (this *StorageClient) UploadAppenderByBuffer(buf []byte, fileExtName string) (*FileId, error) {
//	bufferSize := len(buf)
//	bb := bytes.NewReader(buf)
//	return this.UploadEx(bb, int64(bufferSize), STORAGE_PROTO_CMD_UPLOAD_APPENDER_FILE, "", "", fileExtName)
//}

func (this *StorageClient) UploadEx(input io.Reader, size int64,
	cmd int8, masterFilename string, prefixName string, fileExtName string) (*FileId, error) {

	var (
		conn        net.Conn
		uploadSlave bool
		headerLen   int64 = 15
		reqBuf      []byte
		err         error
	)

	conn, err = this.makeConn()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	masterFilenameLen := int64(len(masterFilename))
	if len(this.GroupName) > 0 && len(masterFilename) > 0 {
		uploadSlave = true
		// #slave_fmt |-master_len(8)-file_size(8)-prefix_name(16)-file_ext_name(6)
		//       #           -master_name(master_filename_len)-|
		headerLen = int64(38) + masterFilenameLen
	}

	th := TrackerHeader{
		PkgLen: headerLen + int64(size),
		Cmd:    cmd,
	}
	th.sendHeader(conn)

	if uploadSlave {
		req := UploadSlaveFileRequest{
			MasterFileNameLen: masterFilenameLen,
			FileSize:          int64(size),
			PrefixName:        prefixName,
			FileExtName:       fileExtName,
			MasterFilename:    masterFilename,
		}
		reqBuf, err = req.Marshal()
	} else {
		req := UploadFileRequest{
			StorePathIndex: uint8(this.StorePathIndex),
			FileSize:       int64(size),
			FileExtName:    fileExtName,
		}
		reqBuf, err = req.Marshal()
	}
	if err != nil {
		return nil, err
	}
	conn.Write(reqBuf)
	_, err = io.CopyN(conn, input, size)

	if err != nil {
		return nil, err
	}

	th.recvHeader(conn)
	if th.Status != 0 {
		return nil, Errno{int(th.Status)}
	}
	recvBuff, recvSize, err := TcpRecvResponse(conn, th.PkgLen)
	if recvSize <= int64(FDFS_GROUP_NAME_MAX_LEN) {
		errmsg := "[-] Error: Storage response length is not match, "
		errmsg += fmt.Sprintf("expect: %d, actual: %d", th.PkgLen, recvSize)
		return nil, errors.New(errmsg)
	}
	ur := &FileId{}
	err = ur.Unmarshal(recvBuff)
	if err != nil {
		errmsg := fmt.Sprintf("recvBuf can not unmarshal :%s", err.Error())
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

	fileNameLen := len(remoteFilename)
	th := TrackerHeader{
		Cmd:    STORAGE_PROTO_CMD_DELETE_FILE,
		PkgLen: int64(FDFS_GROUP_NAME_MAX_LEN + fileNameLen),
	}
	th.sendHeader(conn)
	fid := FileId{
		GroupName: this.GroupName,
		FileName:  remoteFilename,
	}
	reqBuf, err = fid.Marshal()
	if err != nil {
		return err
	}
	conn.Write(reqBuf)

	th.recvHeader(conn)
	if th.Status != 0 {
//		fmt.Println("DeleteFile:", th.Status)
		return Errno{int(th.Status)}
	}
	return nil
}

//如果下载全部文件,那么downloadSize设为0
func (this *StorageClient) DownloadEx(remoteFilename string, output io.Writer, offset int64, downloadSize int64) (size int64, e error) {

	var (
		conn   net.Conn
		reqBuf []byte
	)
	size = 0
	conn, e = this.makeConn()
	defer conn.Close()
	if e != nil {
		return
	}

	th := TrackerHeader{
		Cmd:    STORAGE_PROTO_CMD_DOWNLOAD_FILE,
		PkgLen: int64(FDFS_PROTO_PKG_LEN_SIZE*2 + FDFS_GROUP_NAME_MAX_LEN + len(remoteFilename)),
	}

	th.sendHeader(conn)

	req := DownloadFileRequest{
		Offset:       offset,
		DownloadSize: downloadSize,
		GroupName:    this.GroupName,
		FileName:     remoteFilename,
	}
	reqBuf, e = req.Marshal()
	if e != nil {
		return
	}
	conn.Write(reqBuf)

	th.recvHeader(conn)
	if th.Status != 0 {
		e = Errno{int(th.Status)}
//		fmt.Println("DownloadEx,", e)
		return
	}
	size, e = io.CopyN(output, conn, th.PkgLen)

	if size < downloadSize {
		errmsg := "[-] Error: Storage response length is not match, "
		errmsg += fmt.Sprintf("expect: %d, actual: %d", th.PkgLen, size)
		e = errors.New(errmsg)
	}
	return
}

func (this *StorageClient) Download(remoteFilename string, output io.Writer) (size int64, e error) {
	return this.DownloadEx(remoteFilename, output, 0, 0)
}

func (this *StorageClient) DownloadToFile(remoteFilename string, localFilename string) (size int64, e error) {
	file, err := os.Create(localFilename)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	return this.Download(remoteFilename, file)
}

func (this *StorageClient) makeConn() (net.Conn, error) {
	addr := fmt.Sprintf("%s:%d", this.IpAddr, this.Port)
	return net.DialTimeout("tcp", addr, time.Minute)
}
