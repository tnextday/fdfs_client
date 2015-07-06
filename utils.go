package fdfs_client

import (
	"errors"
	"fmt"
	"io"
	"strings"
)

type Errno struct {
	status int
}

func (e Errno) Error() string {
	errmsg := fmt.Sprintf("errno [%d] ", e.status)
	switch e.status {
	case 17:
		errmsg += "File Exist"
	case 22:
		errmsg += "Argument Invlid"
	}
	return errmsg
}

func readCstr(buff io.Reader, length int) (string, error) {
	str := make([]byte, length)
	n, err := buff.Read(str)
	if err != nil || n != len(str) {
		return "", Errno{255}
	}

	for i, v := range str {
		if v == 0 {
			str = str[:i]
			break
		}
	}
	return string(str), nil
}

func TrimCStr(cstr []byte) string {
	for i, v := range cstr {
		if v == 0 {
			return string(cstr[:i])
		}
	}
	return string(cstr)
}

func splitRemoteFileId(remoteFileId string) (groupName, remoteFilename string, e error) {
	parts := strings.SplitN(remoteFileId, "/", 2)
	if len(parts) != 2 {
		return "", "", errors.New("error remoteFileId")
	}
	return parts[0], parts[1], nil
}

func getFileExt(filename string) string {
	parts := strings.Split(filename, ".")
	if len(parts) >= 2 {
		return parts[len(parts)-1]
	}
	return ""
}
