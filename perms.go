package hdfs

import (
	"os"
	"time"

	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

// Chmod changes the mode of the named file to mode.
func (c *Client) Chmod(name string, perm os.FileMode) error {
	req := &hdfs.SetPermissionRequestProto{
		Src:        proto.String(name),
		Permission: &hdfs.FsPermissionProto{Perm: proto.Uint32(uint32(perm))},
	}
	resp := &hdfs.SetPermissionResponseProto{}

	err := c.namenode.Execute("setPermission", req, resp)
	if err != nil {
		return &os.PathError{"chmod", name, interpretException(err)}
	}

	return nil
}

// CheckAccess returns nil when the client's user may access the named file or
// directory with mode, any combination of 0o4 (read), 0o2 (write) and 0o1
// (execute). A denial wraps os.ErrPermission, a missing path os.ErrNotExist,
// and a mode with any other bit os.ErrInvalid, without calling the namenode.
func (c *Client) CheckAccess(name string, mode os.FileMode) error {
	action, ok := fsActions[mode]
	if !ok {
		return &os.PathError{"check access", name, os.ErrInvalid}
	}
	req := &hdfs.CheckAccessRequestProto{
		Path: proto.String(name),
		Mode: action.Enum(),
	}
	resp := &hdfs.CheckAccessResponseProto{}

	err := c.namenode.Execute("checkAccess", req, resp)
	if err != nil {
		return &os.PathError{"check access", name, interpretException(err)}
	}

	return nil
}

var fsActions = map[os.FileMode]hdfs.AclEntryProto_FsActionProto{
	0o0: hdfs.AclEntryProto_NONE,
	0o1: hdfs.AclEntryProto_EXECUTE,
	0o2: hdfs.AclEntryProto_WRITE,
	0o3: hdfs.AclEntryProto_WRITE_EXECUTE,
	0o4: hdfs.AclEntryProto_READ,
	0o5: hdfs.AclEntryProto_READ_EXECUTE,
	0o6: hdfs.AclEntryProto_READ_WRITE,
	0o7: hdfs.AclEntryProto_PERM_ALL,
}

// Chown changes the user and group of the file. Unlike os.Chown, this takes
// a string username and group (since that's what HDFS uses.)
//
// If an empty string is passed for user or group, that field will not be
// changed remotely.
func (c *Client) Chown(name string, user, group string) error {
	req := &hdfs.SetOwnerRequestProto{
		Src:       proto.String(name),
		Username:  proto.String(user),
		Groupname: proto.String(group),
	}
	resp := &hdfs.SetOwnerResponseProto{}

	err := c.namenode.Execute("setOwner", req, resp)
	if err != nil {
		return &os.PathError{"chown", name, interpretException(err)}
	}

	return nil
}

// Chtimes changes the access and modification times of the named file.
func (c *Client) Chtimes(name string, atime time.Time, mtime time.Time) error {
	req := &hdfs.SetTimesRequestProto{
		Src:   proto.String(name),
		Mtime: proto.Uint64(uint64(mtime.Unix()) * 1000),
		Atime: proto.Uint64(uint64(atime.Unix()) * 1000),
	}
	resp := &hdfs.SetTimesResponseProto{}

	err := c.namenode.Execute("setTimes", req, resp)
	if err != nil {
		return &os.PathError{"chtimes", name, interpretException(err)}
	}

	return nil
}
