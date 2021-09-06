package hdfs

import (
	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
)

// NN group membership
func (c *Client) getActiveNNs() ([]*hdfs.ActiveNodeProto, error) {

	req := &hdfs.ActiveNamenodeListRequestProto{}
	resp := &hdfs.ActiveNamenodeListResponseProto{}

	err := c.namenode.Execute("getActiveNamenodesForClient", req, resp)
	if err != nil {
		return nil, interpretException(err)
	}

	return resp.GetNamenodes(), nil
}
