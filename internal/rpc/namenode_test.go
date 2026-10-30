package rpc

import (
	"io"
	"net"
	"testing"
	"time"

	hadoop "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// answeringTransport stands in for the wire: every readResponse pops the
// next answer, a nil for success or a NamenodeError, and records the call ids
// the requests carried.
type answeringTransport struct {
	answers    []error
	requestIDs []int32
}

func (t *answeringTransport) writeRequest(w io.Writer, method string, requestID int32, req proto.Message) error {
	t.requestIDs = append(t.requestIDs, requestID)
	return nil
}

func (t *answeringTransport) readResponse(r io.Reader, method string, requestID int32, resp proto.Message) error {
	if len(t.answers) == 0 {
		return nil
	}
	answer := t.answers[0]
	t.answers = t.answers[1:]
	return answer
}

func namenodeError(exception string) *NamenodeError {
	return &NamenodeError{
		method:    "getFileInfo",
		code:      int(hadoop.RpcResponseHeaderProto_ERROR_APPLICATION),
		exception: exception,
		message:   "The subtree mlruns is locked by Namenode Id: 1",
	}
}

// connectedWith returns a connection whose dial has already happened, so
// Execute goes straight to the transport.
func connectedWith(t *testing.T, transport transport) *NamenodeConnection {
	client, server := net.Pipe()
	t.Cleanup(func() { client.Close(); server.Close() })
	host := &namenodeHost{address: "namenode:8020"}
	return &NamenodeConnection{
		conn:      client,
		host:      host,
		hostList:  []*namenodeHost{host},
		transport: transport,
		done:      make(chan struct{}),
	}
}

func shortRetriableBackoff(t *testing.T, retries int) {
	initial, max, limit := retriableInitialBackoff, retriableMaxBackoff, retriableMaxRetries
	retriableInitialBackoff, retriableMaxBackoff, retriableMaxRetries = time.Millisecond, 4*time.Millisecond, retries
	t.Cleanup(func() { retriableInitialBackoff, retriableMaxBackoff, retriableMaxRetries = initial, max, limit })
}

func TestExecuteRetriesARetriableExceptionUntilItSucceeds(t *testing.T) {
	shortRetriableBackoff(t, 10)
	transport := &answeringTransport{answers: []error{
		namenodeError(retriableExceptionClass),
		namenodeError(retriableExceptionClass),
		namenodeError(retriableExceptionClass),
		nil,
	}}
	c := connectedWith(t, transport)

	err := c.Execute("getFileInfo", &hadoop.RpcRequestHeaderProto{}, &hadoop.RpcResponseHeaderProto{})

	require.NoError(t, err)
	assert.Equal(t, []int32{1, 2, 3, 4}, transport.requestIDs, "every attempt carries a new call id")
	assert.NotNil(t, c.conn, "the connection stays open across the retries")
	assert.Nil(t, c.host.lastError, "a retriable answer is not a host failure")
}

func TestExecuteGivesUpOnARetriableExceptionAfterTheRetries(t *testing.T) {
	shortRetriableBackoff(t, 3)
	always := make([]error, 100)
	for i := range always {
		always[i] = namenodeError(retriableExceptionClass)
	}
	transport := &answeringTransport{answers: always}
	c := connectedWith(t, transport)

	err := c.Execute("getFileInfo", &hadoop.RpcRequestHeaderProto{}, &hadoop.RpcResponseHeaderProto{})

	require.Error(t, err)
	nerr, ok := err.(*NamenodeError)
	require.True(t, ok, "the namenode's own exception is returned, not a wrapped one")
	assert.Equal(t, retriableExceptionClass, nerr.exception)
	assert.Len(t, transport.requestIDs, 4, "the first attempt plus three retries")
	assert.NotNil(t, c.conn)
}

func TestExecuteReturnsOtherNamenodeExceptionsAtOnce(t *testing.T) {
	shortRetriableBackoff(t, 10)
	transport := &answeringTransport{answers: []error{
		namenodeError("java.io.FileNotFoundException"),
		nil,
	}}
	c := connectedWith(t, transport)

	err := c.Execute("getFileInfo", &hadoop.RpcRequestHeaderProto{}, &hadoop.RpcResponseHeaderProto{})

	require.Error(t, err)
	assert.Equal(t, []int32{1}, transport.requestIDs, "no retry")
	assert.NotNil(t, c.conn, "the connection is kept: the namenode answered")
}
