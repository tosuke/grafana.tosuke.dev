package ltxws

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/coder/websocket"
	"github.com/superfly/ltx"
)

const (
	RPCChunkSize       = 2 * 1024 * 1024 // 2MiB
	websocketReadLimit = 16 << 20
)

func init() {
	litestream.RegisterReplicaClientFactory(ReplicaClientType, NewReplicaClientFromURL)
}

type RPCRequestType string

const (
	RPCRequestTypeListFiles      RPCRequestType = "list"
	RPCRequestTypeReadFile       RPCRequestType = "read"
	RPCRequestTypeWriteFile      RPCRequestType = "write"
	RPCRequestTypeDeleteFiles    RPCRequestType = "delete"
	RPCRequestTypeDeleteAllFiles RPCRequestType = "delete-all"
)

type RPCRequest struct {
	Type        RPCRequestType       `json:"type"`
	ReadFile    *RPCRequestReadFile  `json:"read_file,omitempty"`
	WriteFile   *RPCRequestWriteFile `json:"write_file,omitempty"`
	DeleteFiles *RPCDeleteFiles      `json:"delete_files,omitempty"`
}

type RPCRequestReadFile struct {
	Level   int      `json:"level"`
	MinTXID ltx.TXID `json:"min_txid"`
	MaxTXID ltx.TXID `json:"max_txid"`
	Offset  int64    `json:"offset"`
	Size    int64    `json:"size"`
}

type RPCRequestWriteFile struct {
	Level     int      `json:"level"`
	MinTXID   ltx.TXID `json:"min_txid"`
	MaxTXID   ltx.TXID `json:"max_txid"`
	CreatedAt int64    `json:"created_at"`
}

type RPCDeleteFiles struct {
	Files []*RPCFileInfo `json:"files"`
}

type RPCResponseType string

const (
	RPCResponseTypeListFiles   RPCResponseType = "list"
	RPCResponseTypeReadFile    RPCResponseType = "read"
	RPCResponseTypeWriteFile   RPCResponseType = "write"
	RPCResponseTypeDeleteFiles RPCResponseType = "delete"
	RPCResponseTypeDeleteAll   RPCResponseType = "delete-all"
	RPCResponseTypeNotFound    RPCResponseType = "not-found"
)

type RPCResponse struct {
	Type  RPCResponseType `json:"type"`
	Files []*RPCFileInfo  `json:"files,omitempty"`
	File  *RPCFileInfo    `json:"file,omitempty"`
}

type RPCFileInfo struct {
	Level     int      `json:"level"`
	MinTXID   ltx.TXID `json:"min_txid"`
	MaxTXID   ltx.TXID `json:"max_txid"`
	Size      int64    `json:"size"`
	CreatedAt int64    `json:"created_at"`
}

const ReplicaClientType = "ltxws"

type ReplicaClient struct {
	logger *slog.Logger
	url    string

	pool sync.Pool
}

var _ litestream.ReplicaClient = (*ReplicaClient)(nil)

func NewReplicaClient(url string) *ReplicaClient {
	return &ReplicaClient{
		logger: slog.Default().With("replica", ReplicaClientType),
		url:    url,
	}
}

func NewReplicaClientFromURL(
	scheme, host, urlPath string,
	query url.Values,
	userinfo *url.Userinfo,
) (litestream.ReplicaClient, error) {
	if host == "" {
		return nil, fmt.Errorf("host required for %s replica URL", ReplicaClientType)
	}
	if userinfo != nil {
		return nil, fmt.Errorf("userinfo not supported for %s replica URL", ReplicaClientType)
	}
	return NewReplicaClient((&url.URL{
		Scheme:   "ws",
		Host:     host,
		Path:     urlPath,
		RawQuery: query.Encode(),
	}).String()), nil
}

func (c *ReplicaClient) Type() string {
	return ReplicaClientType
}

func (c *ReplicaClient) SetLogger(logger *slog.Logger) {
	c.logger = logger.WithGroup(ReplicaClientType)
}

func (*ReplicaClient) Init(context.Context) error {
	return nil
}

func (c *ReplicaClient) LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (iter ltx.FileIterator, err error) {
	conn, closeConn, err := c.getConn(ctx)
	if err != nil {
		return nil, err
	}
	defer closeConn(err)

	if err := doRequest(ctx, conn, &RPCRequest{Type: RPCRequestTypeListFiles}); err != nil {
		return nil, fmt.Errorf("send list files request: %w", err)
	}

	resp, err := readResponse(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("read list files response: %w", err)
	} else if resp.Type != RPCResponseTypeListFiles || resp.Files == nil {
		return nil, fmt.Errorf("unexpected list files response type: %v", resp.Type)
	}

	infos := make([]*ltx.FileInfo, 0, len(resp.Files))
	for _, f := range resp.Files {
		if f.Level != level || f.MinTXID < seek {
			continue
		}
		infos = append(infos, &ltx.FileInfo{
			Level:     f.Level,
			MinTXID:   f.MinTXID,
			MaxTXID:   f.MaxTXID,
			Size:      f.Size,
			CreatedAt: time.Unix(f.CreatedAt, 0),
		})
	}
	return ltx.NewFileInfoSliceIterator(infos), nil
}

func (c *ReplicaClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	conn, closeConn, err := c.getConn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			closeConn(err)
		}
	}()

	if err := doRequest(ctx, conn, &RPCRequest{
		Type: RPCRequestTypeReadFile,
		ReadFile: &RPCRequestReadFile{
			Level:   level,
			MinTXID: minTXID,
			MaxTXID: maxTXID,
			Offset:  offset,
			Size:    size,
		},
	}); err != nil {
		return nil, fmt.Errorf("send read file request: %w", err)
	}

	resp, err := readResponse(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("read read file response: %w", err)
	} else if resp.Type == RPCResponseTypeNotFound {
		return nil, os.ErrNotExist
	} else if resp.Type != RPCResponseTypeReadFile {
		return nil, fmt.Errorf("unexpected read file response type: %v", resp.Type)
	}

	return &fileReader{
		ctx:   ctx,
		conn:  conn,
		close: closeConn,
	}, nil
}

type fileReader struct {
	mu    sync.Mutex
	ctx   context.Context
	conn  *websocket.Conn
	close func(error)
	eof   bool
	buf   []byte
}

var _ io.ReadCloser = (*fileReader)(nil)

func (r *fileReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	n := 0
	for {
		bn := copy(p, r.buf)
		r.buf = r.buf[bn:]
		p = p[bn:]
		n += bn

		if len(p) == 0 {
			return n, nil
		} else if r.eof {
			return n, io.EOF
		}

		typ, data, err := r.conn.Read(r.ctx)
		if err != nil {
			return n, fmt.Errorf("read file chunk: %w", err)
		} else if typ != websocket.MessageBinary {
			return n, fmt.Errorf("expected websocket binary message, got %v", typ)
		}

		r.eof = len(data) < RPCChunkSize
		r.buf = data
	}
}

func (r *fileReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.eof {
		r.close(nil)
		return nil
	} else {
		return r.conn.Close(websocket.StatusNormalClosure, "")
	}
}

var writeBufPool = sync.Pool{
	New: func() any {
		return make([]byte, RPCChunkSize)
	},
}

func (c *ReplicaClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (file *ltx.FileInfo, err error) {
	conn, closeConn, err := c.getConn(ctx)
	if err != nil {
		return nil, err
	}
	defer closeConn(err)

	ltxHeader, r, err := ltx.PeekHeader(r)
	if err != nil {
		return nil, fmt.Errorf("extract LTX header: %w", err)
	}

	if err := doRequest(ctx, conn, &RPCRequest{
		Type: RPCRequestTypeWriteFile,
		WriteFile: &RPCRequestWriteFile{
			Level:     level,
			MinTXID:   minTXID,
			MaxTXID:   maxTXID,
			CreatedAt: time.UnixMilli(ltxHeader.Timestamp).Unix(),
		},
	}); err != nil {
		return nil, fmt.Errorf("send write file request: %w", err)
	}

	buf := writeBufPool.Get().([]byte)
	defer writeBufPool.Put(buf)

	for {
		n, rerr := io.ReadFull(r, buf)
		if rerr != nil && rerr != io.EOF && rerr != io.ErrUnexpectedEOF {
			return nil, fmt.Errorf("read file chunk: %w", rerr)
		}

		// If n = ChunkSize && rerr = nil, send the chunk with continuation.
		// If n < ChunkSize && rerr = UnexpectedEOF, send the chunk with EOF (<ChunkSize).
		// If n = 0         && rerr = EOF, send an empty chunk to signal EOF.
		if err := conn.Write(ctx, websocket.MessageBinary, buf[:n]); err != nil {
			return nil, fmt.Errorf("send file chunk: %w", err)
		}

		if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
			break
		}
	}

	resp, err := readResponse(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("read write file response: %w", err)
	} else if resp.Type != RPCResponseTypeWriteFile || resp.File == nil {
		return nil, fmt.Errorf("unexpected write file response type: %v", resp.Type)
	}

	return &ltx.FileInfo{
		Level:     resp.File.Level,
		MinTXID:   resp.File.MinTXID,
		MaxTXID:   resp.File.MaxTXID,
		Size:      resp.File.Size,
		CreatedAt: time.Unix(resp.File.CreatedAt, 0),
	}, nil
}

func (c *ReplicaClient) DeleteLTXFiles(ctx context.Context, files []*ltx.FileInfo) error {
	conn, closeConn, err := c.getConn(ctx)
	if err != nil {
		return err
	}
	defer closeConn(err)

	rpcFiles := make([]*RPCFileInfo, 0, len(files))
	for _, f := range files {
		rpcFiles = append(rpcFiles, &RPCFileInfo{
			Level:     f.Level,
			MinTXID:   f.MinTXID,
			MaxTXID:   f.MaxTXID,
			Size:      f.Size,
			CreatedAt: f.CreatedAt.Unix(),
		})
	}

	if err := doRequest(ctx, conn, &RPCRequest{
		Type: RPCRequestTypeDeleteFiles,
		DeleteFiles: &RPCDeleteFiles{
			Files: rpcFiles,
		},
	}); err != nil {
		return fmt.Errorf("send delete files request: %w", err)
	}

	resp, err := readResponse(ctx, conn)
	if err != nil {
		return fmt.Errorf("read delete files response: %w", err)
	}
	if resp.Type != RPCResponseTypeDeleteFiles {
		return fmt.Errorf("unexpected delete files response type: %v", resp.Type)
	}

	return nil
}

func (c *ReplicaClient) DeleteAll(ctx context.Context) error {
	conn, closeConn, err := c.getConn(ctx)
	if err != nil {
		return err
	}
	defer closeConn(err)

	if err := doRequest(ctx, conn, &RPCRequest{
		Type: RPCRequestTypeDeleteAllFiles,
	}); err != nil {
		return fmt.Errorf("send delete all files request: %w", err)
	}

	resp, err := readResponse(ctx, conn)
	if err != nil {
		return fmt.Errorf("read delete all files response: %w", err)
	}
	if resp.Type != RPCResponseTypeDeleteAll {
		return fmt.Errorf("unexpected delete all files response type: %v", resp.Type)
	}

	return nil
}

func (c *ReplicaClient) getConn(ctx context.Context) (*websocket.Conn, func(error), error) {
	conn, ok := c.pool.Get().(*websocket.Conn)
	if !ok || conn == nil {
		var err error
		conn, _, err = websocket.Dial(ctx, c.url, nil)
		if err != nil {
			return nil, nil, fmt.Errorf("connect to replica %s: %w", c.url, err)
		}
		conn.SetReadLimit(websocketReadLimit)
	}
	closeFn := func(err error) {
		if err == nil {
			c.pool.Put(conn)
		} else {
			conn.Close(websocket.StatusInternalError, "")
		}
	}
	return conn, closeFn, nil
}

func doRequest(ctx context.Context, conn *websocket.Conn, req *RPCRequest) error {
	reqJSON, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	if err := conn.Write(ctx, websocket.MessageText, reqJSON); err != nil {
		return fmt.Errorf("send request: %w", err)
	}

	return nil
}

func readResponse(ctx context.Context, conn *websocket.Conn) (*RPCResponse, error) {
	respType, respData, err := conn.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	} else if respType != websocket.MessageText {
		return nil, fmt.Errorf("expected websocket text message, got %v", respType)
	}

	var resp RPCResponse
	if err := json.Unmarshal(respData, &resp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}
	return &resp, nil
}
