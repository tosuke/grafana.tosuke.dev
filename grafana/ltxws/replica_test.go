package ltxws

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/coder/websocket"
	"github.com/superfly/ltx"
)

type replicaRPCHandler func(context.Context, *websocket.Conn) error

func TestReplicaClientURLRegistration(t *testing.T) {
	client, err := litestream.NewReplicaClientFromURL("ltxws://replica.worker/rpc?token=test")
	if err != nil {
		t.Fatalf("create replica client from URL: %v", err)
	}

	got := client.(*ReplicaClient)
	if got.Type() != ReplicaClientType {
		t.Fatalf("Type() = %q, want %q", got.Type(), ReplicaClientType)
	}
	if got.url != "ws://replica.worker/rpc?token=test" {
		t.Fatalf("WebSocket URL = %q", got.url)
	}
}

func newReplicaRPCClient(t *testing.T, handler replicaRPCHandler) *ReplicaClient {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: []string{"*"}})
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		conn.SetReadLimit(16 << 20)
		defer conn.Close(websocket.StatusNormalClosure, "")
		if err := handler(t.Context(), conn); err != nil {
			t.Errorf("RPC handler: %v", err)
		}
	}))
	t.Cleanup(server.Close)

	return NewReplicaClient("ws" + strings.TrimPrefix(server.URL, "http"))
}

func readTestRPCRequest(ctx context.Context, conn *websocket.Conn) (*RPCRequest, error) {
	typ, data, err := conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	if typ != websocket.MessageText {
		return nil, errors.New("expected text RPC request")
	}

	var req RPCRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, err
	}
	return &req, nil
}

func writeTestRPCResponse(ctx context.Context, conn *websocket.Conn, response RPCResponse) error {
	data, err := json.Marshal(response)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, data)
}

func TestLTXWSReplicaClientLTXFiles(t *testing.T) {
	createdAt := time.Unix(1_700_000_000, 0)
	client := newReplicaRPCClient(t, func(ctx context.Context, conn *websocket.Conn) error {
		req, err := readTestRPCRequest(ctx, conn)
		if err != nil {
			return err
		}
		if req.Type != RPCRequestTypeListFiles {
			return errors.New("unexpected list files request")
		}
		return writeTestRPCResponse(ctx, conn, RPCResponse{
			Type: RPCResponseTypeListFiles,
			Files: []*RPCFileInfo{
				{Level: 0, MinTXID: 2, MaxTXID: 3, Size: 10, CreatedAt: createdAt.Unix()},
				{Level: 0, MinTXID: 4, MaxTXID: 5, Size: 20, CreatedAt: createdAt.Unix()},
				{Level: 1, MinTXID: 4, MaxTXID: 5, Size: 30, CreatedAt: createdAt.Unix()},
			},
		})
	})

	iter, err := client.LTXFiles(t.Context(), 0, 3, false)
	if err != nil {
		t.Fatalf("LTXFiles() error = %v", err)
	}
	files, err := ltx.SliceFileIterator(iter)
	if err != nil {
		t.Fatalf("iterate LTX files: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("LTXFiles() returned %d files, want 1", len(files))
	}
	got := files[0]
	if got.Level != 0 || got.MinTXID != 4 || got.MaxTXID != 5 || got.Size != 20 || !got.CreatedAt.Equal(createdAt) {
		t.Fatalf("LTXFiles() file = %+v, want level 0, TXID 4-5, size 20, created at %v", got, createdAt)
	}
}

func TestLTXWSReplicaClientOpenLTXFileReadsChunks(t *testing.T) {
	const (
		level   = 2
		minTXID = ltx.TXID(10)
		maxTXID = ltx.TXID(20)
		offset  = int64(7)
		size    = int64(12345)
	)
	first := bytes.Repeat([]byte{'a'}, RPCChunkSize)
	second := []byte("last chunk")
	client := newReplicaRPCClient(t, func(ctx context.Context, conn *websocket.Conn) error {
		req, err := readTestRPCRequest(ctx, conn)
		if err != nil {
			return err
		}
		if req.Type != RPCRequestTypeReadFile || req.ReadFile == nil {
			return errors.New("unexpected read file request")
		}
		if req.ReadFile.Level != level || req.ReadFile.MinTXID != minTXID || req.ReadFile.MaxTXID != maxTXID || req.ReadFile.Offset != offset || req.ReadFile.Size != size {
			return errors.New("read file request fields do not match")
		}
		if err := writeTestRPCResponse(ctx, conn, RPCResponse{Type: RPCResponseTypeReadFile}); err != nil {
			return err
		}
		if err := conn.Write(ctx, websocket.MessageBinary, first); err != nil {
			return err
		}
		return conn.Write(ctx, websocket.MessageBinary, second)
	})

	reader, err := client.OpenLTXFile(t.Context(), level, minTXID, maxTXID, offset, size)
	if err != nil {
		t.Fatalf("OpenLTXFile() error = %v", err)
	}
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	want := append(first, second...)
	if !bytes.Equal(got, want) {
		t.Fatalf("read content has length %d, want %d", len(got), len(want))
	}
}

func TestLTXWSReplicaClientOpenLTXFileNotFound(t *testing.T) {
	client := newReplicaRPCClient(t, func(ctx context.Context, conn *websocket.Conn) error {
		if _, err := readTestRPCRequest(ctx, conn); err != nil {
			return err
		}
		return writeTestRPCResponse(ctx, conn, RPCResponse{Type: RPCResponseTypeNotFound})
	})

	_, err := client.OpenLTXFile(t.Context(), 0, 1, 2, 0, 10)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("OpenLTXFile() error = %v, want os.ErrNotExist", err)
	}
}

func TestLTXWSReplicaClientWriteLTXFile(t *testing.T) {
	const (
		level   = 3
		minTXID = ltx.TXID(30)
		maxTXID = ltx.TXID(40)
	)
	createdAtMillis := int64(1_700_000_123_456)
	header := ltx.Header{
		PageSize:  4096,
		Commit:    1,
		MinTXID:   minTXID,
		MaxTXID:   maxTXID,
		Timestamp: createdAtMillis,
	}
	headerBytes, err := header.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal LTX header: %v", err)
	}
	wantContent := append(headerBytes, bytes.Repeat([]byte("x"), RPCChunkSize+3)...)

	client := newReplicaRPCClient(t, func(ctx context.Context, conn *websocket.Conn) error {
		req, err := readTestRPCRequest(ctx, conn)
		if err != nil {
			return err
		}
		if req.Type != RPCRequestTypeWriteFile || req.WriteFile == nil {
			return errors.New("unexpected write file request")
		}
		if req.WriteFile.Level != level || req.WriteFile.MinTXID != minTXID || req.WriteFile.MaxTXID != maxTXID || req.WriteFile.CreatedAt != time.UnixMilli(createdAtMillis).Unix() {
			return errors.New("write file request fields do not match")
		}

		var gotContent []byte
		for {
			typ, data, err := conn.Read(ctx)
			if err != nil {
				return err
			}
			if typ != websocket.MessageBinary {
				return errors.New("expected binary file chunk")
			}
			gotContent = append(gotContent, data...)
			if len(data) < RPCChunkSize {
				break
			}
		}
		if !bytes.Equal(gotContent, wantContent) {
			return errors.New("uploaded content does not match")
		}
		return writeTestRPCResponse(ctx, conn, RPCResponse{
			Type: RPCResponseTypeWriteFile,
			File: &RPCFileInfo{
				Level:     level,
				MinTXID:   minTXID,
				MaxTXID:   maxTXID,
				Size:      int64(len(wantContent)),
				CreatedAt: time.UnixMilli(createdAtMillis).Unix(),
			},
		})
	})

	got, err := client.WriteLTXFile(t.Context(), level, minTXID, maxTXID, bytes.NewReader(wantContent))
	if err != nil {
		t.Fatalf("WriteLTXFile() error = %v", err)
	}
	if got.Level != level || got.MinTXID != minTXID || got.MaxTXID != maxTXID || got.Size != int64(len(wantContent)) || !got.CreatedAt.Equal(time.Unix(time.UnixMilli(createdAtMillis).Unix(), 0)) {
		t.Fatalf("WriteLTXFile() = %+v, want metadata for uploaded file", got)
	}
}

func TestLTXWSReplicaClientDeleteLTXFiles(t *testing.T) {
	createdAt := time.Unix(1_700_000_000, 0)
	client := newReplicaRPCClient(t, func(ctx context.Context, conn *websocket.Conn) error {
		req, err := readTestRPCRequest(ctx, conn)
		if err != nil {
			return err
		}
		if req.Type != RPCRequestTypeDeleteFiles || req.DeleteFiles == nil || len(req.DeleteFiles.Files) != 1 {
			return errors.New("unexpected delete files request")
		}
		file := req.DeleteFiles.Files[0]
		if file.Level != 1 || file.MinTXID != 2 || file.MaxTXID != 3 || file.Size != 4 || file.CreatedAt != createdAt.Unix() {
			return errors.New("delete files request fields do not match")
		}
		return writeTestRPCResponse(ctx, conn, RPCResponse{Type: RPCResponseTypeDeleteFiles})
	})

	err := client.DeleteLTXFiles(t.Context(), []*ltx.FileInfo{{
		Level:     1,
		MinTXID:   2,
		MaxTXID:   3,
		Size:      4,
		CreatedAt: createdAt,
	}})
	if err != nil {
		t.Fatalf("DeleteLTXFiles() error = %v", err)
	}
}

func TestLTXWSReplicaClientDeleteAll(t *testing.T) {
	client := newReplicaRPCClient(t, func(ctx context.Context, conn *websocket.Conn) error {
		req, err := readTestRPCRequest(ctx, conn)
		if err != nil {
			return err
		}
		if req.Type != RPCRequestTypeDeleteAllFiles {
			return errors.New("unexpected delete all request")
		}
		return writeTestRPCResponse(ctx, conn, RPCResponse{Type: RPCResponseTypeDeleteAll})
	})

	if err := client.DeleteAll(t.Context()); err != nil {
		t.Fatalf("DeleteAll() error = %v", err)
	}
}
