package server

import (
	"context"
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/commands"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
	"reflect"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// 4B/4C
// Run runs a transactional command.
func (server *Server) Run(cmd commands.Command) (interface{}, error) {
	return commands.RunCommand(cmd, server.storage, server.Latches)
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raw API.
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	response := new(kvrpcpb.RawGetResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		response.Error = err.Error()
		return response, nil
	}

	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		response.Error = err.Error()
	} else if val == nil {
		response.NotFound = true
	} else {
		response.Value = val
	}
	fmt.Println(response)
	return response, nil
}

func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	response := new(kvrpcpb.RawPutResponse)
	err := server.storage.Write(req.Context, []storage.Modify{
		{
			Data: storage.Put{
				Cf:    req.Cf,
				Key:   req.Key,
				Value: req.Value,
			},
		},
	})
	if err != nil {
		response.Error = err.Error()
	}
	return response, nil
}

func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	response := new(kvrpcpb.RawDeleteResponse)
	err := server.storage.Write(req.Context, []storage.Modify{
		{
			Data: storage.Delete{
				Cf:    req.Cf,
				Key:   req.Key,
			},
		},
	})
	if err != nil {
		response.Error = err.Error()
	}
	return response, nil
}

func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	response := new(kvrpcpb.RawScanResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		response.Error = err.Error()
		return response, nil
	}
	it := reader.IterCF(req.Cf)
	defer it.Close()
	for it.Seek(req.StartKey); it.Valid() && len(response.Kvs) < int(req.Limit); it.Next() {
		item := it.Item()
		key := item.KeyCopy(nil)  // 不推荐直接用item.Key()
		val, err := item.ValueCopy(nil)  // 不推荐直接用item.Value()
		if err != nil {
			response.Error = err.Error()
			break
		}
		response.Kvs = append(response.Kvs, &kvrpcpb.KvPair{Key: key, Value: val})
	}
	return response, nil
}

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	cmd := commands.NewGet(req)
	resp, err := server.Run(&cmd)
	if err != nil {
		resp, err = regionError(err, new(kvrpcpb.GetResponse))
		if err != nil {
			return nil, err
		}
	}
	return resp.(*kvrpcpb.GetResponse), err
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	cmd := commands.NewPrewrite(req)
	resp, err := server.Run(&cmd)
	if err != nil {
		resp, err = regionError(err, new(kvrpcpb.PrewriteResponse))
		if err != nil {
			return nil, err
		}
	}
	return resp.(*kvrpcpb.PrewriteResponse), err
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	cmd := commands.NewCommit(req)
	resp, err := server.Run(&cmd)
	if err != nil {
		resp, err = regionError(err, new(kvrpcpb.CommitResponse))
		if err != nil {
			return nil, err
		}
	}
	return resp.(*kvrpcpb.CommitResponse), err
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}

// 4B/4C
// regionError is a help method for handling region errors. If error is a region error, then it is added to resp (which
// muse have a `RegionError` field; the response is returned. If the error is not a region error, then regionError returns
// nil and the error.
func regionError(err error, resp interface{}) (interface{}, error) {
	if regionError, ok := err.(*raft_storage.RegionError); ok {
		respValue := reflect.ValueOf(resp)
		respValue.FieldByName("RegionError").Set(reflect.ValueOf(regionError.RequestErr))
		return resp, nil
	}

	return nil, err
}
