package commands

import (
	"fmt"
	"reflect"

	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

type Commit struct {
	CommandBase
	request *kvrpcpb.CommitRequest
}

func NewCommit(request *kvrpcpb.CommitRequest) Commit {
	return Commit{
		CommandBase: CommandBase{
			context: request.Context,
			startTs: request.StartVersion,
		},
		request: request,
	}
}

func (c *Commit) PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error) {
	commitTs := c.request.CommitVersion
	if commitTs <= c.request.StartVersion {
		return nil, fmt.Errorf("invalid transaction timestamp: %d (commit TS) <= %d (start TS)", commitTs, c.request.StartVersion)
	}

	response := new(kvrpcpb.CommitResponse)

	// Commit each key.
	for _, k := range c.request.Keys {
		resp, e := commitKey(k, commitTs, txn, response)
		if resp != nil || e != nil {
			return response, e
		}
	}

	return response, nil
}

func commitKey(key []byte, commitTs uint64, txn *mvcc.MvccTxn, response interface{}) (interface{}, error) {
	lock, err := txn.GetLock(key)
	if err != nil {
		return nil, err
	}
	if lock == nil {
		return nil, nil
	}

	if lock.Ts != txn.StartTS {
		// Key is locked by a different transaction.
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		if write == nil || write.Kind == mvcc.WriteKindRollback {
			// Transaction has been rolled back.
			respValue := reflect.ValueOf(response)
			keyError := &kvrpcpb.KeyError{Retryable: fmt.Sprintf("lock not found for key %v", key)}
			reflect.Indirect(respValue).FieldByName("Error").Set(reflect.ValueOf(keyError))
			return response, nil
		} else {
			// Already committed.
			return nil, nil
		}
	}

	// Commit a Write object to the DB
	write := mvcc.Write{StartTS: txn.StartTS, Kind: lock.Kind}
	txn.PutWrite(key, commitTs, &write)
	// Unlock the key
	txn.DeleteLock(key)

	return nil, nil
}

func (c *Commit) WillWrite() [][]byte {
	return c.request.Keys
}
