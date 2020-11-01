package standalone_storage

import (
	"errors"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		db: engine_util.CreateDB(conf.DBPath, false),
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.db.NewTransaction(false)
	return &StandAloneStorageReader{txn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	// 要用transaction包起来，这个batch里任何一个出错，整个batch都可以rollback
	return s.db.Update(func(txn *badger.Txn) (err error) {
		for _, modify := range batch {
			switch modify.Data.(type) {
			case storage.Put:
				data := modify.Data.(storage.Put)
				err = txn.Set(engine_util.KeyWithCF(data.Cf, data.Key), data.Value)
			case storage.Delete:
				data := modify.Data.(storage.Delete)
				err = txn.Delete(engine_util.KeyWithCF(data.Cf, data.Key))
			default:
				err = errors.New("receive modify.data with unsupported type")
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func (sr *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(sr.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		// 若key不存在, 外界不感知error, 只会发现val为nil
		return nil, nil
	}
	return val, err
}

func (sr *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, sr.txn)
}

func (sr *StandAloneStorageReader) Close() {
	sr.txn.Discard()
}