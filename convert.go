package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/m-mizutani/rlogs"
	"github.com/pkg/errors"
)

type partitionKey struct {
	year      int
	month     int
	day       int
	accountID string
}

func newPartitionKey(ts time.Time, flog *FlowLog) partitionKey {
	return partitionKey{
		year:      ts.Year(),
		month:     int(ts.Month()),
		day:       ts.Day(),
		accountID: flog.AccountID,
	}
}

func (x partitionKey) String() string {
	return fmt.Sprintf("%s/%04d/%02d/%02d", x.accountID, x.year, x.month, x.day)
}

func (x partitionKey) Date() string {
	return fmt.Sprintf("%04d-%02d-%02d", x.year, x.month, x.day)
}

type result struct {
	partitionKeys []partitionKey
}

func convert(src, dst S3Location) (*result, error) {
	wmap := map[partitionKey]*ParquetFlowLogWriter{}

	ch := rlogs.Read(src.S3Region, src.S3Bucket, src.S3Key,
		&rlogs.S3GzipLines{}, &VpcFlowLogParser{})

	for q := range ch {
		if q.Error != nil {
			return nil, q.Error
		}

		flowlog, ok := q.Record.Entity.(*FlowLog)
		if !ok {
			continue
		}

		pkey := newPartitionKey(q.Record.Timestamp, flowlog)
		w, ok := wmap[pkey]
		if !ok {
			logger.WithField("pkey", pkey).Info("create a new writer")
			newWriter, err := NewParquetFlowLogWriter()
			if err != nil {
				return nil, err
			}
			w = &newWriter
			wmap[pkey] = w
		}

		w.Write(flowlog)
	}

	srcPath := strings.Split(src.S3Key, "/")
	srcFileName := srcPath[len(srcPath)-1]
	res := result{}

	for pkey, w := range wmap {
		if err := w.Close(); err != nil {
			return nil, errors.Wrap(err, "Fail to close a Parquet file")
		}

		fd, err := os.Open(w.FileName)
		if err != nil {
			return nil, errors.Wrap(err, "Fail to close a Parquet file")
		}

		dstKey := fmt.Sprintf("%s%s/%s.parquet", dst.S3Key, pkey, srcFileName)
		if err = uploadS3(dst.S3Region, dst.S3Bucket, dstKey, fd); err != nil {
			return nil, err
		}

		res.partitionKeys = append(res.partitionKeys, pkey)
	}

	return &res, nil
}
