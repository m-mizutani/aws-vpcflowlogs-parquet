package main

import (
	"io/ioutil"

	"github.com/pkg/errors"
	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/ParquetWriter"
	"github.com/xitongsys/parquet-go/parquet"
)

type ParquetFlowLogWriter struct {
	FileName string
	file     ParquetFile.ParquetFile
	writer   *ParquetWriter.ParquetWriter
}

func NewParquetFlowLogWriter() (ParquetFlowLogWriter, error) {
	x := ParquetFlowLogWriter{}

	// Create a temporary file
	tfd, err := ioutil.TempFile("", "")
	if err != nil {
		return x, errors.Wrap(err, "Fail to open local file")
	}
	x.FileName = tfd.Name()
	tfd.Close()

	// Re-open as a Pqrquet file.
	fw, err := ParquetFile.NewLocalFileWriter(x.FileName)
	x.file = fw

	pw, err := ParquetWriter.NewParquetWriter(fw, new(FlowLog), 1)
	pw.RowGroupSize = 128 * 1024 * 1024 //128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	x.writer = pw
	return x, nil
}

func (x *ParquetFlowLogWriter) Write(v FlowLog) error {
	if err := x.writer.Write(v); err != nil {
		return err
	}
	return nil
}

func (x *ParquetFlowLogWriter) Close() error {
	if err := x.writer.WriteStop(); err != nil {
		return err
	}

	x.file.Close()
	return nil
}
