package main_test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"os"
	"strings"
	"testing"

	log "github.com/sirupsen/logrus"

	flowlog "github.com/m-mizutani/aws-vpcflowlogs-parquet"
	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/ParquetReader"
)

func TestWriteFile(t *testing.T) {
	w, err := flowlog.NewParquetFlowLogWriter()
	require.NoError(t, err)

	fd, err := os.Open("samples/sample1.log")
	require.NoError(t, err)

	scanner := bufio.NewScanner(fd)
	require.NoError(t, err)

	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "version ") {
			continue
		}

		var flog flowlog.FlowLog
		err := flog.Parse(scanner.Text())

		require.NoError(t, err)
		err = w.Write(flog)
		require.NoError(t, err)
	}

	err = w.Close()
	require.NoError(t, err)

	fr, err := ParquetFile.NewLocalFileReader(w.FileName)
	require.NoError(t, err)
	pr, err := ParquetReader.NewParquetReader(fr, new(flowlog.FlowLog), 1)
	require.NoError(t, err)

	wflog := make([]flowlog.FlowLog, 2)
	assert.Equal(t, 2, int(pr.GetNumRows()))
	err = pr.Read(&wflog)
	require.NoError(t, err)

	pr.ReadStop()
	fr.Close()
}

func TestParse(t *testing.T) {
	fd, err := os.Open("samples/sample1.log")
	require.NoError(t, err)
	defer fd.Close()

	scanner := bufio.NewScanner(fd)
	require.NoError(t, err)

	require.True(t, scanner.Scan()) // 1st line
	require.True(t, scanner.Scan()) // 2nd line

	var flog flowlog.FlowLog
	err = flog.Parse(scanner.Text())
	require.NoError(t, err)
	assert.Equal(t, flog.Version, int32(2))
	// assert.Equal(t, flog.AccountID, "123456789000")
	assert.Equal(t, flog.InterfaceID, "eni-70594239")
	assert.Equal(t, flog.SrcAddr, "10.0.2.2")
	assert.Equal(t, flog.DstAddr, "10.0.4.8")
	assert.Equal(t, flog.SrcPort, int32(1234))
	assert.Equal(t, flog.DstPort, int32(5245))
	assert.Equal(t, flog.Protocol, int32(17))
	assert.Equal(t, flog.Packets, int32(6))
	assert.Equal(t, flog.Bytes, int32(965))
	assert.Equal(t, flog.Start, int64(1541375637))
	assert.Equal(t, flog.End, int64(1541376057))
	assert.Equal(t, flog.Action, "ACCEPT")
	assert.Equal(t, flog.LogStatus, "OK")
}

type testParameters struct {
	S3Region string `json:"s3region"`
	S3Bucket string `json:"s3bucket"`
	S3Prefix string `json:"s3prefix"`
}

func loadTestParameters() testParameters {
	paramFile := "test.json"
	fd, err := os.Open(paramFile)
	if err != nil {
		log.Printf("Can not open")
		log.Error(err)
	}

	data, err := ioutil.ReadAll(fd)
	if err != nil {
		log.Error(err)
	}

	var p testParameters
	err = json.Unmarshal(data, &p)
	if err != nil {
		log.Error(err)
	}

	return p
}

func TestMain(t *testing.T) {
	p := loadTestParameters()
	fd, err := os.Open("samples/sample2.log.gz")
	defer fd.Close()

	tmpID := uuid.New().String()
	srcS3Key := fmt.Sprintf("%s%s/AWSLogs/1234567890/vpcflowlogs/ap-northeast-1/2018/11/20/sample.gz", p.S3Prefix, tmpID)
	dstS3Prefix := fmt.Sprintf("%s%s/dst/", p.S3Prefix, tmpID)

	uploader := s3manager.NewUploader(
		session.Must(session.NewSession(&aws.Config{Region: &p.S3Region})))
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(p.S3Bucket),
		Key:    aws.String(srcS3Key),
		Body:   fd,
	})

	require.NoError(t, err)

	args := flowlog.NewArguments(p.S3Region, p.S3Bucket, dstS3Prefix)
	args.AddSrc(p.S3Region, p.S3Bucket, srcS3Key)
	results, err := flowlog.MainProc(args)
	require.NoError(t, err)
	assert.Equal(t, 1, len(results.Logs))
	assert.Equal(t, 2, results.Logs[0].FlowCount)
	assert.Equal(t, 0, results.Logs[0].ErrorCount)
}
