package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/m-mizutani/rlogs"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var logger = logrus.New()

type S3Location struct {
	S3Region string
	S3Bucket string
	S3Key    string
}

type Arguments struct {
	s3Targets   []S3Location
	dstS3Region string
	dstS3Bucket string
	dstS3Prefix string
}

func NewArguments(s3Region, s3Bucket, s3Prefix string) Arguments {
	args := Arguments{
		dstS3Region: s3Region,
		dstS3Bucket: s3Bucket,
		dstS3Prefix: s3Prefix,
	}
	return args
}

func (x *Arguments) AddSrc(s3Region, s3Bucket, s3Key string) {
	tgt := S3Location{
		S3Region: s3Region,
		S3Bucket: s3Bucket,
		S3Key:    s3Key,
	}
	x.s3Targets = append(x.s3Targets, tgt)
}

type ResultLog struct {
	Src, Dst   S3Location
	FlowCount  int
	ErrorCount int
}
type Results struct {
	Logs []ResultLog
}

func main() {
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.SetLevel(logrus.InfoLevel)

	lambda.Start(func(ctx context.Context, event events.SNSEvent) (Results, error) {
		args, err := createArgs(event)
		if err != nil {
			logger.WithFields(logrus.Fields{
				"error": err,
				"event": event,
			}).Error("Fail to createArgs")
			return Results{}, err
		}

		res, err := MainProc(args)
		if err != nil {
			logger.WithFields(logrus.Fields{
				"error":   err,
				"event":   event,
				"results": res,
			}).Error("Error in main procedure")
		}
		return res, nil
	})
}

func createArgs(event events.SNSEvent) (Arguments, error) {
	args := Arguments{
		dstS3Region: os.Getenv("S3_REGION"),
		dstS3Bucket: os.Getenv("S3_BUCKET"),
		dstS3Prefix: os.Getenv("S3_PREFIX"),
	}

	for _, record := range event.Records {
		var s3Event events.S3Event
		err := json.Unmarshal([]byte(record.SNS.Message), &s3Event)
		if err != nil {
			return args, errors.Wrap(err, "Fail to unmarshal SNS message")
		}

		for _, s3Record := range s3Event.Records {
			tgt := S3Location{
				S3Bucket: s3Record.S3.Bucket.Name,
				S3Key:    s3Record.S3.Object.Key,
				S3Region: s3Record.AWSRegion,
			}
			args.s3Targets = append(args.s3Targets, tgt)
		}
	}

	return args, nil
}

type TimeKey struct {
	year   int
	month  int
	day    int
	hour   int
	minute int
}

func newTimeKey(ts time.Time) TimeKey {
	return TimeKey{
		year:  ts.Year(),
		month: int(ts.Month()),
		day:   ts.Day(),
		hour:  ts.Hour(),
	}
}

func (x *TimeKey) toDatetime() string {
	return fmt.Sprintf("%04d-%02d-%02d-%02d-%02d", x.year, x.month, x.day, x.hour, x.minute-(x.minute%5))
}

func convert(src, dst S3Location) (ResultLog, error) {
	wmap := map[TimeKey]*ParquetFlowLogWriter{}

	result := ResultLog{src, S3Location{}, 0, 0}

	ch := rlogs.Read(src.S3Region, src.S3Bucket, src.S3Key,
		&rlogs.S3GzipLines{}, &VpcFlowLogParser{})

	for q := range ch {
		flowlog, ok := q.Record.Entity.(*FlowLog)
		if !ok {
			continue
		}

		result.FlowCount++

		tkey := newTimeKey(q.Record.Timestamp)

		w, ok := wmap[tkey]
		if !ok {
			newWriter, err := NewParquetFlowLogWriter()
			if err != nil {
				return result, err
			}
			w = &newWriter
			wmap[tkey] = w
		}

		w.Write(flowlog)
	}

	srcPath := strings.Split(src.S3Key, "/")
	srcFileName := srcPath[len(srcPath)-1]

	for tkey, w := range wmap {
		if err := w.Close(); err != nil {
			return result, errors.Wrap(err, "Fail to close a Parquet file")
		}

		fd, err := os.Open(w.FileName)
		if err != nil {
			return result, errors.Wrap(err, "Fail to close a Parquet file")
		}

		dstKey := fmt.Sprintf("%s/dt=%s/%s.parquet", dst.S3Key, tkey.toDatetime(), srcFileName)
		err = uploadS3(dst.S3Region, dst.S3Bucket, dstKey, fd)
		if err != nil {
			return result, err
		}
	}

	return result, nil
}

// MainProc is a main function of VPCFlowLogs converter.
func MainProc(args Arguments) (Results, error) {
	var results Results

	for _, tgt := range args.s3Targets {
		logger.WithFields(logrus.Fields{
			"region": tgt.S3Region,
			"bucket": tgt.S3Bucket,
			"key":    tgt.S3Key,
		}).Info("Downloading a log file from S3")

		dst := S3Location{args.dstS3Region, args.dstS3Bucket, args.dstS3Prefix}

		result, err := convert(tgt, dst)
		if err != nil {
			return results, err
		}

		results.Logs = append(results.Logs, result)
	}

	sql := fmt.Sprintf("ALTER TABLE %s ADD IF NOT EXISTS PARTITION (region='ap-northeast-1') location 's3://mizutani-test/flow-logs-parquet'", os.Getenv("ATHENA_TABLE_NAME"))

	ssn := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(args.dstS3Region),
	}))
	athenaClient := athena.New(ssn)

	resultConf := &athena.ResultConfiguration{}
	resultConf.SetOutputLocation(fmt.Sprintf("s3://%s/%s/result", args.dstS3Bucket, args.dstS3Prefix))

	input := &athena.StartQueryExecutionInput{
		QueryString:         aws.String(sql),
		ResultConfiguration: resultConf,
	}
	output, err := athenaClient.StartQueryExecution(input)
	logger.WithFields(logrus.Fields{"err": err, "input": input, "output": output}).Info("done")

	return results, nil
}

func uploadS3(s3Region, s3Bucket, s3Key string, reader io.Reader) error {
	// Upload
	logger.WithFields(logrus.Fields{"s3Bucket": s3Bucket, "s3key": s3Key}).Info("try to upload")
	ssn := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(s3Region),
	}))
	uploader := s3manager.NewUploader(ssn)
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(s3Bucket),
		Key:    aws.String(s3Key),
		Body:   reader,
	})
	if err != nil {
		return errors.Wrap(err, "Fail to upload data to your bucket")
	}

	return nil
}
