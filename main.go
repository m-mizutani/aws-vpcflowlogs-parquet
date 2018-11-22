package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

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
	log.SetFormatter(&log.JSONFormatter{})
	log.SetLevel(log.InfoLevel)
	lambda.Start(func(ctx context.Context, event events.SNSEvent) (Results, error) {
		args, err := createArgs(event)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
				"event": event,
			}).Error("Fail to createArgs")
			return Results{}, err
		}

		res, err := MainProc(args)
		if err != nil {
			log.WithFields(log.Fields{
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

func newTimeKey(timestamp int64) TimeKey {
	ts := time.Unix(timestamp, 0)
	m := ts.Minute()
	tk := TimeKey{
		year:   ts.Year(),
		month:  int(ts.Month()),
		day:    ts.Day(),
		hour:   ts.Hour(),
		minute: m - (m % 5),
	}
	return tk
}

func (x *TimeKey) toDatetime() string {
	return fmt.Sprintf("%04d-%02d-%02d-%02d-%02d", x.year, x.month, x.day, x.hour, x.minute-(x.minute%5))
}

func convert(src, dst S3Location) (ResultLog, error) {
	wmap := map[TimeKey]*ParquetFlowLogWriter{}

	result := ResultLog{src, S3Location{}, 0, 0}

	reader, err := downloadS3(src.S3Region, src.S3Bucket, src.S3Key)
	if err != nil {
		return result, errors.Wrap(err, "Fail to dwonload a original log file")
	}
	zr, err := gzip.NewReader(reader)
	if err != nil {
		return result, errors.Wrap(err, "Fail to decode a downloaded S3 file as gzip")
	}

	scanner := bufio.NewScanner(zr)

	for scanner.Scan() {
		var flowlog FlowLog
		line := scanner.Text()
		if line == "version account-id interface-id srcaddr dstaddr srcport dstport protocol packets bytes start end action log-status" { // Header
			continue
		}

		err := flowlog.Parse(line)
		if err != nil {
			result.ErrorCount++
			log.WithField("log", line).Warn(err)
		} else {
			result.FlowCount++
		}

		tkey := newTimeKey(flowlog.Start)

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
		err = w.Close()
		if err != nil {
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

		log.WithFields(log.Fields{
			"region": tgt.S3Region,
			"bucket": tgt.S3Bucket,
			"key":    tgt.S3Key,
		}).Info("Downloading a log file from S3")

		s3Path := strings.Split(tgt.S3Key, "/")
		var p int
		for p = 0; s3Path[p] != "AWSLogs" && p < len(s3Path); p++ {
		}
		if len(s3Path)-p != 8 {
			log.WithFields(log.Fields{
				"s3key": tgt.S3Key,
				"p":     p,
			}).Warn("Invalid S3 path")
			continue
		}

		dstKey := fmt.Sprintf("%saccount=%s/region=%s", args.dstS3Prefix, s3Path[p+1], s3Path[p+3])
		dst := S3Location{args.dstS3Region, args.dstS3Bucket, dstKey}

		result, err := convert(tgt, dst)
		if err != nil {
			return results, err
		}

		results.Logs = append(results.Logs, result)
	}

	return results, nil
}

func uploadS3(s3Region, s3Bucket, s3Key string, reader io.Reader) error {
	// Upload
	log.WithFields(log.Fields{"s3Bucket": s3Bucket, "s3key": s3Key}).Info("try to upload")
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

func downloadS3(s3Region, s3Bucket, s3Key string) (io.Reader, error) {
	log.WithFields(log.Fields{"s3Bucket": s3Bucket, "s3key": s3Key}).Info("try to download")
	ssn := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(s3Region),
	}))
	svc := s3.New(ssn)

	input := &s3.GetObjectInput{
		Bucket: aws.String(s3Bucket),
		Key:    aws.String(s3Key),
	}

	result, err := svc.GetObject(input)
	if err != nil {
		return nil, err
	}

	return result.Body, nil
}
