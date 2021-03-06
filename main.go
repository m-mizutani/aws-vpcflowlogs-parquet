package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

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
	s3Targets          []S3Location
	dstS3Region        string
	dstS3Bucket        string
	dstS3Prefix        string
	athenaTableName    string
	athenaDatabaseName string
}

type awsEvent struct {
	Records []map[string]interface{} `json:"Records"`
}

func newArguments(s3Region, s3Bucket, s3Prefix, athenaTableName, athenaDatabaseName string) Arguments {
	args := Arguments{
		dstS3Region:        s3Region,
		dstS3Bucket:        s3Bucket,
		dstS3Prefix:        s3Prefix,
		athenaTableName:    athenaTableName,
		athenaDatabaseName: athenaDatabaseName,
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

func main() {
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.SetLevel(logrus.InfoLevel)

	lambda.Start(func(ctx context.Context, event awsEvent) error {
		args, err := createArgs(event)
		if err != nil {
			logger.WithFields(logrus.Fields{
				"error": err,
				"event": event,
			}).Error("Fail to createArgs")
			return err
		}

		if err := handler(args); err != nil {
			logger.WithFields(logrus.Fields{
				"error": err,
				"event": event,
			}).Error("Error in main procedure")
		}
		return nil
	})
}

func recordToTarget(record map[string]interface{}) ([]S3Location, error) {
	var s3Records []events.S3EventRecord
	if _, ok := record["Sns"]; ok {
		// Extract S3 event records from SNS event record
		raw, err := json.Marshal(record)
		if err != nil {
			return nil, errors.Wrap(err, "Fail to marshal SNS record")
		}

		var snsRecord events.SNSEventRecord
		if err := json.Unmarshal(raw, &snsRecord); err != nil {
			return nil, errors.Wrap(err, "Fail to unmarshal SNS record")
		}

		var s3Event events.S3Event
		if err := json.Unmarshal([]byte(snsRecord.SNS.Message), &s3Event); err != nil {
			return nil, errors.Wrap(err, "Fail to unmarshal S3 event in SNS message")
		}

		s3Records = s3Event.Records

	} else if _, ok := record["s3"]; ok {
		// Unmarshal general record to S3 event record
		raw, err := json.Marshal(record)
		if err != nil {
			return nil, errors.Wrap(err, "Fail to marshal SNS record")
		}

		if err := json.Unmarshal(raw, &s3Records); err != nil {
			return nil, errors.Wrap(err, "Fail to unmarshal S3 event record")
		}

	} else {
		// Unsupported
		logger.WithField("record", record).Warn("Unsupported AWS record")
		return nil, nil
	}

	locations := []S3Location{}
	for _, s3Record := range s3Records {
		locations = append(locations, S3Location{
			S3Bucket: s3Record.S3.Bucket.Name,
			S3Key:    s3Record.S3.Object.Key,
			S3Region: s3Record.AWSRegion,
		})
	}

	return locations, nil
}

func createArgs(event awsEvent) (Arguments, error) {
	args := newArguments(os.Getenv("S3_REGION"), os.Getenv("S3_BUCKET"), os.Getenv("S3_PREFIX"), os.Getenv("ATHENA_TABLE_NAME"), os.Getenv("ATHENA_DATABASE_NAME"))

	for _, record := range event.Records {
		locations, err := recordToTarget(record)
		if err != nil {
			return args, err
		}

		args.s3Targets = append(args.s3Targets, locations...)
	}

	return args, nil
}

func handler(args Arguments) error {
	pkeySet := map[partitionKey]struct{}{}

	for _, tgt := range args.s3Targets {
		logger.WithFields(logrus.Fields{
			"region": tgt.S3Region,
			"bucket": tgt.S3Bucket,
			"key":    tgt.S3Key,
		}).Info("Downloading a log file from S3")

		dst := S3Location{args.dstS3Region, args.dstS3Bucket, args.dstS3Prefix}

		res, err := convert(tgt, dst)
		if err != nil {
			return err
		}

		for _, pkey := range res.partitionKeys {
			logger.WithField("pkey", pkey).Info("created pkey")
			pkeySet[pkey] = struct{}{}
		}
	}

	for pkey := range pkeySet {
		// athenaDBName := os.Getenv("ATHENA_DATABASE_NAME")
		sql := fmt.Sprintf("ALTER TABLE %s.%s ADD IF NOT EXISTS PARTITION (dt='%s', account='%s') LOCATION 's3://%s/%s%s'", args.athenaDatabaseName, args.athenaTableName, pkey.Date(), pkey.accountID, args.dstS3Bucket, args.dstS3Prefix, pkey)

		ssn := session.Must(session.NewSession(&aws.Config{
			Region: aws.String(args.dstS3Region),
		}))
		athenaClient := athena.New(ssn)

		resultConf := &athena.ResultConfiguration{}
		resultConf.SetOutputLocation(fmt.Sprintf("s3://%s/%sresult", args.dstS3Bucket, args.dstS3Prefix))

		input := &athena.StartQueryExecutionInput{
			QueryString:         aws.String(sql),
			ResultConfiguration: resultConf,
		}
		output1, err := athenaClient.StartQueryExecution(input)
		logger.WithFields(logrus.Fields{"err": err, "input": input, "output": output1}).Info("done")

		if err != nil {
			return errors.Wrap(err, "Fail to execute a partitioning query")
		}

		if os.Getenv("CHECK_QUERY_RESULT") != "" {
			for {
				output2, err := athenaClient.GetQueryExecution(&athena.GetQueryExecutionInput{
					QueryExecutionId: output1.QueryExecutionId,
				})
				if err != nil {
					return errors.Wrap(err, "Fail to get an execution result")
				}

				if *output2.QueryExecution.Status.State == "RUNNING" {
					logger.WithField("output", output2).Info("Waiting...")
					time.Sleep(time.Second * 3)
					continue
				}

				logger.WithField("output", output2).Info("done")
				break
			}
		}
	}
	return nil
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
