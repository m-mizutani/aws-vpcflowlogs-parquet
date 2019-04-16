package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/m-mizutani/rlogs"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type FlowLog struct {
	Version     int32  `parquet:"name=version, type=INT32"`
	AccountID   string `parquet:"name=accountid, type=UTF8, encoding=PLAIN_DICTIONARY"`
	InterfaceID string `parquet:"name=interfaceid, type=UTF8, encoding=PLAIN_DICTIONARY"`
	SrcAddr     string `parquet:"name=srcaddr, type=UTF8, encoding=PLAIN_DICTIONARY"`
	DstAddr     string `parquet:"name=dstaddr, type=UTF8, encoding=PLAIN_DICTIONARY"`
	SrcPort     int32  `parquet:"name=srcport, type=INT32"`
	DstPort     int32  `parquet:"name=dstport, type=INT32"`
	Protocol    int32  `parquet:"name=protocol, type=INT32"`
	Packets     int32  `parquet:"name=packets, type=INT32"`
	Bytes       int32  `parquet:"name=bytes, type=INT32"`
	Start       int64  `parquet:"name=start_ts, type=INT64"`
	End         int64  `parquet:"name=end_ts, type=INT64"`
	Action      string `parquet:"name=action, type=UTF8, encoding=PLAIN_DICTIONARY"`
	LogStatus   string `parquet:"name=logstatus, type=UTF8, encoding=PLAIN_DICTIONARY"`
}

type VpcFlowLogParser struct{}

func (x *VpcFlowLogParser) Parse(data []byte) ([]rlogs.LogRecord, error) {
	line := string(data)

	row := strings.Split(line, " ")
	if len(row) != 14 {
		log.WithField("line", line).Info("Fail to parse")
		return nil, errors.New(fmt.Sprintf("Invalid row length (expected 14, but %d)", len(row)))
	}

	if row[0] == "version" {
		return nil, nil // Skip header
	}

	if row[0] != "2" {
		return nil, errors.New(fmt.Sprintf("Unsupported VPC Flow Logs version:cat  %s", row[0]))
	}

	log := FlowLog{
		AccountID:   row[1],
		InterfaceID: row[2],
		SrcAddr:     row[3],
		DstAddr:     row[4],
		Action:      row[12],
		LogStatus:   row[13],
	}

	if n0, err := strconv.Atoi(row[0]); err == nil {
		log.Version = int32(n0)
	}

	if n5, err := strconv.Atoi(row[5]); err == nil {
		log.SrcPort = int32(n5)
	}

	if n6, err := strconv.Atoi(row[6]); err == nil {
		log.DstPort = int32(n6)
	}

	if n7, err := strconv.Atoi(row[7]); err == nil {
		log.Protocol = int32(n7)
	}

	if n8, err := strconv.Atoi(row[8]); err == nil {
		log.Packets = int32(n8)
	}

	if n9, err := strconv.Atoi(row[9]); err == nil {
		log.Bytes = int32(n9)
	}

	if n10, err := strconv.Atoi(row[10]); err == nil {
		log.Start = int64(n10)
	}
	ts := time.Unix(log.Start, 0)

	if n11, err := strconv.Atoi(row[11]); err == nil {
		log.End = int64(n11)
	}

	return []rlogs.LogRecord{{
		Entity:    &log,
		Encodable: &log,
		Timestamp: ts,
		Tag:       "aws.vpcflowlogs",
	}}, nil
}
