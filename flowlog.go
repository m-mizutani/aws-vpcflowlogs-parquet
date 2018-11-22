package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type FlowLog struct {
	Version int32 `parquet:"name=version, type=INT32"`
	// AccountID   string `parquet:"name=accountid, type=UTF8, encoding=PLAIN_DICTIONARY"`
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

func (x *FlowLog) Parse(line string) error {
	row := strings.Split(line, " ")
	if len(row) != 14 {
		log.WithField("line", line).Info("Fail to parse")
		return errors.New(fmt.Sprintf("Invalid row length (expected 14, but %d)", len(row)))
	}

	*x = FlowLog{
		// AccountID:   row[1],
		InterfaceID: row[2],
		SrcAddr:     row[3],
		DstAddr:     row[4],
		Action:      row[12],
		LogStatus:   row[13],
	}

	n0, err := strconv.Atoi(row[0])
	if err != nil {
		return err
	}
	x.Version = int32(n0)

	n5, err := strconv.Atoi(row[5])
	if err != nil {
		return err
	}
	x.SrcPort = int32(n5)

	n6, err := strconv.Atoi(row[6])
	if err != nil {
		return err
	}
	x.DstPort = int32(n6)

	n7, err := strconv.Atoi(row[7])
	if err != nil {
		return err
	}
	x.Protocol = int32(n7)

	n8, err := strconv.Atoi(row[8])
	if err != nil {
		return err
	}
	x.Packets = int32(n8)

	n9, err := strconv.Atoi(row[9])
	if err != nil {
		return err
	}
	x.Bytes = int32(n9)

	n10, err := strconv.Atoi(row[10])
	if err != nil {
		return err
	}
	x.Start = int64(n10)

	n11, err := strconv.Atoi(row[11])
	if err != nil {
		return err
	}
	x.End = int64(n11)

	return nil
}
