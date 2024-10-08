/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package admin

import (
	"encoding/json"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	jsoniter "github.com/json-iterator/go"
	"github.com/tidwall/gjson"
	"strconv"
	"strings"
)

type RemotingSerializable struct {
}

func (r *RemotingSerializable) Encode(obj interface{}) ([]byte, error) {
	jsonStr := r.ToJson(obj, false)
	if jsonStr != "" {
		return []byte(jsonStr), nil
	}
	return nil, nil
}

func (r *RemotingSerializable) ToJson(obj interface{}, prettyFormat bool) string {
	if prettyFormat {
		jsonBytes, err := json.MarshalIndent(obj, "", "  ")
		if err != nil {
			return ""
		}
		return string(jsonBytes)
	} else {
		jsonBytes, err := json.Marshal(obj)
		if err != nil {
			return ""
		}
		return string(jsonBytes)
	}
}
func (r *RemotingSerializable) Decode(data []byte, classOfT interface{}) (interface{}, error) {
	jsonStr := string(data)
	return r.FromJson(jsonStr, classOfT)
}

func (r *RemotingSerializable) FromJson(jsonStr string, classOfT interface{}) (interface{}, error) {
	err := json.Unmarshal([]byte(jsonStr), classOfT)
	if err != nil {
		return nil, err
	}
	return classOfT, nil
}

type TopicList struct {
	TopicList  []string
	BrokerAddr string
	RemotingSerializable
}

type SubscriptionGroupWrapper struct {
	SubscriptionGroupTable map[string]SubscriptionGroupConfig
	DataVersion            DataVersion
	RemotingSerializable
}

type DataVersion struct {
	Timestamp int64
	Counter   int32
}

type SubscriptionGroupConfig struct {
	GroupName                      string
	ConsumeEnable                  bool
	ConsumeFromMinEnable           bool
	ConsumeBroadcastEnable         bool
	RetryMaxTimes                  int
	RetryQueueNums                 int
	BrokerId                       int
	WhichBrokerWhenConsumeSlowly   int
	NotifyConsumerIdsChangedEnable bool
	RemotingSerializable
}

type TopicConfig struct {
	TopicName      string
	ReadQueueNums  int
	WriteQueueNums int
	Perm           int
	Order          bool
	Attributes     map[string]string
	RemotingSerializable
}

type ClusterInfo struct {
	BrokerAddrTable  map[string]internal.BrokerData
	ClusterAddrTable map[string][]string
	RemotingSerializable
}

func (clusterData *ClusterInfo) Decode(data string) error {
	res := gjson.Parse(data)
	err := jsoniter.Unmarshal([]byte(res.Get("clusterAddrTable").String()), &clusterData.ClusterAddrTable)

	if err != nil {
		return err
	}

	brokerAddrTable := res.Get("brokerAddrTable").Map()
	clusterData.BrokerAddrTable = make(map[string]internal.BrokerData)
	for brokerName, v := range brokerAddrTable {
		bd := &internal.BrokerData{
			BrokerName:      v.Get("brokerName").String(),
			Cluster:         v.Get("cluster").String(),
			BrokerAddresses: make(map[int64]string, 0),
		}
		addrs := v.Get("brokerAddrs").String()
		strs := strings.Split(addrs[1:len(addrs)-1], ",")
		if strs != nil {
			for _, str := range strs {
				i := strings.Index(str, ":")
				if i < 0 {
					continue
				}
				brokerId := strings.ReplaceAll(str[0:i], "\"", "")
				id, _ := strconv.ParseInt(brokerId, 10, 64)
				bd.BrokerAddresses[id] = strings.Replace(str[i+1:], "\"", "", -1)
			}
		}
		clusterData.BrokerAddrTable[brokerName] = *bd
	}
	return nil
}

type TopicOffset struct {
	MinOffset           int64
	MaxOffset           int64
	LastUpdateTimestamp int64
}

type TopicStatsTable struct {
	OffsetTable map[primitive.MessageQueue]TopicOffset
	RemotingSerializable
}

func (topicStatsTable *TopicStatsTable) Decode(data string) error {
	res := gjson.Parse(data)
	offsetTableStr := res.Get("offsetTable").String()

	offsetTable := make(map[primitive.MessageQueue]TopicOffset)

	trimStr := offsetTableStr[2 : len(offsetTableStr)-1]

	split := strings.Split(trimStr, ",{")

	var err error

	for _, v := range split {
		tuple := strings.Split(v, "}:")

		queueStr := "{" + tuple[0] + "}"

		var queue primitive.MessageQueue
		err = json.Unmarshal([]byte(queueStr), &queue)

		var topicOffset TopicOffset
		err = json.Unmarshal([]byte(tuple[1]), &topicOffset)
		offsetTable[queue] = topicOffset
	}
	topicStatsTable.OffsetTable = offsetTable
	return err
}

type OffsetWrapper struct {
	BrokerOffset   int64
	ConsumerOffset int64
	LastTimestamp  int64
}

type ConsumeStats struct {
	OffsetTable     map[primitive.MessageQueue]OffsetWrapper
	ConsumeTps      float64
	ConsumeByteRate float64
	RemotingSerializable
}

func (consumeStats *ConsumeStats) Decode(data string) error {
	res := gjson.Parse(data)
	consumeStats.ConsumeByteRate = res.Get("consumeByteRate").Float()
	consumeStats.ConsumeTps = res.Get("consumeTps").Float()

	offsetTableStr := res.Get("offsetTable").String()

	offsetTable := make(map[primitive.MessageQueue]OffsetWrapper)

	trimStr := offsetTableStr[2 : len(offsetTableStr)-1]

	split := strings.Split(trimStr, ",{")

	var err error

	for _, v := range split {
		tuple := strings.Split(v, "}:")

		queueStr := "{" + tuple[0] + "}"

		var queue primitive.MessageQueue
		err = json.Unmarshal([]byte(queueStr), &queue)

		var offsetWrapper OffsetWrapper
		err = json.Unmarshal([]byte(tuple[1]), &offsetWrapper)
		offsetTable[queue] = offsetWrapper
	}
	consumeStats.OffsetTable = offsetTable
	return err
}

type ConsumerConnection struct {
	ConnectionSet     []Connection
	SubscriptionTable map[string]internal.SubscriptionData
	ConsumeType       consumer.ConsumeType
	MessageModel      string
	ConsumeFromWhere  string
	RemotingSerializable
}

type Connection struct {
	ClientId   string
	ClientAddr string
	Language   string
	Version    int64
}
