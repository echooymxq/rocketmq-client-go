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

import "fmt"

func defaultTopicConfigCreate() TopicConfigCreate {
	opts := TopicConfigCreate{
		DefaultTopic:    "defaultTopic",
		ReadQueueNums:   8,
		WriteQueueNums:  8,
		Perm:            6,
		TopicFilterType: "SINGLE_TAG",
		TopicSysFlag:    0,
		Order:           false,
		Attributes:      "",
	}
	return opts
}

type TopicConfigCreate struct {
	Topic           string
	BrokerAddr      string
	DefaultTopic    string
	ReadQueueNums   int
	WriteQueueNums  int
	Perm            int
	TopicFilterType string
	TopicSysFlag    int
	Order           bool
	Attributes      string
}

type OptionCreate func(*TopicConfigCreate)

func WithTopicCreate(Topic string) OptionCreate {
	return func(opts *TopicConfigCreate) {
		opts.Topic = Topic
	}
}

func WithBrokerAddrCreate(BrokerAddr string) OptionCreate {
	return func(opts *TopicConfigCreate) {
		opts.BrokerAddr = BrokerAddr
	}
}

func WithReadQueueNums(ReadQueueNums int) OptionCreate {
	return func(opts *TopicConfigCreate) {
		opts.ReadQueueNums = ReadQueueNums
	}
}

func WithWriteQueueNums(WriteQueueNums int) OptionCreate {
	return func(opts *TopicConfigCreate) {
		opts.WriteQueueNums = WriteQueueNums
	}
}

func WithPerm(Perm int) OptionCreate {
	return func(opts *TopicConfigCreate) {
		opts.Perm = Perm
	}
}

func WithTopicFilterType(TopicFilterType string) OptionCreate {
	return func(opts *TopicConfigCreate) {
		opts.TopicFilterType = TopicFilterType
	}
}

func WithTopicSysFlag(TopicSysFlag int) OptionCreate {
	return func(opts *TopicConfigCreate) {
		opts.TopicSysFlag = TopicSysFlag
	}
}

func WithOrder(Order bool) OptionCreate {
	return func(opts *TopicConfigCreate) {
		opts.Order = Order
	}
}

// WithAttribute +key1=value1,+key2=value2
func WithAttribute(key string, val string) OptionCreate {
	return func(opts *TopicConfigCreate) {
		if len(opts.Attributes) == 0 {
			opts.Attributes = fmt.Sprintf("+%s=%s", key, val)
		} else {
			opts.Attributes = fmt.Sprintf("%s,+%s=%s", opts.Attributes, key, val)
		}
	}
}

func defaultTopicConfigDelete() TopicConfigDelete {
	opts := TopicConfigDelete{}
	return opts
}

type TopicConfigDelete struct {
	Topic       string
	ClusterName string
	NameSrvAddr []string
	BrokerAddr  string
}

type OptionDelete func(*TopicConfigDelete)

func WithTopicDelete(Topic string) OptionDelete {
	return func(opts *TopicConfigDelete) {
		opts.Topic = Topic
	}
}

func WithBrokerAddrDelete(BrokerAddr string) OptionDelete {
	return func(opts *TopicConfigDelete) {
		opts.BrokerAddr = BrokerAddr
	}
}

func WithClusterName(ClusterName string) OptionDelete {
	return func(opts *TopicConfigDelete) {
		opts.ClusterName = ClusterName
	}
}

func WithNameSrvAddr(NameSrvAddr []string) OptionDelete {
	return func(opts *TopicConfigDelete) {
		opts.NameSrvAddr = NameSrvAddr
	}
}

func defaultSubscriptionConfigCreate() SubscriptionConfigCreate {
	opts := SubscriptionConfigCreate{
		RetryQueueNums: 1,
		RetryMaxTimes:  16,
	}
	return opts
}

type SubscriptionConfigCreate struct {
	GroupName      string
	BrokerAddr     string
	RetryQueueNums int
	RetryMaxTimes  int
	Attributes     string
}

type OptionSubscriptionCreate func(*SubscriptionConfigCreate)

func WithGroupName(GroupName string) OptionSubscriptionCreate {
	return func(opts *SubscriptionConfigCreate) {
		opts.GroupName = GroupName
	}
}

func WithBrokerAddr(BrokerAddr string) OptionSubscriptionCreate {
	return func(opts *SubscriptionConfigCreate) {
		opts.BrokerAddr = BrokerAddr
	}
}

func WithRetryQueueNums(RetryQueueNums int) OptionSubscriptionCreate {
	return func(opts *SubscriptionConfigCreate) {
		opts.RetryQueueNums = RetryQueueNums
	}
}

func WithRetryMaxTimes(RetryMaxTimes int) OptionSubscriptionCreate {
	return func(opts *SubscriptionConfigCreate) {
		opts.RetryMaxTimes = RetryMaxTimes
	}
}
