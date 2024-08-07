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
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"strings"
	"sync"
	"time"

	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/internal/remote"
	"github.com/apache/rocketmq-client-go/v2/internal/utils"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/rlog"
)

type Admin interface {
	CreateTopic(ctx context.Context, opts ...OptionCreate) error
	DeleteTopic(ctx context.Context, opts ...OptionDelete) error

	GetAllSubscriptionGroup(ctx context.Context, brokerAddr string, timeoutMillis time.Duration) (*SubscriptionGroupWrapper, error)
	FetchAllTopicList(ctx context.Context) (*TopicList, error)
	//GetBrokerClusterInfo(ctx context.Context) (*remote.RemotingCommand, error)
	FetchPublishMessageQueues(ctx context.Context, topic string) ([]*primitive.MessageQueue, error)
	ExamineTopicRouteInfo(ctx context.Context, topic string) (*internal.TopicRouteData, error)
	ExamineTopicConfig(ctx context.Context, addr string, topic string) (*TopicConfig, error)
	ExamineBrokerClusterInfo() (*ClusterInfo, error)
	CreateSubscriptionGroup(ctx context.Context, opts ...OptionSubscriptionCreate) error
	ViewMessage(offsetMsgId string) (*primitive.MessageExt, error)
	GetBrokerConfig(addr string) (map[string]string, error)
	UpdateBrokerConfig(addr, configKey, configValue string) error
	Close() error
}

// TODO: move outdated context to ctx
type adminOptions struct {
	internal.ClientOptions
}

type AdminOption func(options *adminOptions)

func defaultAdminOptions() *adminOptions {
	opts := &adminOptions{
		ClientOptions: internal.DefaultClientOptions(),
	}
	opts.GroupName = "TOOLS_ADMIN"
	opts.InstanceName = time.Now().String()
	return opts
}

// WithResolver nameserver resolver to fetch nameserver addr
func WithResolver(resolver primitive.NsResolver) AdminOption {
	return func(options *adminOptions) {
		options.Resolver = resolver
	}
}

func WithCredentials(c primitive.Credentials) AdminOption {
	return func(options *adminOptions) {
		options.ClientOptions.Credentials = c
	}
}

// WithNamespace set the namespace of admin
func WithNamespace(namespace string) AdminOption {
	return func(options *adminOptions) {
		options.ClientOptions.Namespace = namespace
	}
}

func WithTls(useTls bool) AdminOption {
	return func(options *adminOptions) {
		options.ClientOptions.RemotingClientConfig.UseTls = useTls
	}
}

type admin struct {
	cli internal.RMQClient

	opts *adminOptions

	closeOnce sync.Once
}

// NewAdmin initialize admin
func NewAdmin(opts ...AdminOption) (*admin, error) {
	defaultOpts := defaultAdminOptions()
	for _, opt := range opts {
		opt(defaultOpts)
	}
	namesrv, err := internal.NewNamesrv(defaultOpts.Resolver, defaultOpts.RemotingClientConfig)
	defaultOpts.Namesrv = namesrv
	if err != nil {
		return nil, err
	}

	cli := internal.GetOrNewRocketMQClient(defaultOpts.ClientOptions, nil)
	if cli == nil {
		return nil, fmt.Errorf("GetOrNewRocketMQClient faild")
	}
	defaultOpts.Namesrv = cli.GetNameSrv()
	//log.Printf("Client: %#v", namesrv.srvs)
	return &admin{
		cli:  cli,
		opts: defaultOpts,
	}, nil
}

func (a *admin) GetAllSubscriptionGroup(ctx context.Context, brokerAddr string, timeoutMillis time.Duration) (*SubscriptionGroupWrapper, error) {
	cmd := remote.NewRemotingCommand(internal.ReqGetAllSubscriptionGroupConfig, nil, nil)
	a.cli.RegisterACL()
	response, err := a.cli.InvokeSync(ctx, brokerAddr, cmd, timeoutMillis)
	if err != nil {
		rlog.Error("Get all group list error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	} else {
		rlog.Info("Get all group list success", map[string]interface{}{})
	}
	var subscriptionGroupWrapper SubscriptionGroupWrapper
	_, err = subscriptionGroupWrapper.Decode(response.Body, &subscriptionGroupWrapper)
	if err != nil {
		rlog.Error("Get all group list decode error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	}
	return &subscriptionGroupWrapper, nil
}

func (a *admin) FetchAllTopicList(ctx context.Context) (*TopicList, error) {
	cmd := remote.NewRemotingCommand(internal.ReqGetAllTopicListFromNameServer, nil, nil)
	response, err := a.cli.InvokeSync(ctx, a.cli.GetNameSrv().AddrList()[0], cmd, 3*time.Second)
	if err != nil {
		rlog.Error("Fetch all topic list error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	} else {
		rlog.Info("Fetch all topic list success", map[string]interface{}{})
	}
	var topicList TopicList
	_, err = topicList.Decode(response.Body, &topicList)
	if err != nil {
		rlog.Error("Fetch all topic list decode error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	}
	return &topicList, nil
}

// CreateTopic create topic.
// TODO: another implementation like sarama, without brokerAddr as input
func (a *admin) CreateTopic(ctx context.Context, opts ...OptionCreate) error {
	cfg := defaultTopicConfigCreate()
	for _, apply := range opts {
		apply(&cfg)
	}

	request := &internal.CreateTopicRequestHeader{
		Topic:           cfg.Topic,
		DefaultTopic:    cfg.DefaultTopic,
		ReadQueueNums:   cfg.ReadQueueNums,
		WriteQueueNums:  cfg.WriteQueueNums,
		Perm:            cfg.Perm,
		TopicFilterType: cfg.TopicFilterType,
		TopicSysFlag:    cfg.TopicSysFlag,
		Order:           cfg.Order,
		Attributes:      cfg.Attributes,
	}

	cmd := remote.NewRemotingCommand(internal.ReqCreateTopic, request, nil)
	_, err := a.cli.InvokeSync(ctx, cfg.BrokerAddr, cmd, 5*time.Second)
	if err != nil {
		rlog.Error("create topic error", map[string]interface{}{
			rlog.LogKeyTopic:         cfg.Topic,
			rlog.LogKeyBroker:        cfg.BrokerAddr,
			rlog.LogKeyUnderlayError: err,
		})
	} else {
		rlog.Info("create topic success", map[string]interface{}{
			rlog.LogKeyTopic:  cfg.Topic,
			rlog.LogKeyBroker: cfg.BrokerAddr,
		})
	}
	return err
}

// DeleteTopicInBroker delete topic in broker.
func (a *admin) deleteTopicInBroker(ctx context.Context, topic string, brokerAddr string) (*remote.RemotingCommand, error) {
	request := &internal.DeleteTopicRequestHeader{
		Topic: topic,
	}

	cmd := remote.NewRemotingCommand(internal.ReqDeleteTopicInBroker, request, nil)
	return a.cli.InvokeSync(ctx, brokerAddr, cmd, 5*time.Second)
}

// DeleteTopicInNameServer delete topic in nameserver.
func (a *admin) deleteTopicInNameServer(ctx context.Context, topic string, nameSrvAddr string) (*remote.RemotingCommand, error) {
	request := &internal.DeleteTopicRequestHeader{
		Topic: topic,
	}

	cmd := remote.NewRemotingCommand(internal.ReqDeleteTopicInNameSrv, request, nil)
	return a.cli.InvokeSync(ctx, nameSrvAddr, cmd, 5*time.Second)
}

// DeleteTopic delete topic in both broker and nameserver.
func (a *admin) DeleteTopic(ctx context.Context, opts ...OptionDelete) error {
	cfg := defaultTopicConfigDelete()
	for _, apply := range opts {
		apply(&cfg)
	}
	//delete topic in broker
	if cfg.BrokerAddr == "" {
		a.cli.GetNameSrv().UpdateTopicRouteInfo(cfg.Topic)
		cfg.BrokerAddr = a.cli.GetNameSrv().FindBrokerAddrByTopic(cfg.Topic)
	}

	if _, err := a.deleteTopicInBroker(ctx, cfg.Topic, cfg.BrokerAddr); err != nil {
		rlog.Error("delete topic in broker error", map[string]interface{}{
			rlog.LogKeyTopic:         cfg.Topic,
			rlog.LogKeyBroker:        cfg.BrokerAddr,
			rlog.LogKeyUnderlayError: err,
		})
		return err
	}

	//delete topic in nameserver
	if len(cfg.NameSrvAddr) == 0 {
		a.cli.GetNameSrv().UpdateTopicRouteInfo(cfg.Topic)
		cfg.NameSrvAddr = a.cli.GetNameSrv().AddrList()
		_, _, err := a.cli.GetNameSrv().UpdateTopicRouteInfo(cfg.Topic)
		if err != nil {
			rlog.Error("delete topic in nameserver error", map[string]interface{}{
				rlog.LogKeyTopic:         cfg.Topic,
				rlog.LogKeyUnderlayError: err,
			})
		}
		cfg.NameSrvAddr = a.cli.GetNameSrv().AddrList()
	}

	for _, nameSrvAddr := range cfg.NameSrvAddr {
		if _, err := a.deleteTopicInNameServer(ctx, cfg.Topic, nameSrvAddr); err != nil {
			rlog.Error("delete topic in nameserver error", map[string]interface{}{
				"nameServer":             nameSrvAddr,
				rlog.LogKeyTopic:         cfg.Topic,
				rlog.LogKeyUnderlayError: err,
			})
			return err
		}
	}
	rlog.Info("delete topic success", map[string]interface{}{
		"nameServer":      cfg.NameSrvAddr,
		rlog.LogKeyTopic:  cfg.Topic,
		rlog.LogKeyBroker: cfg.BrokerAddr,
	})
	return nil
}

func (a *admin) FetchPublishMessageQueues(ctx context.Context, topic string) ([]*primitive.MessageQueue, error) {
	return a.cli.GetNameSrv().FetchPublishMessageQueues(utils.WrapNamespace(a.opts.Namespace, topic))
}

func (a *admin) ExamineTopicRouteInfo(ctx context.Context, topic string) (*internal.TopicRouteData, error) {
	return a.cli.GetNameSrv().QueryTopicRouteInfo(topic)
}

func (a *admin) ExamineTopicConfig(ctx context.Context, addr string, topic string) (*TopicConfig, error) {
	request := &internal.GetTopicConfigRequestHeader{
		Topic: topic,
	}
	cmd := remote.NewRemotingCommand(internal.ReqGetTopicConfig, request, nil)
	response, err := a.cli.InvokeSync(ctx, addr, cmd, 5*time.Second)
	if err != nil {
		return nil, err
	}
	var topicConfig TopicConfig
	_, err = topicConfig.Decode(response.Body, &topicConfig)
	if err != nil {
		return nil, err
	}
	return &topicConfig, nil
}

func (a *admin) ExamineBrokerClusterInfo() (*ClusterInfo, error) {
	var (
		response *remote.RemotingCommand
		err      error
	)

	namesrvs := a.cli.GetNameSrv().AddrList()
	for i := 0; i < len(namesrvs); i++ {
		rc := remote.NewRemotingCommand(internal.ReqGetBrokerClusterInfo, nil, nil)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		response, err = a.cli.InvokeSync(ctx, namesrvs[i], rc, 5*time.Second)

		if err == nil {
			cancel()
			break
		}
		cancel()
	}

	if err != nil {
		return nil, primitive.NewRemotingErr(err.Error())
	}

	var clusterInfo ClusterInfo
	err = clusterInfo.Decode(string(response.Body))
	return &clusterInfo, err
}

func (a *admin) CreateSubscriptionGroup(ctx context.Context, opts ...OptionSubscriptionCreate) error {
	cfg := defaultSubscriptionConfigCreate()
	for _, apply := range opts {
		apply(&cfg)
	}

	var (
		brokerAddr string
		err        error
	)

	data, err := json.Marshal(cfg)

	if err == nil {
		if len(cfg.BrokerAddr) == 0 {
			clusterInfo, e := a.ExamineBrokerClusterInfo()
			if e != nil {
				return e
			}
			for _, brokerAddrTable := range clusterInfo.BrokerAddrTable {
				brokerAddr = brokerAddrTable.BrokerAddresses[internal.MasterId]
				cmd := remote.NewRemotingCommand(internal.ReqCreateSubscriptionGroupConfig, nil, data)
				_, err = a.cli.InvokeSync(ctx, brokerAddr, cmd, 5*time.Second)
				if err == nil {
					rlog.Info("create group success", map[string]interface{}{
						rlog.LogKeyConsumerGroup: cfg.GroupName,
						rlog.LogKeyBroker:        brokerAddr,
					})
				}
			}
		} else {
			cmd := remote.NewRemotingCommand(internal.ReqCreateSubscriptionGroupConfig, nil, data)
			_, err = a.cli.InvokeSync(ctx, cfg.BrokerAddr, cmd, 5*time.Second)
			if err == nil {
				rlog.Info("create group success", map[string]interface{}{
					rlog.LogKeyConsumerGroup: cfg.GroupName,
					rlog.LogKeyBroker:        brokerAddr,
				})
			}
		}
	}

	if err != nil {
		rlog.Error("create group error", map[string]interface{}{
			rlog.LogKeyConsumerGroup: cfg.GroupName,
			rlog.LogKeyBroker:        brokerAddr,
			rlog.LogKeyUnderlayError: err,
		})
	}
	return err
}

func (a *admin) ViewMessage(offsetMsgId string) (*primitive.MessageExt, error) {
	messageID, err := primitive.UnmarshalMsgID([]byte(offsetMsgId))
	if err != nil {
		return nil, err
	}
	request := &internal.ViewMessageRequestHeader{
		Offset: messageID.Offset,
	}

	cmd := remote.NewRemotingCommand(internal.ReqViewMessageByID, request, nil)

	res, err := a.cli.InvokeSync(context.Background(), fmt.Sprintf("%s:%d", messageID.Addr, messageID.Port), cmd, 5*time.Second)

	if err == nil {
		if res.Code == internal.ResSuccess {
			messageExt := primitive.DecodeMessage(res.Body)[0]
			return messageExt, err
		} else {
			err = fmt.Errorf("view message error: CODE:%d, Remark:%s", res.Code, res.Remark)
		}
	}
	return nil, err
}

func (a *admin) GetBrokerConfig(addr string) (map[string]string, error) {
	cmd := remote.NewRemotingCommand(internal.ReqGetBrokerConfig, nil, nil)
	res, err := a.cli.InvokeSync(context.Background(), addr, cmd, 5*time.Second)
	if err != nil {
		return nil, err
	}

	if res.Code != internal.ResSuccess {
		return nil, fmt.Errorf("get broker config response code: %d, remarks: %s", res.Code, res.Remark)
	}

	properties := make(map[string]string)

	scanner := bufio.NewScanner(strings.NewReader(string(res.Body)))
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
			properties[key] = value
		}
	}
	return properties, nil
}

func (a *admin) UpdateBrokerConfig(addr, configKey, configValue string) error {
	if len(configKey) < 0 || len(configValue) < 0 {
		return errors.New("config key or value is not valid")
	}

	configStr := fmt.Sprintf("%s=%s", configKey, configValue)
	cmd := remote.NewRemotingCommand(internal.ReqUpdateBrokerConfig, nil, []byte(configStr))
	res, err := a.cli.InvokeSync(context.Background(), addr, cmd, 5*time.Second)
	if err != nil {
		return err
	}
	if res.Code != internal.ResSuccess {
		return fmt.Errorf("update broker config error, %s", res.Remark)
	}
	return nil
}

func (a *admin) Close() error {
	a.closeOnce.Do(func() {
		a.cli.Shutdown()
	})
	return nil
}
