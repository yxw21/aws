package sqs

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type SQS struct {
	client *sqs.Client
}

func (ctx *SQS) GetQueueURL(queueName string) (*sqs.GetQueueUrlOutput, error) {
	qInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	return ctx.client.GetQueueUrl(context.TODO(), qInput)
}

func (ctx *SQS) SendMessageWithFIFO(queueName, messageGroupId, message string, messageAttributes map[string]types.MessageAttributeValue) (*sqs.SendMessageOutput, error) {
	getQueueUrlOutput, err := ctx.GetQueueURL(queueName)
	if err != nil {
		return nil, err
	}
	sendMessageInput := &sqs.SendMessageInput{
		QueueUrl:          getQueueUrlOutput.QueueUrl,
		MessageGroupId:    &messageGroupId,
		MessageBody:       aws.String(message),
		MessageAttributes: messageAttributes,
	}
	return ctx.client.SendMessage(context.TODO(), sendMessageInput)
}

func (ctx *SQS) SendMessage(queueName, message string, delaySeconds int32, messageAttributes map[string]types.MessageAttributeValue) (*sqs.SendMessageOutput, error) {
	getQueueUrlOutput, err := ctx.GetQueueURL(queueName)
	if err != nil {
		return nil, err
	}
	sendMessageInput := &sqs.SendMessageInput{
		QueueUrl:          getQueueUrlOutput.QueueUrl,
		MessageBody:       aws.String(message),
		DelaySeconds:      delaySeconds,
		MessageAttributes: messageAttributes,
	}
	return ctx.client.SendMessage(context.TODO(), sendMessageInput)
}

func (ctx *SQS) DeleteMessage(queueName, receiptHandle string) (*sqs.DeleteMessageOutput, error) {
	getQueueUrlOutput, err := ctx.GetQueueURL(queueName)
	if err != nil {
		return nil, err
	}
	deleteMessageInput := &sqs.DeleteMessageInput{
		QueueUrl:      getQueueUrlOutput.QueueUrl,
		ReceiptHandle: &receiptHandle,
	}
	return ctx.client.DeleteMessage(context.TODO(), deleteMessageInput)
}

func (ctx *SQS) GetQueues() (*sqs.ListQueuesOutput, error) {
	listQueuesInput := &sqs.ListQueuesInput{}
	return ctx.client.ListQueues(context.TODO(), listQueuesInput)
}

func (ctx *SQS) GetMessages(queueName string, maxNumberOfMessages int32, visibilityTimeout, waitTimeSeconds int32) (*sqs.ReceiveMessageOutput, error) {
	getQueueUrlOutput, err := ctx.GetQueueURL(queueName)
	if err != nil {
		return nil, err
	}
	receiveMessageInput := &sqs.ReceiveMessageInput{
		QueueUrl: getQueueUrlOutput.QueueUrl,
		AttributeNames: []types.QueueAttributeName{
			"All",
		},
		MaxNumberOfMessages: maxNumberOfMessages,
		MessageAttributeNames: []string{
			"All",
		},
		VisibilityTimeout: visibilityTimeout,
		WaitTimeSeconds:   waitTimeSeconds,
	}
	return ctx.client.ReceiveMessage(context.TODO(), receiveMessageInput)
}

func (ctx *SQS) GetClient() *sqs.Client {
	return ctx.client
}

func NewSQS(region, key, secret, session string) (*SQS, error) {
	if region == "" {
		return nil, errors.New("region cannot be empty")
	}
	if key == "" {
		return nil, errors.New("key cannot be empty")
	}
	if secret == "" {
		return nil, errors.New("secret cannot be empty")
	}
	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(key, secret, session)),
	)
	if err != nil {
		return nil, err
	}
	return &SQS{
		client: sqs.NewFromConfig(cfg),
	}, nil
}
