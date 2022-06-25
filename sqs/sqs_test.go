package sqs

import (
	"fmt"
	"log"
	"testing"
)

func testSendMessage(queueName string, messageGroupId string, message string, sqs *SQS) {
	resp, err := sqs.SendMessageWithFIFO(queueName, messageGroupId, message, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Sent message with ID: " + *resp.MessageId)
}

func testDeleteMessage(queueName string, receiptHandle string, sqs *SQS) {
	_, err := sqs.DeleteMessage(queueName, receiptHandle)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Deleted message success")
}

func testGetMessages(queueName string, maxNumberOfMessages int32, visibilityTimeout, waitTimeSeconds int32, sqs *SQS) {
	resp, err := sqs.GetMessages(queueName, maxNumberOfMessages, visibilityTimeout, waitTimeSeconds)
	if err != nil {
		log.Fatal(err)
	}
	for _, msg := range resp.Messages {
		log.Println("Message IDs:    ")
		log.Println(*msg.MessageId)
		log.Println("Message Attributes:    ")
		log.Println(msg.MessageAttributes)
		log.Println("Message Body:    ")
		log.Println(*msg.Body)
		log.Println(*msg.ReceiptHandle)
	}
}

func TestSQS(t *testing.T) {
	queueName := "test.fifo"

	// init
	sqs, err := NewSQS("us-east-2", "{key}", "{secret}", "{session}")
	if err != nil {
		t.Error(err)
	}

	// list queue
	listQueuesOutput, err := sqs.GetQueues()
	if err != nil {
		t.Error(err)
	}
	log.Println(listQueuesOutput.QueueUrls)

	// send message
	testSendMessage(queueName, "default", "t1", sqs)
	testSendMessage(queueName, "default", "t2", sqs)
	testSendMessage(queueName, "default", "t3", sqs)

	// get message
	testGetMessages(queueName, 10, 10, 10, sqs)

	// delete message
	testDeleteMessage(queueName, "AQEB96X9hsELpSV+mwX2Qxzlg0z1G2ueB/7t9bs3qvbGeL1okWwWukHYn/FW4VUJIcYVT4SZXzpKfLOP4Zd4ORjohMX2YwURhq4xCFdQg4jF7h2qOKJPJ2htX0X5OHhvZfKoKqBGZ1whSjPHb+1GEXGCrgdWOl2VTgD4DFsueMellq0bx1k5aRvYQtdBxPts0kQODscj72kueuwByzlhAVOyaPc3TMCjN8ra/EvS45eBjMrAiEqo078e8cI9aDhmT76X4Ha2OmpsK4m3k0BP6nrwdgOqRl4HSMrhZAniivH7nSZ4e6K7JZUejMIIphe5VKeMG2BkTdzhYFPbQpAPxTn27xXNWIM6nvp3RnJKgpgCB25gkPzGQewOvBDQRMrj5Qm0", sqs)
}
