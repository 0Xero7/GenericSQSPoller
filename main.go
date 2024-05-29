package genericsqspoller

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func handleMessage[T any](m types.Message, wg *sync.WaitGroup, queueUrl string, sqsClient *sqs.Client, messageHandler func(m any) error) {
	defer wg.Done()

	data := new(T)
	err := json.Unmarshal([]byte(*m.Body), data)
	if err != nil {
		fmt.Println("Errored!", err)
		return
	}

	err = messageHandler(data)
	if err != nil {
		fmt.Println(err)
	} else {
		sqsClient.DeleteMessage(context.TODO(), &sqs.DeleteMessageInput{
			QueueUrl:      &queueUrl,
			ReceiptHandle: m.ReceiptHandle,
		})
	}
}

func Start(context context.Context, sqsClient *sqs.Client, queueDetails *sqs.ReceiveMessageInput, messageHandler func(m any) error) {
	for {
		out, err := sqsClient.ReceiveMessage(context, queueDetails)

		if err != nil {
			fmt.Println(err)
		}

		handler := func(m any) error {
			return nil
		}

		wg := sync.WaitGroup{}
		wg.Add(len(out.Messages))
		for _, v := range out.Messages {
			go handleMessage[map[string]any](v, &wg, *queueDetails.QueueUrl, sqsClient, handler)
		}

		wg.Wait()
	}
}
