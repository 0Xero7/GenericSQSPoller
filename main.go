package genericsqspoller

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func handleMessage(m types.Message, wg *sync.WaitGroup, queueUrl string, sqsClient *sqs.Client, messageHandler func(m map[string]any) error) {
	defer wg.Done()

	data := new(map[string]any)
	err := json.Unmarshal([]byte(*m.Body), data)
	if err != nil {
		fmt.Println("Errored!", err)
		return
	}

	fmt.Println("Processing", data)
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

func Start(context context.Context, sqsClient *sqs.Client, queueDetails *sqs.ReceiveMessageInput, messageHandler func(m map[string]any) error) {
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
			go handleMessage(v, &wg, *queueDetails.QueueUrl, sqsClient, handler)
		}

		wg.Wait()
	}
}
