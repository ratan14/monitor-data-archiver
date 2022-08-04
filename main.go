package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type Event struct {
	Name string `json:"name"`
}

type MonitorData struct {
	MonitorId string                 `json:"monitorId"`
	TimeStamp string                 `json:"timestamp"`
	OrgId     string                 `json:"orgId"`
	Values    map[string]interface{} `json:"values"`
}

func main() {
	lambda.Start(HandleRequest)
}

/** Steps:
1. Fetch all monitor data from dynamo starting 24 hours ago and going backwards.
2. Separate into different monitors.
3. Run a data compile thread on each monitor data which does the following:
	a. Make files compiling all the data for each 5 minute chunk.
	b. Store files into S3.
*/

func HandleRequest(ctx context.Context, event Event) (string, error) {

	log.Println("Starting Monitor Data Archive")

	/*Initiate AWS Client using config*/
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("eu-west-2"))
	if err != nil {
		log.Fatalf("unable to load SDK config:, %v", err)
	}
	// s3Client := s3.NewFromConfig(cfg)
	dynamoClient := dynamodb.NewFromConfig(cfg)

	allMonitorData, err := fetchAllMonitorData(dynamoClient)

	fmt.Println("all monitor data ", allMonitorData)

	return fmt.Sprintf("Hello %s", event.Name), nil
}

func fetchAllMonitorData(client *dynamodb.Client) ([]MonitorData, error) {
	expr, err := expression.NewBuilder().WithFilter(
		expression.LessThan(expression.Name("Timestamp"), expression.Value(time.Now().Add(time.Hour*-240).UTC().Format(time.RFC3339))),
	).Build()
	if err != nil {
		return nil, err
	}
	out, err := client.Scan(context.Background(), &dynamodb.ScanInput{
		TableName:                 aws.String("Lumi-Monitoring-Logs"),
		FilterExpression:          expr.Filter(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		Limit:                     aws.Int32(10),
	})
	if err != nil {
		return nil, err
	}

	result := []MonitorData{}
	for _, item := range out.Items {
		monitorData := MonitorData{}
		err = attributevalue.UnmarshalMap(item, &monitorData)
		if err != nil {
			return nil, err
		}
		result = append(result, monitorData)
	}
	return result, nil
}
