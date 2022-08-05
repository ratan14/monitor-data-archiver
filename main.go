package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const FILE_DURATION = time.Duration(5 * time.Minute)
const BUCKET_NAME = "lumi-monitor-data"

type Event struct {
	Name string `json:"name"`
}

type MonitorData struct {
	MonitorId string                 `json:"monitorId"`
	Timestamp string                 `json:"timestamp"`
	OrgId     string                 `json:"orgId"`
	Values    map[string]interface{} `json:"values"`
}

type Entry struct {
	Timestamp string                 `json:"timestamp"`
	Values    map[string]interface{} `json:"monitorId"`
}

type CompiledMonitorData struct {
	MonitorId string  `json:"monitorId"`
	OrgId     string  `json:"orgId"`
	StartTime string  `json:"startTime"`
	Entries   []Entry `json:"entries"`
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
	s3Client := s3.NewFromConfig(cfg)
	dynamoClient := dynamodb.NewFromConfig(cfg)

	allMonitorData, err := fetchAllMonitorData(dynamoClient)
	monitorDataMap := map[string][]MonitorData{}

	for _, data := range allMonitorData {
		monitorDataMap[data.MonitorId] = append(monitorDataMap[data.MonitorId], data)
	}

	//for each entry in the monitorDataMap, start a new thread for data compiling
	var wg sync.WaitGroup
	for _, dataArray := range monitorDataMap {
		wg.Add(1)
		go compileMonitorData(&wg, dataArray, s3Client)
	}
	wg.Wait()

	// fmt.Println("all monitor data ", monitorDataMap)

	return fmt.Sprintf("Hello %s", event.Name), nil
}

func fetchAllMonitorData(client *dynamodb.Client) ([]MonitorData, error) {
	expr, err := expression.NewBuilder().WithFilter(
		expression.LessThan(expression.Name("Timestamp"), expression.Value(time.Now().UTC().Format(time.RFC3339))),
	).Build()
	if err != nil {
		return nil, err
	}
	out, err := client.Scan(context.Background(), &dynamodb.ScanInput{
		TableName:                 aws.String("Lumi-Monitoring-Logs"),
		FilterExpression:          expr.Filter(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		Limit:                     aws.Int32(1000),
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

func compileMonitorData(wg *sync.WaitGroup, dataArray []MonitorData, client *s3.Client) {
	/*
		1. Sort the array ascendingly with timestamp.
		2. Segregate the data in 5 minute chunks.
		3. Compile into one json, and store in s3.
	*/
	defer wg.Done()

	sort.Slice(dataArray, func(i, j int) bool {
		timestampI, err := time.Parse(time.RFC3339, dataArray[i].Timestamp)
		if err != nil {
			//Todo, create error behaviour for one timestamp fail.
		}
		timestampJ, err := time.Parse(time.RFC3339, dataArray[j].Timestamp)
		if err != nil {
			//Todo, create error behaviour for one timestamp fail.
		}
		return timestampI.Before(timestampJ)
	})

	//get the first timestamp and start with the rounded off 5 minute mark just before it. Run a loop for every 5 minutes until the last timestamp creating files.
	firstTimestamp, _ := time.Parse(time.RFC3339, dataArray[0].Timestamp)
	lastTimestamp, _ := time.Parse(time.RFC3339, dataArray[len(dataArray)-1].Timestamp)

	roundedDownStartTime := firstTimestamp.Round(FILE_DURATION)
	if roundedDownStartTime.After(firstTimestamp) {
		roundedDownStartTime = roundedDownStartTime.Add(-FILE_DURATION)
	}
	roundedUpEndTime := lastTimestamp.Round(FILE_DURATION)
	if roundedUpEndTime.Before(lastTimestamp) {
		roundedUpEndTime = roundedUpEndTime.Add(FILE_DURATION)
	}

	splitTime := roundedDownStartTime.Add(FILE_DURATION)

	var fileWg sync.WaitGroup
	for !splitTime.After(roundedUpEndTime) {
		//For each 5 minute time slot, seprate data and send for file creation
		slotStartTime := splitTime.Add(-FILE_DURATION)
		splitDataArray := []MonitorData{}
		for _, data := range dataArray {
			currentTimestamp, _ := time.Parse(time.RFC3339, data.Timestamp)
			if currentTimestamp.After(slotStartTime) && currentTimestamp.Before(splitTime) {
				splitDataArray = append(splitDataArray, data)
			}
		}
		fileWg.Add(1)
		go compileAndStoreinS3(&fileWg, splitDataArray, slotStartTime, client)

		splitTime = splitTime.Add(FILE_DURATION)
	}
	fileWg.Wait()

	fmt.Println("start time", roundedDownStartTime, "endtime", roundedUpEndTime)
}

func compileAndStoreinS3(fileWg *sync.WaitGroup, splitDataArray []MonitorData, slotStartTime time.Time, client *s3.Client) {
	defer fileWg.Done()

	if len(splitDataArray) == 0 {
		return
	}

	orgId := splitDataArray[0].OrgId
	monitorId := splitDataArray[0].MonitorId

	entries := []Entry{}

	for _, data := range splitDataArray {
		entries = append(entries, Entry{
			Timestamp: data.Timestamp,
			Values:    data.Values,
		})
	}

	compileMonitorData := CompiledMonitorData{
		MonitorId: monitorId,
		OrgId:     orgId,
		StartTime: slotStartTime.Format(time.RFC3339),
		Entries:   entries,
	}

	/*Upload the manifest file to S3*/
	manifestJson, _ := json.MarshalIndent(compileMonitorData, "", " ")
	filename := orgId + "/" + monitorId + "/" + slotStartTime.Format(time.RFC3339) + "-data.json"
	reader := bytes.NewReader(manifestJson)
	input := &s3.PutObjectInput{
		Bucket: aws.String(BUCKET_NAME),
		Key:    aws.String(filename),
		Body:   reader,
	}
	_, err := client.PutObject(context.TODO(), input)
	if err != nil {
		log.Println("Got error uploading file:", err)
		return
	}

	log.Println("Archived Data for orgId=", orgId, "monitorId=", monitorId, "start-time=", slotStartTime)
}
