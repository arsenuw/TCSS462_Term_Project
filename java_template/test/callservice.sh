#!/bin/bash
# JSON object to pass to Lambda Function
json={"\"row\"":50,"\"col\"":10,"\"bucketname\"":\"test.bucket.462-562.f22.as\"","\"filename\"":\"test.csv\""}
echo "Invoking Lambda function using API Gateway"
time output=`curl -s -H "Content-Type: application/json" -X POST -d $json {https://dhysmom861.execute-api.us-east-2.amazonaws.com/tcss462_tutorial5_dev}`
echo “”
echo ""
echo "JSON RESULT:"
echo $output | jq
echo ""
#echo "Invoking Lambda function using AWS CLI"
#time output=`aws lambda invoke --invocation-type RequestResponse --function-name arsensh_tutorial5 --region us-east-2 --payload $json /dev/stdout | head -n 1 | head -c -2 ; echo`
#echo ""
#echo "JSON RESULT:"
#echo $output | jq
#echo ""
