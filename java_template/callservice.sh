#!/bin/bash 
# JSON object to pass to Lambda Function 
json={"\"row\"":50,"\"col\"":10,"\"bucketname\"":\"term-project-sales-bucket\"","\"filename\"":\"SalesRecords.csv\""}
echo "Invoking Lambda function using API Gateway"
time output=`curl -s -H "Content-Type: application/json" -X POST -d $json {https://6rgztqwloe.execute-api.us-east-2.amazonaws.com/project-load}`
echo “”
echo ""
echo "JSON RESULT:"
echo $output | jq
echo ""
