#!/bin/bash 
# JSON object to pass to Lambda Function 
json={"\"row\"":50,"\"col\"":10,"\"bucketname\"":\"term-project-bucket-462\"","\"filename\"":\"mytest.db\""}
echo "Invoking Lambda function using API Gateway"
time output=`curl -s -H "Content-Type: application/json" -X POST -d $json {https://jtudcq77r2.execute-api.us-east-2.amazonaws.com/query}`
echo “”
echo ""
echo "JSON RESULT:"
echo $output | jq
echo ""
