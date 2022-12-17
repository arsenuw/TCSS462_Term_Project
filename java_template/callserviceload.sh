#!/bin/bash 
# JSON object to pass to Lambda Function 
json={"\"row\"":50,"\"col\"":10,"\"bucketname\"":\"term-project-transformed\"","\"filename\"":\"transformSales.csv\""}
echo "Invoking Lambda function using API Gateway"
time output=`curl -s -H "Content-Type: application/json" -X POST -d $json {https://hoi2zd5dpk.execute-api.us-east-2.amazonaws.com/load-final}`
echo “”
echo ""
echo "JSON RESULT:"
echo $output | jq
echo ""
