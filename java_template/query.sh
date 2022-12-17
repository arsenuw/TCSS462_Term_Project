#!/bin/bash
# JSON object to pass to Lambda Function
json={"\"bucketname\"":\"readandwritecsv\"","\"filename\"":\"mytest.db\""}

echo "Invoking Lambda function using API Gateway"
time output=`curl -s -H "Content-Type: application/json" -X POST -d $json https://ye7xihkpv8.execute-api.us-east-2.amazonaws.com/load/`
echo ""

echo ""
echo "JSON RESULT:"
echo $output | jq
echo ""

