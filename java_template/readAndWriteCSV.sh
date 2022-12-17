#!/bin/bash
# JSON object to pass to Lambda Function
json={"\"bucketname\"":\"term-project-transformed\"","\"filename\"":\"sales.csv\"","\"transformName\"":\"transformSales.csv\"}

echo "Invoking Lambda function using API Gateway"
time output=`curl -s -H "Content-Type: application/json" -X POST -d $json {https://j52bn6f2yd.execute-api.us-east-2.amazonaws.com/term_project_transform}`
echo ""

echo ""
echo "JSON RESULT:"
echo $output | jq
echo ""

