#!/bin/bash 
# JSON object to pass to Lambda Function 
json={"\"bucketname\"":\"readandwritecsv\"","\"filename\"":\"sales.csv\"","\"transformName\"":\"transformSale.csv\""}
echo "Invoking Lambda function using API Gateway"
time output=`curl -s -H "Content-Type: application/json" -X POST -d $json {https://0shj9wumm1.execute-api.us-east-2.amazonaws.com/Ryan}`
echo “”
echo ""
echo "JSON RESULT:"
echo $output | jq
echo ""
