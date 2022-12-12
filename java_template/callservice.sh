#!/bin/bash 
# JSON object to pass to Lambda Function 
json={"\"row\"":50,"\"col\"":10,"\"bucketname\"":\"test.bucket.462-562.f22.rmonto6\"","\"filename\"":\"mytest.db\"""\"sqlbucketname\"":\"test.bucket.462-562.f22.rmonto6\""}
echo "Invoking Lambda function using API Gateway"
time output=`curl -s -H "Content-Type: application/json" -X POST -d $json {https://67stkog3wa.execute-api.us-east-2.amazonaws.com/CreateCSV}`
echo “”
echo ""
echo "JSON RESULT:"
echo $output | jq
echo ""
