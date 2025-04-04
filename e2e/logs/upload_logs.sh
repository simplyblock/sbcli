#!/bin/bash

sudo yum install -y unzip

if [ ! -f "awscliv2.zip" ]; then
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
    unzip -q awscliv2.zip
    sudo ./aws/install
else
    echo "awscli already exists."
fi


AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
AWS_DEFAULT_REGION=$AWS_REGION
S3_BUCKET=$S3_BUCKET_NAME

# Configure AWS CLI with provided environment variables
aws configure set aws_access_key_id $AWS_ACCESS_KEY_ID
aws configure set aws_secret_access_key $AWS_SECRET_ACCESS_KEY
aws configure set default.region $AWS_DEFAULT_REGION
aws configure set default.output json

LOCAL_LOGS_DIR="$RUN_ID"

for file in *.log; do
    if [ -f "$file" ] && [ -s "$file" ]; then
        filename=$(basename "$file")
        aws s3 cp "$file" "s3://$S3_BUCKET/$LOCAL_LOGS_DIR/$filename"
    fi
done

rm -rf "*.log"

echo "Done uploading test log files to S3 with run ID: ${RUN_ID}"
