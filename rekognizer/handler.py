import json
import os
import urllib

import boto3


def start_label_detection(bucket, key):
    rekognition_client = boto3.client('rekognition')
    response = rekognition_client.start_label_detection(
        Video={
            'S3Object': {
                'Bucket': bucket,
                'Name': key
            }
        },
        # Define sns topic to publish to
        # SNSTopic and RoleArn are defined as environment variables in serverless.yml
        NotificationChannel={
            'SNSTopicArn': os.environ['REKOGNITION_SNS_TOPIC_ARN'],
            'RoleArn': os.environ['REKOGNITION_ROLE_ARN']
        })

def start_processing_video(event, context):
    # event is a dictionary which tells us about the event
    for record in event['Records']:  # the record key contains one or more lists
        # call start_label_detection function passing bucket name and key name
        start_label_detection(
            record['s3']['bucket']['name'],
            # the unqote method removes the plus signs placed in the spaces in the s3 object key/file name
            urllib.parse.unquote_plus(record['s3']['object']['key'])
        )

    return

def handle_label_detection(event, context):
    print(event)

    return



