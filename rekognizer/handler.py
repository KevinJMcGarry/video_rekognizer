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
        })

def start_processing_video(event, context):
    # event is a dictionary which tells us about the event
    for record in event['Records']:  # the record key contains one or more lists
        start_label_detection(
            record['s3']['bucket']['name'],
            # the unqote method removes the plus signs placed in the spaces in the s3 object key/file name
            urllib.parse.unquote_plus(record['s3']['object']['key'])
        )

    return


