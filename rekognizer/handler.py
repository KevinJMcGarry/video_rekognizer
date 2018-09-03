import json
import os
import urllib

import boto3


# Helper functions
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


def get_video_labels(job_id):
    rekognition_client = boto3.client('rekognition')

    # get labels for the specific job
    response = rekognition_client.get_label_detection(JobId=job_id)

    next_token = response.get('NextToken', None)  # this is used if more than 1000 labels are returned, otherwise None

    while next_token:  # if there is a next_token object that has been created (while there are more labels to get)
        next_page = rekognition_client.get_label_detection(
            JobId=job_id,
            NextToken=next_token
        )

        next_token = next_page.get('NextToken', None)

        # extending list of labels for the response
        response['Labels'].extend(next_page['Labels'])

    return response


# function that checks the type of the data (dict, list, float) and uses

def make_item(data):
    """if we receive a dictionary, return a dictionary comprehension where we call make_item on each value
    inside the dictionary
    also if we receive a list, return a list comprehension where we call make_item on each value inside
    the list"""
    if isinstance(data, dict):
        return {k: make_item(v) for k, v in data.items()}

    if isinstance(data, list):
        return [make_item(v) for v in data]

    if isinstance(data, float):  # if data type is float, return the data as a string
        return str(data)

    return data


# data argument is JobId from get_video_labels function
# video_name and video_bucket arguments come from handle_label_detection function
def put_labels_in_db(data, video_name, video_bucket):
    del data['ResponseMetadata']  # field data we don't need
    del data['JobStatus']  # field data we don't need

    # these two pieces of data we didn't get back from the handle_label_detection call
    # we'll need these to associate the dynamodb record back to the original video uploaded to s3
    # so the values being passed in are added to the data list
    data['videoName'] = video_name
    data['videoBucket'] = video_bucket

    dynamodb = boto3.resource('dynamodb')
    table_name = os.environ['DYNAMODB_TABLE_NAME']
    videos_table = dynamodb.Table(table_name)

    # perform conversion on entire data structure
    # use make_item to convert float values found in the data to strings
    # this is to solve for the different ways python and dynamodb handle floats
    data = make_item(data)

    videos_table.put_item(Item=data)

    return


# Lambda Event functions

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
    for record in event['Records']:  # event payload could contain multiple records
        # Sns[Message] is a string that contains json structured data. need to convert to python dict
        message = json.loads(record['Sns']['Message'])
        job_id = message['JobId']
        s3_object = message['Video']['S3ObjectName']
        s3_bucket = message['Video']['S3Bucket']

        response = get_video_labels(job_id)  # get video labels from Rekognition
        put_labels_in_db(response, s3_object, s3_bucket)  # insert labels into dynamodb

    return


