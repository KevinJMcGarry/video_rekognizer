import boto3
import pathlib
import urllib

session = boto3.Session(profile_name='eureka-terraform', region_name='us-east-2')

# rekognition requires an s3 bucket to store images and videos
s3 = session.resource('s3')
# bucket = s3.create_bucket(Bucket='rekognizer-kevin',
#                          CreateBucketConfiguration={'LocationConstraint': 'us-east-2'}
#                          )

# if bucket already exists
bucket = s3.Bucket('rekognizer-kevin')

# upload a file
pathname = '~/Downloads/Blurry Video of People Working.mp4'

# make sure to get the absolute path (see webinator for more details)
path = pathlib.Path(pathname).expanduser().resolve()

# upload file to s3, path to the source file and destination file name
# path.name method extracts just the name portion from the full path
bucket.upload_file(str(path), str(path.name))

rekognition_client = session.client('rekognition')

# use label detection method to detect all labels recognition assigns to the image/video
response = rekognition_client.start_label_detection(Video={'S3Object': {'Bucket': bucket.name,'Name': path.name}})

# the response object contains a JobId (attribute/key) that will be used next to get all the detected labels
job_id = response['JobId']
result = rekognition_client.get_label_detection(JobId=job_id)

# pprint.pprint(result)