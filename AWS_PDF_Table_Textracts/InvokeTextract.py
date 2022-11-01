from logging import exception
import boto3
import time
from botocore.exceptions import ClientError

SNSTopicArn = "arn:aws:sns:us-east-1:357171621133:AmazonTextractTopic1664247109070"
roleArn = "arn:aws:iam::357171621133:role/AWS_PDF_Table_Textract_Role"
textract = boto3.client('textract', region_name='us-east-1')

def TagS3ObjectWithJobId(s3_bucket, s3_key, JobId):
    try:
        s3_client = boto3.client('s3')
        put_tags_response = s3_client.put_object_tagging(
                                Bucket=s3_bucket,
                                Key=s3_key,    
                                Tagging={
                                    'TagSet': [
                                        {
                                            'Key': 'TableExtractJobId',
                                            'Value': JobId
                                        },
                                    ]
                                }
                            )
        if put_tags_response['ResponseMetadata']['HTTPStatusCode'] == 200:
            print("Successfully tagged..")
            return True
        else:
            print("Tagging failed..")
            return False
    except Exception as exception :
        print("Exception happend message is: ", exception)
        return False
def ProcessDocument(s3_bucket, s3_key):
    sleepy_time = 1
    retry = 0
    flag = 'False'
    try:
        while retry < 4 and  flag == 'False' :
            response = textract.start_document_analysis(DocumentLocation={'S3Object': {'Bucket': s3_bucket, 'Name': s3_key}},
                                                FeatureTypes=["TABLES", "FORMS"],
                                                NotificationChannel={'RoleArn': roleArn, 'SNSTopicArn': SNSTopicArn})
            print(response)
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                print('Start Job Id: ' + response['JobId'])
                flag == 'True'
                return response['JobId']
            else:
                time_to_sleep = 2**retry
                retry +=1
                time.sleep(time_to_sleep)
    except Exception as exception :
        print("Exception happend message is: ", exception)
        return False
def lambda_handler(event, context):
    print(f"event collected is {event}")
    for record in event['Records']:
        s3_bucket = record['s3']['bucket']['name']
        print(f"Bucket name is {s3_bucket}")
        s3_key = record['s3']['object']['key']
        print(f"Bucket key name is {s3_key}")
        from_path = f"s3://{s3_bucket}/{s3_key}"
        print(f"from path {from_path}")
        if TextractResult := ProcessDocument(s3_bucket, s3_key):
            print("job id returned..")
            if TagResults := TagS3ObjectWithJobId(
                s3_bucket, s3_key, TextractResult
            ):
                print("Tagging successfully completed")
                return TextractResult
            else:
                return False
        else:
            return False
