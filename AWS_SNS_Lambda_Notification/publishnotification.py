import boto3

topic_arn = ""
def send_sns(message, subject):
    try:
        client = boto3.client("sns")
        result = client.publish(TopicArn=topic_arn, Message=message, Subject=subject)
        if result['ResponseMetadata']['HTTPStatusCode'] == 200:
            print(result)
            print("Notification send successfully..!!!")
            return True
    except Exception as e:
        print("Error occured while publish notifications and error is : ", e)
        return True

def lambda_handler(event, context):
    print(f"event collected is {event}")
    subject = "Processes completion Notification"
    for record in event['Records']:
        s3_bucket = record['s3']['bucket']['name']
        print(f"Bucket name is {s3_bucket}")
        s3_key = record['s3']['object']['key']
        print(f"Bucket key name is {s3_key}")
        from_path = f"s3://{s3_bucket}/{s3_key}"
        print(f"from path {from_path}")
        message = f"The file is uploaded at S3 bucket path {from_path}"
        if SNSResult := send_sns(message, subject):
            print("Notification Sent..")
            return SNSResult
        else:
            return False