import json
import boto3

firehose = boto3.client('firehose')
deliveryStreamName = 'PUT-RED-8o70r'
def convertToFirehoseRecord(ddbRecord):
    newImage = ddbRecord['NewImage']
    return (
        f"{newImage['ID']['S']},{newImage['Name']['S']},{newImage['City']['S']},{newImage['Email']['S']},{newImage['Designation']['S']},{newImage['PhoneNumber']['S']}"
        + '\n'
    )
def lambda_handler(event, context):
    print(event)
    for record in event['Records']:
        print(record)
        ddbRecord = record['dynamodb']
        print(f'DDB Record: {json.dumps(ddbRecord)}')

        firehoseRecord = convertToFirehoseRecord(ddbRecord)
        print(f'Firehose Record: {firehoseRecord}')

        result = firehose.put_record(DeliveryStreamName=deliveryStreamName, Record={ 'Data': firehoseRecord})
        print(result)
    return f"processed {len(event['Records'])} records."
