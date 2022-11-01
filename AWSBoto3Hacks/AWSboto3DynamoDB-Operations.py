import boto3
from botocore.exceptions import ClientError
from pprint import pprint
from decimal import Decimal
import time


client = boto3.client('dynamodb')

#Create DynamoDB table
def create_movie_table():
    return client.create_table(
        TableName='Movies',
        KeySchema=[
            {'AttributeName': 'year', 'KeyType': 'HASH'},  # Partition key
            {'AttributeName': 'title', 'KeyType': 'RANGE'},  # Sort key
        ],
        AttributeDefinitions=[
            {'AttributeName': 'year', 'AttributeType': 'N'},
            {'AttributeName': 'title', 'AttributeType': 'S'},
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10,
        },
    )

##Create record in a DynamoDB table
def put_movie(title, year, plot, rating):
    return client.put_item(
        TableName='Movies',
        Item={
            'year': {'N': f"{year}"},
            'title': {'S': f"{title}"},
            'plot': {"S": f"{plot}"},
            'rating': {"N": f"{rating}"},
        },
    )

##Get a record in from DynamoDB table
def get_movie(title, year):
    try:
        response = client.get_item(
            TableName='Movies',
            Key={'year': {'N': f"{year}"}, 'title': {'S': f"{title}"}},
        )

    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response['Item']

## Update a record in DynamoDB table
def update_movie(title, year, rating, plot, actors):
    return client.update_item(
        TableName='Movies',
        Key={'year': {'N': f"{year}"}, 'title': {'S': f"{title}"}},
        ExpressionAttributeNames={
            '#R': 'rating',
            '#P': 'plot',
            '#A': 'actors',
        },
        ExpressionAttributeValues={
            ':r': {'N': f"{rating}"},
            ':p': {'S': f"{plot}"},
            ':a': {
                'SS': actors,
            },
        },
        UpdateExpression='SET #R = :r, #P = :p, #A = :a',
        ReturnValues="UPDATED_NEW",
    )

## Increment an Atomic Counter in DynamoDB table
def increase_rating(title, year, rating_increase):
    return client.update_item(
        TableName='Movies',
        Key={'year': {'N': f"{year}"}, 'title': {'S': f"{title}"}},
        ExpressionAttributeNames={'#R': 'rating'},
        ExpressionAttributeValues={':r': {'N': f"{Decimal(rating_increase)}"}},
        UpdateExpression='SET #R = #R + :r',
        ReturnValues="UPDATED_NEW",
    )

## Delete an Item in DynamoDB table
def delete_underrated_movie(title, year, rating):
    try:
        response = client.delete_item(
            TableName='Movies',
            Key={'year': {'N': f"{year}"}, 'title': {'S': f"{title}"}},
            ConditionExpression="rating <= :a",
            ExpressionAttributeValues={':a': {'N': f"{rating}"}},
        )

    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            print(e.response['Error']['Message'])
        else:
            raise
    else:
        return response 

if __name__ == '__main__':

    ## Create DynamoDB
    movie_table = create_movie_table()
    print("Create DynamoDB succeeded............")
    print(f"Table status:{movie_table}")

    time.sleep(30)

    ## Insert in to DynamoDB
    movie_resp = put_movie("The Big New Movie", 2015,"Nothing happens at all.", 0)
    print("Insert in to DynamoDB succeeded............")
    pprint(movie_resp, sort_dicts=False)


    if movie := get_movie(
        "The Big New Movie",
        2015,
    ):
        print("Get an item from DynamoDB succeeded............")
        pprint(movie, sort_dicts=False)


    ## Update and item in  DynamoDB
    update_response = update_movie( "The Big New Movie", 2015, 5.5, "Everything happens all at once.",["Larry", "Moe", "Curly"])
    print("Update and item in  DynamoDB succeeded............")
    pprint(update_response, sort_dicts=False)


    ## Increment an Atomic Counter in DynamoDB
    update_response = increase_rating("The Big New Movie", 2015, 1)
    print("Increment an Atomic Counter in DynamoDB succeeded............")
    pprint(update_response, sort_dicts=False)


    if delete_response := delete_underrated_movie(
        "The Big New Movie", 2015, 7.5
    ):
        print("Delete an Item in DynamoDB table.........................")
        pprint(delete_response, sort_dicts=False)
