import json
import psycopg2
import os

def lambda_handler(event, context):
    print(f"event collected is {event}")
    for record in event['Records']:
        s3_bucket = record['s3']['bucket']['name']
        print(f"Bucket name is {s3_bucket}")
        s3_key = record['s3']['object']['key']
        print(f"Bucket key name is {s3_key}")
        from_path = f"s3://{s3_bucket}/{s3_key}"
        print(f"from path {from_path}")
        Access_key = os.getenv('AWS_Access_key')
        Access_Secrete = os.getenv('AWS_Access_Secrete')
        dbname = os.getenv('dbname')
        host = os.getenv('host')
        user = os.getenv('user')
        password = os.getenv('password')
        tablename = os.getenv('tablename')
        connection = psycopg2.connect(dbname = dbname,
                                       host = host,
                                       port = '5439',
                                       user = user,
                                       password = password)

        print('after connection....')
        curs = connection.cursor()
        print('after cursor....')
        querry = f"COPY {tablename} FROM '{from_path}' CREDENTIALS 'aws_access_key_id={Access_key};aws_secret_access_key={Access_Secrete}' CSV;"

        print(f"query is {querry}")
        print('after querry....')
        curs.execute(querry)
        connection.commit()
        #print(curs.fetchmany(3))
        print('after execute....')
        curs.close()
        print('after curs close....')
        connection.close()
        print('after connection close....')
        print('wow..executed....')
