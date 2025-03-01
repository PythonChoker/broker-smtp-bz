import json
from configparser import ConfigParser
import mysql.connector
import pika
import requests


# Loading config
config = ConfigParser()
config.read('config.ini')

def callback(ch, method, properties, body):
    body_dict = json.loads(body)

    # Connecting the database
    db = mysql.connector.connect(
        host=config.get(section='DATABASE', option='host'),
        port=config.getint(section='DATABASE', option='port'),
        database=config.get(section='DATABASE', option='database'),
        user=config.get(section='DATABASE', option='user'),
        password=config.get(section='DATABASE', option='password'),
    )
    cursor = db.cursor()

    # Loading an integration
    cursor.execute(
        operation='SELECT subject, email_from, email_to, api_key FROM emails_bz WHERE id=%(id)s LIMIT 1;',
        params={'id': body_dict['integration_id']}
    )

    record = cursor.fetchone()
    db.close()
    if record is not None:
        data = {
            'subject': record[0],
            'from': record[1],
            'to': record[2],
            'html': body_dict['text'],
        }

        if len(body_dict['attachments']) > 0:
            data['files'] = json.dumps(tuple(
                {'name': file['name'], 'body': file['body']} for file in body_dict['attachments']
            ))

        response = requests.post(
            url='https://api.smtp.bz/v1/smtp/send',
            data=data,
            headers={'Authorization': record[3]}
        )

        print(f'Запрос: {data}')
        print(f'Ответ: {response.json()}')

    ch.basic_ack(delivery_tag=method.delivery_tag)
# callback


if __name__ == '__main__':
    rabbit_connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=config.get(section='RABBIT', option='host'),
            port=config.getint(section='RABBIT', option='port'),
            credentials=pika.PlainCredentials(
                username=config.get(section='RABBIT', option='user'),
                password=config.get(section='RABBIT', option='password'),
            ),
        )
    )

    rabbit_channel = rabbit_connection.channel()

    rabbit_channel.queue_declare(
        queue=config.get(section='APP', option='queue_email_bz'),
        durable=True,
    )
    rabbit_channel.basic_qos(prefetch_count=1)

    rabbit_channel.basic_consume(
        queue=config.get(section='APP', option='queue_email_bz'),
        on_message_callback=callback
    )

    rabbit_channel.start_consuming()
