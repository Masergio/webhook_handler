import datetime
import boto3
import json
import gzip
import psycopg2
import psycopg2.extras


DB_HOST = "localhost"
DB_PORT = 5432
DB_USER = "postgres"
DB_PASSWORD = "mysecretpassword"
CONTAINER_NAME = "local-email-postgres"
DB_NAME = "webhook_events"

PROCESSED = 1
DROPPED = 2
DELIVERED = 3
DEFERRED = 4
BOUNCE = 5
OPENED = 6
CLICKED = 7
SPAM_REPORT = 8
UNSUBSCRIBE = 9
GROUP_UNSUBSCRIBE = 10
GROUP_RESUBSCRIBE = 11

EVENT_MAPPER = {
    "processed": PROCESSED,
    "dropped": DROPPED,
    "delivered": DELIVERED,
    "deferred": DEFERRED,
    "bounce": BOUNCE,
    "open": OPENED,
    "click": CLICKED,
    "spamreport": SPAM_REPORT,
    "unsubscribe": UNSUBSCRIBE,
    "group_unsubscribe": GROUP_UNSUBSCRIBE,
    "group_resubscribe": GROUP_RESUBSCRIBE,
}

FAILED_EVENTS = {"bounce", "deferred", "dropped"}

FIELDS = (
    "campaign_id",
    "event_type",
    "recipient",
    "message_id",
    "event_id",
    "details",
    "timestamp",
)

UPSERT_PLACEHOLDER_TEMPLATE = "({})".format(", ".join(["%s"] * len(FIELDS)))

BULK_UPSERT = f"""
    INSERT INTO email_events({", ".join(FIELDS)})
    VALUES %s
    ON CONFLICT (event_id)
    DO NOTHING;
"""


def create_table(cur):
    cur.execute(
        """CREATE TABLE IF NOT EXISTS email_events (
            id SERIAL PRIMARY KEY,
            campaign_id INT,
            event_type INT,
            recipient VARCHAR(255),
            message_id VARCHAR(200),
            event_id VARCHAR(200) UNIQUE NOT NULL,
            timestamp TIMESTAMP,
            details VARCHAR(200)
        );"""
    )


def main(file, host, port, database, user, password):
    conn = None
    print("Connect to the PostgreSQL database server")
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
        )
        print(f"connection: {conn}")
        conn.autocommit = True

        # Read webhook events
        contents = read_data(file)
        # print(f"contents: {contents}\n\n")

        with conn.cursor() as cursor:
            # Create table if not exists
            create_table(cursor)
            # Process webhook events data
            # events = [parse(campaign_event=json.loads(line.decode('utf-8'))) for line in contents]
            events = []
            for line in contents:
                campaign_event = json.loads(line.decode('utf-8'))
                parsed_event = parse(campaign_event)
                if parsed_event:
                    events.append(parsed_event)

            print(f"Length events: {len(events)}")
            # print(f"events: {events}\n\n")
            save_data(cursor, events)
    except Exception as err:
        print(repr(err))
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


def read_data(file):
    with gzip.open(file, mode="rb") as f:
        contents = f.read().splitlines()
        print(f"Content length: {len(contents)}")
        # for i, line in enumerate(contents):
        #     if i < 10:
        #         parsed_data = json.loads(line.decode('utf-8'))
        #
        #         print("- " * 50)
        #         print(f"'campaign_id' in parsed_data: {'campaign_id' in parsed_data}")
        #         print(f"parsed_data: {parsed_data}")
        #
        #     else:
        #         break
    return contents


def parse(campaign_event: dict):
    if campaign_event.get("campaign_id") and campaign_event["campaign_id"].isdigit():
        event_type = campaign_event["event"]
        details = None
        if event_type in FAILED_EVENTS:
            details = (
                f"event={event_type} type={campaign_event.get('type')} "
                f"reason={campaign_event.get('reason')} "
                f"response={campaign_event.get('response')}"
            )[:200]

        parse_campaign_event = dict(
            campaign_id=int(campaign_event["campaign_id"]),
            event_type=EVENT_MAPPER[event_type],
            recipient=campaign_event["email"],
            message_id=campaign_event["sg_message_id"].split(".")[0],
            event_id=campaign_event["sg_event_id"],
            details=details,
            timestamp=datetime.datetime.fromtimestamp(campaign_event["timestamp"]),
        )
        return tuple([parse_campaign_event[field] for field in FIELDS])


def save_data(cur, events):
    psycopg2.extras.execute_values(
        cur=cur,
        sql=BULK_UPSERT,
        argslist=events,
        template=UPSERT_PLACEHOLDER_TEMPLATE,
    )


if __name__ == '__main__':
    db_connection = {
        "host": DB_HOST,
        "port": DB_PORT,
        "database": DB_NAME,
        "user": DB_USER,
        "password": DB_PASSWORD,
    }
    filename = "webhook_data.gz"
    main(filename, **db_connection)


def parse_events(file):
    with gzip.open(file, mode="rt") as f:
        file_content = f.read()
        print(file_content)


def save_files():
    s3_client = boto3.client("s3")
    bucket = "spoton-prod-message-analytics"
    bucket_prefix = "/2021/12"
    result = s3_client.list_objects(Bucket=bucket, Prefix=bucket_prefix)
    for obj in result.get('Contents'):
        data = s3_client.get_object(Bucket=bucket, Key=obj.get("Key"))
        contents = data['Body'].read().splitlines()
        print(f"Content length: {len(contents)}")
        for i, line in enumerate(contents):
            if i < 10:
                parsed_data = json.loads(line.decode('utf-8'))
                print(f"parsed_data: {parsed_data}")
            else:
                break

        # # Download and Save file locally
        # path, filename = os.path.split(obj.get("Key"))
        # # boto3 s3 download_file will throw exception if folder not exists
        # try:
        #     os.makedirs(path)
        # except FileExistsError:
        #     pass
        # with open(filename, 'wb') as data:
        #     s3_client.download_fileobj(bucket, obj.get("Key"), data)
