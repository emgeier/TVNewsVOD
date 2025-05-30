import json
import os
import time
import hmac
import hashlib
import boto3
from botocore.exceptions import ClientError

s3 = boto3.client("s3")
mediaconvert = boto3.client("mediaconvert", endpoint_url=os.environ["MEDIACONVERT_ENDPOINT"])
rds = boto3.client("rds-data")

# Environment variables
SEGMENT_BUCKET = os.environ["SEGMENT_BUCKET"]
MEDIACONVERT_ROLE = os.environ["MEDIACONVERT_ROLE"]
SHARED_SECRET = os.environ["SHARED_SECRET"].encode()
DB_SECRET_ARN = os.environ["DB_SECRET_ARN"]
DB_CLUSTER_ARN = os.environ["DB_CLUSTER_ARN"]
DB_NAME = os.environ["DB_NAME"]


def lambda_handler(event, context):
    segment_id = event["queryStringParameters"].get("segment_id")
    user_id = event["requestContext"]["authorizer"]["claims"].get("sub")

    if not segment_id or not user_id:
        return respond(400, "Missing required parameters")
    # Placeholder-- use actual folder where the processed segments will go/are (check if segments or broadcasts have been processed)
    s3_key = f"segments/{segment_id}/hls/master.m3u8"

    # Step 1: Check if segment already exists
    try:
        s3.head_object(Bucket=SEGMENT_BUCKET, Key=s3_key)
        cookie_value = generate_cookie(segment_id)
        return {
            "statusCode": 200,
            "headers": {
                "Set-Cookie": f"segment_access={cookie_value}; Path=/; Secure; HttpOnly; SameSite=None",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Credentials": "true",
                "Content-Type": "application/json"
            },
            "body": json.dumps({"message": "Segment already exists", "segment_path": s3_key})
        }
    except ClientError as e:
        if e.response["Error"]["Code"] != "404":
            raise

    # Step 2: Get metadata from RDS
    metadata = get_segment_metadata(segment_id)
    if not metadata:
        return respond(404, "Segment ID not found")
    # Adjust for actual path based on broadcast id
    input_path = metadata["input_path"]
    start_time = metadata["start_time"]
    duration = metadata["duration"]

    # Convert start_time and duration to end_time (assumes HH:MM:SS format)
    def time_to_seconds(t):
        h, m, s = map(int, t.split(":"))
        return h * 3600 + m * 60 + s

    def seconds_to_time(secs):
        h = secs // 3600
        m = (secs % 3600) // 60
        s = secs % 60
        return f"{h:02}:{m:02}:{s:02}:00"  # frame suffix for MediaConvert

    start_seconds = time_to_seconds(start_time)
    duration_seconds = int(duration)
    end_seconds = start_seconds + duration_seconds
    end_time = seconds_to_time(end_seconds)

    # Step 3: Trigger MediaConvert job
    job = build_mediaconvert_job(input_path, start_time + ":00", end_time, segment_id)
    mediaconvert.create_job(Role=MEDIACONVERT_ROLE, Settings=job)

    # Step 4: Wait for the segment to appear
    wait_seconds = 60
    poll_interval = 5
    deadline = time.time() + wait_seconds

    while time.time() < deadline:
        try:
            s3.head_object(Bucket=SEGMENT_BUCKET, Key=s3_key)
            cookie_value = generate_cookie(segment_id)
            return {
                "statusCode": 200,
                "headers": {
                    "Set-Cookie": f"segment_access={cookie_value}; Path=/; Secure; HttpOnly; SameSite=None",
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Credentials": "true",
                    "Content-Type": "application/json"
                },
                "body": json.dumps({"message": "Segment is ready", "segment_path": s3_key})
            }
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                time.sleep(poll_interval)
            else:
                raise

    # Timed out
    cookie_value = generate_cookie(segment_id)
    return {
        "statusCode": 202,
        "headers": {
            "Set-Cookie": f"segment_access={cookie_value}; Path=/; Secure; HttpOnly; SameSite=None",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Credentials": "true",
            "Content-Type": "application/json"
        },
        "body": json.dumps({"message": "Segment is being processed. Please try again in a moment."})
    }

# Placeholder-- ProServe is going to generate the cookies with a limit/user
def generate_cookie(segment_id, ttl=3600):
    expiry = int(time.time()) + ttl
    payload = f"{segment_id}.{expiry}"
    signature = hmac.new(SHARED_SECRET, payload.encode(), hashlib.sha256).hexdigest()
    return f"{segment_id}.{expiry}.{signature}"


def get_segment_metadata(segment_id):
    sql = """
        SELECT input_path, start_time, duration
        FROM segment_metadata
        WHERE segment_id = :segment_id
    """
    result = rds.execute_statement(
        secretArn=DB_SECRET_ARN,
        resourceArn=DB_CLUSTER_ARN,
        database=DB_NAME,
        sql=sql,
        parameters=[{"name": "segment_id", "value": {"stringValue": segment_id}}]
    )

    records = result.get("records")
    if not records:
        return None
    # Adjust based on data schema
    record = records[0]
    return {
        "input_path": record[0]["stringValue"],
        "start_time": record[1]["stringValue"],
        "duration": record[2]["stringValue"]
    }


def build_mediaconvert_job(input_path, start_time, end_time, segment_id):
    return {
        "Inputs": [
            {
                "FileInput": input_path,
                "InputClippings": [
                    {"StartTimecode": start_time, "EndTimecode": end_time}
                ]
            }
        ],
        "OutputGroups": [
            {
                "Name": "HLS Group",
                "OutputGroupSettings": {
                    "Type": "HLS_GROUP_SETTINGS",
                    "HlsGroupSettings": {
                        "Destination": f"s3://{SEGMENT_BUCKET}/segments/{segment_id}/hls/"
                    }
                },
                "Outputs": [
                    {
                        "ContainerSettings": {"Container": "M3U8"},
                        "VideoDescription": {
                            "CodecSettings": {
                                "Codec": "H_264",
                                "H264Settings": {
                                    "RateControlMode": "QVBR",
                                    "MaxBitrate": 5000000
                                }
                            }
                        },
                        "AudioDescriptions": [
                            {"CodecSettings": {"Codec": "AAC"}}
                        ]
                    }
                ]
            }
        ]
    }


def respond(status_code, message):
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Credentials": "true",
            "Content-Type": "application/json"
        },
        "body": json.dumps({"message": message})
    }