import boto3
import uuid
import os

MEDIACONVERT_ROLE = os.environ['MEDIACONVERT_ROLE']  # IAM role ARN
OUTPUT_BUCKET = os.environ['OUTPUT_BUCKET']          # Target bucket name
DESTINATION_BUCKET = os.environ['DESTINATION_BUCKET']          # Target bucket name

# Get MediaConvert endpoint (caching is recommended in real usage)
mc_client = boto3.client('mediaconvert')
endpoints = mc_client.describe_endpoints()
mc_endpoint = endpoints['Endpoints'][0]['Url']

# Now use this endpoint to create the MediaConvert client
mc = boto3.client('mediaconvert', endpoint_url=mc_endpoint)

def lambda_handler(event, context):
    input_s3 = event['input_s3']           # s3://input-proxy-bucket/*/*/*/filename.mp4
    start_time = event['start_time']       
    duration = event['duration']   
    segment_id = event['segment_id']        
    job_id = segment_id

    start_tc = convert_to_timecode(start_time)
    end_tc = _add_timecodes(start_time, duration)

    input_filename = input_s3.split('/')[-1]
    output_prefix = f"segments/{segment_id}/"

    job_settings = {
        "Role": MEDIACONVERT_ROLE,
        "Settings": {
            "OutputGroups": [
                {
                    "Name": "File Group",
                    "OutputGroupSettings": {
                        "Type": "FILE_GROUP_SETTINGS",
                        "FileGroupSettings": {
                            "Destination": f"s3://{OUTPUT_BUCKET}/{output_prefix}"
                        }
                    },
                    "Outputs": [
                        {
                            "ContainerSettings": {
                                "Container": "MP4"
                            },
                            "VideoDescription": {
                                "CodecSettings": {
                                    "Codec": "H_264",
                                    "H264Settings": {
                                        "RateControlMode": "QVBR",
                                        "SceneChangeDetect": "TRANSITION_DETECTION",
                                        "MaxBitrate": 5000000,
                                        "QualityTuningLevel": "SINGLE_PASS"
                                    }
                                }
                            },
                            "AudioDescriptions": [
                                {
                                    "AudioSourceName": "Audio Selector 1",  
                                    "CodecSettings": {
                                        "Codec": "AAC",
                                        "AacSettings": {
                                            "Bitrate": 96000,
                                            "CodingMode": "CODING_MODE_2_0",
                                            "SampleRate": 48000
                                        }
                                    }
                                }
                            ],
                            "NameModifier": f"_{segment_id}"
                        }
                    ]
                },
                {
                    "Name": "Thumbnails",
                    "OutputGroupSettings": {
                        "Type": "FILE_GROUP_SETTINGS",
                        "FileGroupSettings": {
                            "Destination": f"s3://{DESTINATION_BUCKET}/thumbnails/{output_prefix}"
                        }
                    },
                    "Outputs": [
                        {
                            "ContainerSettings": {
                                "Container": "RAW"
                            },
                            "VideoDescription": {
                                "CodecSettings": {
                                    "Codec": "FRAME_CAPTURE",
                                    "FrameCaptureSettings": {
                                        "FramerateNumerator": 1,
                                        "FramerateDenominator": int(float(duration.split(':')[-1])),  # One frame in duration window
                                        "MaxCaptures": 1,
                                        "Quality": 80
                                    }
                                }
                            },
                            "NameModifier": "_thumb"
                        }
                    ]
                }
            ],
            "Inputs": [
                {
                    "FileInput": input_s3,
                    "AudioSelectors": {
                        "Audio Selector 1": {
                            "DefaultSelection": "DEFAULT"
                        }
                    },
                    "InputClippings": [
                        {
                            "StartTimecode": start_tc,
                            "EndTimecode": end_tc
                        }
                    ],
                    "TimecodeSource": "ZEROBASED"
                }
            ]
        },
        "UserMetadata": {
            "source": input_filename,
            "job": job_id
        }
    }


    response = mc.create_job(**job_settings)
    return {
        "status": "submitted",
        "jobId": response['Job']['Id'],
        "outputPrefix": output_prefix
    }

def _add_timecodes(start, duration, framerate=30):
    from datetime import datetime, timedelta

    def to_timecode_string(dt, framerate):
        total_seconds = dt.hour * 3600 + dt.minute * 60 + dt.second + dt.microsecond / 1_000_000
        hours = int(total_seconds // 3600)
        minutes = int((total_seconds % 3600) // 60)
        seconds = int(total_seconds % 60)
        frames = int((total_seconds - int(total_seconds)) * framerate)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}:{frames:02d}"

    fmt = '%H:%M:%S.%f'
    start_td = datetime.strptime(start, fmt)
    dur_td = datetime.strptime(duration, fmt)
    end_td = start_td + timedelta(
        hours=dur_td.hour, minutes=dur_td.minute,
        seconds=dur_td.second, microseconds=dur_td.microsecond
    )

    return to_timecode_string(end_td, framerate)

def convert_to_timecode(hhmmss_mmm, framerate=30):
    from datetime import datetime
    t = datetime.strptime(hhmmss_mmm, '%H:%M:%S.%f')
    frames = int(t.microsecond / 1_000_000 * framerate)
    return f"{t.hour:02d}:{t.minute:02d}:{t.second:02d}:{frames:02d}"
