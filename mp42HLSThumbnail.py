import boto3
import os

MEDIACONVERT_ROLE = os.environ['MEDIACONVERT_ROLE']
OUTPUT_BUCKET = os.environ['OUTPUT_BUCKET']
DESTINATION_BUCKET = os.environ['DESTINATION_BUCKET']

# Get MediaConvert endpoint
mc_client = boto3.client('mediaconvert')
endpoints = mc_client.describe_endpoints()
mc_endpoint = endpoints['Endpoints'][0]['Url']
mc = boto3.client('mediaconvert', endpoint_url=mc_endpoint)
# Process inexplicably slow??!!
def lambda_handler(event, context):
    input_s3 = event['input_s3']
    segment_id = event.get('segment_id', 'fulljob')
    input_filename = input_s3.split('/')[-1]
    output_prefix = f"segments/{segment_id}/"

    job_settings = {
        "Role": MEDIACONVERT_ROLE,
        "Settings": {
            "OutputGroups": [
                {
                    "Name": "Apple HLS",
                    "OutputGroupSettings": {
                        "Type": "HLS_GROUP_SETTINGS",
                        "HlsGroupSettings": {
                            "Destination": f"s3://{DESTINATION_BUCKET}/hls/{output_prefix}",
                            "SegmentLength": 6,
                            "MinSegmentLength": 0,
                            "OutputSelection": "MANIFESTS_AND_SEGMENTS",
                            "StreamInfResolution": "INCLUDE",
                            "DirectoryStructure": "SINGLE_DIRECTORY",
                            "ManifestDurationFormat": "INTEGER",
                            "CodecSpecification": "RFC_4281"
                        }
                    },
                    "Outputs": [
                        {
                            "VideoDescription": {
                                "Width": 640,
                                "Height": 360,
                                "CodecSettings": {
                                    "Codec": "H_264",
                                    "H264Settings": {
                                        "Bitrate": 600000,
                                        "RateControlMode": "CBR",
                                        "SceneChangeDetect": "TRANSITION_DETECTION",
                                        "QualityTuningLevel": "SINGLE_PASS"
                                    }
                                }
                            },
                            "AudioDescriptions": [{
                                "AudioSourceName": "Audio Selector 1",
                                "CodecSettings": {
                                    "Codec": "AAC",
                                    "AacSettings": {
                                        "Bitrate": 96000,
                                        "CodingMode": "CODING_MODE_2_0",
                                        "SampleRate": 48000
                                    }
                                }
                            }],
                            "ContainerSettings": {
                                "Container": "M3U8"
                            },
                            "NameModifier": "_360p"
                        },
                        {
                            "VideoDescription": {
                                "Width": 1280,
                                "Height": 720,
                                "CodecSettings": {
                                    "Codec": "H_264",
                                    "H264Settings": {
                                        "Bitrate": 2500000,
                                        "RateControlMode": "CBR",
                                        "SceneChangeDetect": "TRANSITION_DETECTION",
                                        "QualityTuningLevel": "SINGLE_PASS"
                                    }
                                }
                            },
                            "AudioDescriptions": [{
                                "AudioSourceName": "Audio Selector 1",
                                "CodecSettings": {
                                    "Codec": "AAC",
                                    "AacSettings": {
                                        "Bitrate": 128000,
                                        "CodingMode": "CODING_MODE_2_0",
                                        "SampleRate": 48000
                                    }
                                }
                            }],
                            "ContainerSettings": {
                                "Container": "M3U8"
                            },
                            "NameModifier": "_720p"
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
                                        "FramerateDenominator": 1,  # One frame every second
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
                    "TimecodeSource": "ZEROBASED"
                }
            ]
        },
        "UserMetadata": {
            "source": input_filename,
            "job": segment_id
        }
    }

    response = mc.create_job(**job_settings)
    return {
        "status": "submitted",
        "jobId": response['Job']['Id'],
        "outputPrefix": output_prefix
    }
