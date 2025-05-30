import boto3
import uuid
import os
from datetime import datetime, timedelta

dynamodb = boto3.resource('dynamodb')
DDB_TABLE_NAME = 'tvna-streaming-solution-dev'

MEDIACONVERT_ROLE = os.environ['MEDIACONVERT_ROLE']
OUTPUT_BUCKET = os.environ['OUTPUT_BUCKET']
DESTINATION_BUCKET = os.environ['DESTINATION_BUCKET']

mc_client = boto3.client('mediaconvert')
endpoints = mc_client.describe_endpoints()
mc_endpoint = endpoints['Endpoints'][0]['Url']
mc = boto3.client('mediaconvert', endpoint_url=mc_endpoint)

def lambda_handler(event, context):
    input_s3 = event['input_s3']
    
  
    
    id = event.get('segment_id') or event.get('broadcast_id')
    if not id:
        return {"status": "error", "message": "Missing 'segment_id' or 'broadcast_id' in event."}
    
    # Check DynamoDB for existing HLS
    table = dynamodb.Table(DDB_TABLE_NAME)
    ddb_response = table.get_item(Key={'guid': id})
    if 'Item' in ddb_response:
        return {
            "status": "exists",
            "message": "HLS already available.",
            "hlsUrl": ddb_response['Item'].get('hlsUrl')
        }
    segment_id = event['segment_id']
    #   Else and if segment_id (and not in hls table): creates 2 hls streams of segment no captions with thumbnail
    start_time = event['start_time']
    duration = event['duration']
    
    job_id = segment_id
    start_tc = convert_to_timecode(start_time)
    end_tc = _add_timecodes(start_time, duration)
    input_filename = input_s3.split('/')[-1]
    output_prefix = f"segments/{segment_id}/"

    job_settings = {
        "Role": MEDIACONVERT_ROLE,
        "Settings": {
            "OutputGroups": [
                # MP4 File output
                {
                    "Name": "File Group",
                    "OutputGroupSettings": {
                        "Type": "FILE_GROUP_SETTINGS",
                        "FileGroupSettings": {
                            "Destination": f"s3://{OUTPUT_BUCKET}/{output_prefix}"
                        }
                    },
                    "Outputs": [{
                        "ContainerSettings": { "Container": "MP4" },
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
                        "NameModifier": f"_{segment_id}"
                    }]
                },
                # Thumbnails
                {
                    "Name": "Thumbnails",
                    "OutputGroupSettings": {
                        "Type": "FILE_GROUP_SETTINGS",
                        "FileGroupSettings": {
                            "Destination": f"s3://{DESTINATION_BUCKET}/thumbnails/{output_prefix}"
                        }
                    },
                    "Outputs": [{
                        "ContainerSettings": { "Container": "RAW" },
                        "VideoDescription": {
                            "CodecSettings": {
                                "Codec": "FRAME_CAPTURE",
                                "FrameCaptureSettings": {
                                    "FramerateNumerator": 1,
                                    "FramerateDenominator": int(float(duration.split(':')[-1])),
                                    "MaxCaptures": 1,
                                    "Quality": 80
                                }
                            }
                        },
                        "NameModifier": "_thumb"
                    }]
                },
            {
                "Name": "Apple HLS",
                "Outputs": [
                {
                    "ContainerSettings": {
                    "Container": "M3U8",
                    "M3u8Settings": {
                        "AudioFramesPerPes": 4,
                        "PcrControl": "PCR_EVERY_PES_PACKET",
                        "PmtPid": 480,
                        "PrivateMetadataPid": 503,
                        "ProgramNumber": 1,
                        "PatInterval": 0,
                        "PmtInterval": 0,
                        "VideoPid": 481,
                        "AudioPids": [
                        482,
                        483,
                        484,
                        485,
                        486,
                        487,
                        488,
                        489,
                        490,
                        491,
                        492,
                        493,
                        494,
                        495,
                        496,
                        497,
                        498
                        ]
                    }
                    },
                    "VideoDescription": {
                    "Width": 480,
                    "ScalingBehavior": "DEFAULT",
                    "Height": 270,
                    "TimecodeInsertion": "DISABLED",
                    "AntiAlias": "ENABLED",
                    "Sharpness": 100,
                    "CodecSettings": {
                        "Codec": "H_264",
                        "H264Settings": {
                        "InterlaceMode": "PROGRESSIVE",
                        "ParNumerator": 1,
                        "NumberReferenceFrames": 3,
                        "Syntax": "DEFAULT",
                        "GopClosedCadence": 1,
                        "HrdBufferInitialFillPercentage": 90,
                        "GopSize": 3,
                        "Slices": 1,
                        "GopBReference": "ENABLED",
                        "HrdBufferSize": 1000000,
                        "MaxBitrate": 400000,
                        "SlowPal": "DISABLED",
                        "ParDenominator": 1,
                        "SpatialAdaptiveQuantization": "ENABLED",
                        "TemporalAdaptiveQuantization": "ENABLED",
                        "FlickerAdaptiveQuantization": "ENABLED",
                        "EntropyEncoding": "CABAC",
                        "FramerateControl": "INITIALIZE_FROM_SOURCE",
                        "RateControlMode": "QVBR",
                        "QvbrSettings": {
                            "QvbrQualityLevel": 7
                        },
                        "CodecProfile": "HIGH",
                        "Telecine": "NONE",
                        "MinIInterval": 0,
                        "AdaptiveQuantization": "MEDIUM",
                        "FieldEncoding": "PAFF",
                        "SceneChangeDetect": "ENABLED",
                        "QualityTuningLevel": "SINGLE_PASS_HQ",
                        "FramerateConversionAlgorithm": "DUPLICATE_DROP",
                        "UnregisteredSeiTimecode": "DISABLED",
                        "GopSizeUnits": "SECONDS",
                        "ParControl": "SPECIFIED",
                        "NumberBFramesBetweenReferenceFrames": 5,
                        "RepeatPps": "DISABLED",
                        "DynamicSubGop": "ADAPTIVE"
                        }
                    },
                    "AfdSignaling": "NONE",
                    "DropFrameTimecode": "ENABLED",
                    "RespondToAfd": "NONE",
                    "ColorMetadata": "INSERT"
                    },
                    "AudioDescriptions": [
                    {
                        "AudioTypeControl": "FOLLOW_INPUT",
                        "AudioSourceName": "Audio Selector 1",
                        "CodecSettings": {
                        "Codec": "AAC",
                        "AacSettings": {
                            "AudioDescriptionBroadcasterMix": "NORMAL",
                            "Bitrate": 64000,
                            "RateControlMode": "CBR",
                            "CodecProfile": "HEV1",
                            "CodingMode": "CODING_MODE_2_0",
                            "RawFormat": "NONE",
                            "SampleRate": 48000,
                            "Specification": "MPEG4"
                        }
                        },
                        "LanguageCodeControl": "FOLLOW_INPUT",
                        "AudioType": 0
                    }
                    ],
                    "NameModifier": "_Ott_Hls_Ts_Avc_Aac_16x9_480x270p_0.4Mbps_qvbr"
                },
                {
                    "ContainerSettings": {
                    "Container": "M3U8",
                    "M3u8Settings": {
                        "AudioFramesPerPes": 4,
                        "PcrControl": "PCR_EVERY_PES_PACKET",
                        "PmtPid": 480,
                        "PrivateMetadataPid": 503,
                        "ProgramNumber": 1,
                        "PatInterval": 0,
                        "PmtInterval": 0,
                        "VideoPid": 481,
                        "AudioPids": [
                        482,
                        483,
                        484,
                        485,
                        486,
                        487,
                        488,
                        489,
                        490,
                        491,
                        492,
                        493,
                        494,
                        495,
                        496,
                        497,
                        498
                        ]
                    }
                    },
                    "VideoDescription": {
                    "Width": 640,
                    "ScalingBehavior": "DEFAULT",
                    "Height": 360,
                    "TimecodeInsertion": "DISABLED",
                    "AntiAlias": "ENABLED",
                    "Sharpness": 100,
                    "CodecSettings": {
                        "Codec": "H_264",
                        "H264Settings": {
                        "InterlaceMode": "PROGRESSIVE",
                        "ParNumerator": 1,
                        "NumberReferenceFrames": 3,
                        "Syntax": "DEFAULT",
                        "GopClosedCadence": 1,
                        "HrdBufferInitialFillPercentage": 90,
                        "GopSize": 3,
                        "Slices": 1,
                        "GopBReference": "ENABLED",
                        "HrdBufferSize": 3750000,
                        "MaxBitrate": 1500000,
                        "SlowPal": "DISABLED",
                        "ParDenominator": 1,
                        "SpatialAdaptiveQuantization": "ENABLED",
                        "TemporalAdaptiveQuantization": "ENABLED",
                        "FlickerAdaptiveQuantization": "ENABLED",
                        "EntropyEncoding": "CABAC",
                        "FramerateControl": "INITIALIZE_FROM_SOURCE",
                        "RateControlMode": "QVBR",
                        "QvbrSettings": {
                            "QvbrQualityLevel": 7
                        },
                        "CodecProfile": "HIGH",
                        "Telecine": "NONE",
                        "MinIInterval": 0,
                        "AdaptiveQuantization": "MEDIUM",
                        "FieldEncoding": "PAFF",
                        "SceneChangeDetect": "ENABLED",
                        "QualityTuningLevel": "SINGLE_PASS_HQ",
                        "FramerateConversionAlgorithm": "DUPLICATE_DROP",
                        "UnregisteredSeiTimecode": "DISABLED",
                        "GopSizeUnits": "SECONDS",
                        "ParControl": "SPECIFIED",
                        "NumberBFramesBetweenReferenceFrames": 5,
                        "RepeatPps": "DISABLED",
                        "DynamicSubGop": "ADAPTIVE"
                        }
                    },
                    "AfdSignaling": "NONE",
                    "DropFrameTimecode": "ENABLED",
                    "RespondToAfd": "NONE",
                    "ColorMetadata": "INSERT"
                    },
                    "AudioDescriptions": [
                    {
                        "AudioTypeControl": "FOLLOW_INPUT",
                        "AudioSourceName": "Audio Selector 1",
                        "CodecSettings": {
                        "Codec": "AAC",
                        "AacSettings": {
                            "AudioDescriptionBroadcasterMix": "NORMAL",
                            "Bitrate": 64000,
                            "RateControlMode": "CBR",
                            "CodecProfile": "HEV1",
                            "CodingMode": "CODING_MODE_2_0",
                            "RawFormat": "NONE",
                            "SampleRate": 48000,
                            "Specification": "MPEG4"
                        }
                        },
                        "LanguageCodeControl": "FOLLOW_INPUT",
                        "AudioType": 0
                    }
                    ],
                    "NameModifier": "_Ott_Hls_Ts_Avc_Aac_16x9_640x360p_1.5Mbps_qvbr"
                }
                ],
                "OutputGroupSettings": {
                "Type": "HLS_GROUP_SETTINGS",
                "HlsGroupSettings": {
                    "ManifestDurationFormat": "INTEGER",
                    "SegmentLength": 3,
                    "TimedMetadataId3Period": 10,
                    "CaptionLanguageSetting": "OMIT",
                    "Destination": f"s3://{DESTINATION_BUCKET}/{id}/hls/{output_prefix}",
                    "TimedMetadataId3Frame": "PRIV",
                    "CodecSpecification": "RFC_4281",
                    "OutputSelection": "MANIFESTS_AND_SEGMENTS",
                    "ProgramDateTimePeriod": 600,
                    "MinSegmentLength": 0,
                    "DirectoryStructure": "SINGLE_DIRECTORY",
                    "ProgramDateTime": "EXCLUDE",
                    "SegmentControl": "SEGMENTED_FILES",
                    "ManifestCompression": "NONE",
                    "ClientCache": "ENABLED",
                    "StreamInfResolution": "INCLUDE"
                }
                }
            }
            ],
            "Inputs": [{
                "FileInput": input_s3,
                "AudioSelectors": {
                    "Audio Selector 1": { "DefaultSelection": "DEFAULT" }
                },
                "InputClippings": [{
                    "StartTimecode": start_tc,
                    "EndTimecode": end_tc
                }],
                "TimecodeSource": "ZEROBASED"
            }]
        },
        "UserMetadata": {
            "source": input_filename,
            "job": job_id
        }
    }

    response = mc.create_job(**job_settings)
        # Construct HLS master playlist URL (assumes CloudFront or public S3)
    base_filename = input_filename.split('.')[-1]
    print(base_filename)
    master_playlist_url = f"https://d1hlyf3q0uigxh.cloudfront.net/{id}/hls/{id}/{base_filename}.m3u8"

    # Save metadata to DynamoDB
    
    table.put_item(
        Item={
            'guid': id,
            'filename': input_filename,
            'hlsUrl': master_playlist_url,
            'srcVideo': input_filename,
            'created_at': datetime.utcnow().isoformat(),
            'encodeJobId': response['Job']['Id'],
            'status': 'submitted',
            'renditions': ['360p', '720p'],
            'segemntId': id,
        }
    )
    return {
        "status": "submitted",
        "jobId": response['Job']['Id'],
        "outputPrefix": output_prefix
    }

def _add_timecodes(start, duration, framerate=30):
    fmt = '%H:%M:%S.%f'
    start_td = datetime.strptime(start, fmt)
    dur_td = datetime.strptime(duration, fmt)
    end_td = start_td + timedelta(
        hours=dur_td.hour, minutes=dur_td.minute,
        seconds=dur_td.second, microseconds=dur_td.microsecond
    )
    return f"{end_td.hour:02d}:{end_td.minute:02d}:{end_td.second:02d}:{int(end_td.microsecond / 1e6 * framerate):02d}"

def convert_to_timecode(hhmmss_mmm, framerate=30):
    t = datetime.strptime(hhmmss_mmm, '%H:%M:%S.%f')
    frames = int(t.microsecond / 1_000_000 * framerate)
    return f"{t.hour:02d}:{t.minute:02d}:{t.second:02d}:{frames:02d}"
