import boto3
import uuid
import os

MEDIACONVERT_ROLE = os.environ['MEDIACONVERT_ROLE']
OUTPUT_BUCKET = os.environ['OUTPUT_BUCKET']
DESTINATION_BUCKET = os.environ['DESTINATION_BUCKET']

# Get MediaConvert endpoint
mc_client = boto3.client('mediaconvert')
endpoints = mc_client.describe_endpoints()
mc_endpoint = endpoints['Endpoints'][0]['Url']
mc = boto3.client('mediaconvert', endpoint_url=mc_endpoint)

def lambda_handler(event, context):
    input_s3 = event['input_s3']
    id = event.get('id', 'fulljob')
    input_filename = input_s3.split('/')[-1]
    output_prefix = f"{id}/"

    job_settings = {
        "UserMetadata": {
            "source": input_filename,
            "job": id
        },
        "Role": MEDIACONVERT_ROLE,
        "Settings": {
            "TimecodeConfig": {
            "Source": "ZEROBASED"
            },
            "OutputGroups": [
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
            "AdAvailOffset": 0,
            "Inputs": [
            {
                "AudioSelectors": {
                "Audio Selector 1": {
                    "Offset": 0,
                    "DefaultSelection": "NOT_DEFAULT",
                    "ProgramSelection": 1
                }
                },
                "VideoSelector": {
                "ColorSpace": "FOLLOW",
                "Rotate": "DEGREE_0"
                },
                "FilterEnable": "AUTO",
                "PsiControl": "USE_PSI",
                "FilterStrength": 0,
                "DeblockFilter": "DISABLED",
                "DenoiseFilter": "DISABLED",
                "TimecodeSource": "ZEROBASED",
                "FileInput": input_s3,            }
            ]
        },
        "BillingTagsSource": "JOB",
        "AccelerationSettings": {
            "Mode": "PREFERRED"
        },
        "StatusUpdateInterval": "SECONDS_60",
        "Priority": 0,
        "Tags": {
            "SolutionId": "SO0021"
        }
    }

    response = mc.create_job(**job_settings)
    return {
        "status": "submitted",
        "jobId": response['Job']['Id'],
        "outputPrefix": output_prefix
    }