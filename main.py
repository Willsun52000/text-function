# -*- coding: utf-8 -*-
import base64
import json
import os
import wave

import pandas as pd
from google.cloud import storage
from google.cloud import pubsub_v1
from google.cloud import speech_v1p1beta1 as speech
from google.cloud.speech_v1p1beta1 import enums
from google.cloud.speech_v1p1beta1 import types

storage_client = storage.Client()
publisher = pubsub_v1.PublisherClient()
bucket_text = storage_client.get_bucket('glaucusis_meeting_text')

project_id = os.environ['GCP_PROJECT']

with open('config.json') as f:
    data = f.read()
config = json.loads(data)


def parse_audio(uri, speaker_num=2):
    print('Looking for audio in storage {}'.format(uri))

    futures = []

    topic_name = config['OPERATION_TOPIC']

    client = speech.SpeechClient()
    metadata = speech.types.RecognitionMetadata()
    metadata.industry_naics_code_of_audio = 523930
    audio = types.RecognitionAudio(uri=uri)
    mp3flg = uri.split("/")[-1].split(".")[-1]
    if mp3flg == 'mp3':
        fencoding = enums.RecognitionConfig.AudioEncoding.MP3
    else:
        fencoding = enums.RecognitionConfig.AudioEncoding.LINEAR16
    config = types.RecognitionConfig(
        sample_rate_hertz=44100,
        encoding=fencoding,
        language_code='en-US',
        enable_speaker_diarization=True,
        diarization_speaker_count=2,
        enable_automatic_punctuation=True,
        enable_word_time_offsets=True,
        use_enhanced=True,
        model='phone_call',
        audio_channel_count=2,
        metadata=metadata
    )
    operation = client.long_running_recognize(config, audio)

    filename = uri.split("/")[-1].split(".")[0]
    message = {
        'operation': operation,
        'filename': filename + ".csv"
    }
    message_data = json.dumps(message).encode('utf-8')
    topic_path = publisher.topic_path(project_id, topic_name)
    future = publisher.publish(topic_path, data=message_data)
    futures.append(future)

    for future in futures:
        future.result()


def hello_world(request):
    jobid = "2368578696000157672"
    client = speech.SpeechClient()
    operation_instance = client.transport._operations_client.get_operation(jobid)
    operation_response = operation_core.from_gapic(
        operation_instance,
        client.transport._operations_client,
        types.LongRunningRecognizeResponse,
        metadata_type=types.LongRunningRecognizeMetadata,
    )
    print("start")
    if operation_instance.done:
        print("done")
        response = operation_response.result()
        print(response)
        print(response.results)
        counts = [len(i.alternatives[0].words) for i in response.results[:-1]]
        count = 0
        label = []
        for i in counts:
            count += i
            label.append(count)
        speakers = [response.results[-1].alternatives[0].words[i - 1].speaker_tag for i in label]
        data = [(speakers[idx], result.alternatives[0].words[0].start_time.seconds,
                 result.alternatives[0].words[-1].end_time.seconds,
                 result.alternatives[0].transcript) for idx, result in enumerate(response.results[:-1])]
        sentences = []
        se = []
        speakers = []
        sp = []
        starts = []
        st = []
        ends = []
        ed = []
        results = []
        rs = []
        for i in range(1, len(data)):
            rs.append(data[i - 1])
            sp.append(data[i - 1][0])
            st.append(data[i - 1][1])
            ed.append(data[i - 1][2])
            se.append(data[i - 1][3])
            if data[i - 1][0] == data[i][0]:
                pass
            else:
                sentences.append(se)
                speakers.append(sp)
                starts.append(st)
                ends.append(ed)
                results.append(rs)
                se = []
                sp = []
                ed = []
                st = []
                rs = []
        if not results is None:
            sentences.append(data[-1][3])
            speakers[-1].append(data[-1][0])
            starts[-1].append(data[-1][1])
            ends[-1].append(data[-1][2])
        if results[-1][0][0] == data[-1][0]:
            sentences[-1].append(data[-1][3])
            speakers[-1].append(data[-1][0])
            starts[-1].append(data[-1][1])
            ends[-1].append(data[-1][2])
        else:
            sentences.append(data[-1][3])
            speakers[-1].append(data[-1][0])
            starts[-1].append(data[-1][1])
            ends[-1].append(data[-1][2])
        speaker = [max(i) for i in speakers]
        start = [min(i) for i in starts]
        end = [max(i) for i in ends]
        sentence = ["".join(i) for i in sentences]
        db = [i for i in zip(speaker, start, end, sentence)]
        df = pd.DataFrame(db, columns=["speaker", "start_time", "end_time", "transcript"])
        name = 'test1'
        bucket_text.blob(name + ".csv").upload_from_string(df.to_csv(), "text/csv")
        print("task is done")
    else:
        print("task failed")


def hello_gcs(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    bucket_name = config['AUDIO_BUCKET']
    file_name = event['name']
    parse_audio("gs://{}/{}".format(bucket_name, file_name))
