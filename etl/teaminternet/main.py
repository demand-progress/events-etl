# encoding=utf8

from etl.teaminternet import action as teaminternet_action

import boto3
import os
import json
import gzip

def run():
    data = teaminternet_action.grab_data()
    raw = json.dumps(data)
    content = 'window.TEAMINTERNET_EVENTS=' + raw

    # Locally Store Data
    with gzip.open('data/teaminternet.js.gz', 'wb') as f:
        f.write(str(content).encode('utf-8'))

    with open('data/teaminternet.json', 'w') as f:
        f.write(raw)

    # START
    s3 = boto3.client('s3')
    s3.upload_file('data/teaminternet.js.gz', 'teaminternet-map-data', 'output/teaminternet.js.gz', ExtraArgs={'ACL': 'public-read', "Metadata": {'Content-Type': 'text/plain', 'Content-Encoding': 'gzip'}})
    s3.upload_file('data/teaminternet.json', 'teaminternet-map-data', 'raw/teaminternet.json', ExtraArgs={'ACL': 'public-read'})

    # Cloudfront Invalidation requests
    # print("Invalidating Team Internet Output")
    # cloudfront = boto.connect_cloudfront()
    # paths = ['/output/*']
    # inval_req = cloudfront.create_invalidation_request(u'EXFHJXIFH495H', paths)

    os.remove("data/teaminternet.js.gz")
    os.remove("data/teaminternet.json")


# Retrieve all data

def queue():
    run()