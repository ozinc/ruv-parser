from __future__ import print_function
import os
import logging
import argparse

import requests
import bs4 as bs

from oz_core_api import OZCoreApi

EPG_URL = 'http://muninn.ruv.is/files/rs/ruv/';
AS_RUN_URL = 'http://muninn.ruv.is/files/rstiming/ruv/';

username = os.environ['OZ_USERNAME']
password = os.environ['OZ_PASSWORD']

api = OZCoreApi(username, password, '9f16f362-abad-4042-9e26-a69759347bd9')

# Logging setup
log = logging.getLogger(__name__)
log.setLevel(logging.WARN)
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)

def import_as_run():
    log.info('> importing RUV AS-RUN from:', AS_RUN_URL)
    r = requests.get(AS_RUN_URL)
    if r.status_code is not 200:
        raise Exception('Unable to fetch EPG from RUV, status was: %d', r.status_code)
    soup = bs.BeautifulSoup(r.content, 'xml')
    events = soup.findAll('event')
    log.info('> found %d as run items', len(events))
    for event in events:
        collection_id = None
        # For some reason RUV declares a "materialIdentifier" which is a
        # combination of serieId and episodeNumber so we have to parse that.
        materialIdentifier = event.find('material-identifier').text
        if materialIdentifier:
            # Get rid of leading zero which RUV prepends to the "material identifier".
            serieId = str(int(materialIdentifier.split('-')[0]))
            collection_id = import_collection({
                'externalId': 'ruv_' + serieId,
                'type': 'series', # TODO: You shouldn't need to do this.
                'name': event.title.text
            })

        # Create the video:
        import_video({
            'videoType': 'recording',
            'title': event.title.text,
            'externalId': 'ruv_' + event.id.text,
            'collectionId': collection_id
        })

def import_epg():
    log.info('> importing RUV EPG from: %s', EPG_URL)
    r = requests.get(EPG_URL)
    if r.status_code is not 200:
        raise Exception('Unable to fetch EPG from RUV, status was: %d', r.status_code)
    soup = bs.BeautifulSoup(r.content, 'xml')
    events = soup.findAll('event')
    log.info('> found %d scheduled items', len(events))
    for event in events:
        # Check if the event is associated with a collection
        serieId = event.get('serie-id')
        collection_id = None
        if serieId:
            # Populate the collection object
            collection = {
                'externalId': 'ruv_' + serieId,
                'type': 'series', # TODO: You shouldn't need to do this.
                'name': event.title.text
            }
            collection_id = import_collection(collection)
            log.info('collection_id: %s', collection_id)

        # Create the video:
        import_video({
            'title': event.title.text,
            'externalId': 'ruv_' + event.get('event-id'),
            'collectionId': collection_id
        })


def import_collection(collection):
    external_collection = api.fetch_collection_by_external_id(collection['externalId'])
    if external_collection is None:
        log.info('creating collection:', collection)
        new_collection = api.create_collection(collection)
        return new_collection['id']
    else:
        log.info('updating collection:', collection)
        return external_collection['id']

def import_video(video):
    external_video = api.fetch_video_by_external_id(video['externalId'])
    if external_video is None:
        log.info('creating video:', video)
        return api.create_video(video)
    else:
        log.info('video already existed, doing nothing...')
        return None

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Import EPG and As-Run data from RUV to OZ')
    parser.add_argument('-v', help='turn on verbose mode', action='store_true')
    args = parser.parse_args()
    if args.v:
        log.setLevel(logging.DEBUG)
        log.info("Verbose mode on")

    try:
        import_epg()
    except Exception as e:
        log.error(e)
