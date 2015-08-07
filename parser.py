from __future__ import print_function
import logging
import argparse
import json

import requests
import bs4 as bs

from oz_core_api import OZCoreApi

EPG_URL = 'http://muninn.ruv.is/files/rs/ruv/';
AS_RUN_URL = 'http://muninn.ruv.is/files/rstiming/ruv/';

api = OZCoreApi('9f5d3f4900000f5bc8f73db9d677c48478bc09cb')

# Logging setup
log = logging.getLogger(__name__)
log.setLevel(logging.WARN)
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)

def import_as_run():
    log.info('importing RUV AS-RUN from:', AS_RUN_URL)
    r = requests.get(AS_RUN_URL)
    if r.status_code is not 200:
        raise Exception('unable to fetch AS RUN from RUV, status was: {0}'.format(r.status_code))
    soup = bs.BeautifulSoup(r.content, 'xml')
    events = soup.findAll('event')
    log.info('found %d as run items', len(events))
    for event in events:
        # Here we do a PATCH on the video with the only additional data that we get from RUVs
        # as run service; the start and end timestamp of the video.
        # Note that since we only know the "externalId" of the video we first need to fetch it.
        external_id = 'ruv_' + event.id.text
        external_video = api.fetch_video_by_external_id(external_id)
        if external_video is None:
            log.warn('as run video did not exist: {0}'.format(external_id))
        else:
            log.info('as run video did exist, updating: {0}'.format(external_video['id']))
            # TODO: This.

def import_epg():
    log.info('importing RUV EPG from: %s', EPG_URL)
    r = requests.get(EPG_URL)
    if r.status_code is not 200:
        raise Exception('unable to fetch EPG from RUV, status was: {0}'.format(r.status_code))
    soup = bs.BeautifulSoup(r.content, 'xml')
    events = soup.findAll('event')
    log.info('found %d scheduled items', len(events))
    for event in events:
        # Check if the event is associated with a collection
        serie_id = event.get('serie-id')
        collection_id = None
        if serie_id:
            # Populate the collection object.
            collection = {
                'externalId': 'ruv_' + serie_id,
                'type': 'series', # TODO: You shouldn't need to do this.
                'name': event.title.text
            }

            # RUV _sometimes_ has a "details" object associated with a "event" (schedule item)
            # which enlists some info on the series which this episode belongs to.
            seriesDetails = event.findAll('details', { 'id': serie_id })
            if len(seriesDetails) > 0:
                collection['name'] = seriesDetails[0].find('series-title').text
                collection['description'] = seriesDetails[0].find('series-description').text

            # TODO: The image.
            collection_id = upsert_collection(collection)

        # Populate the metadata object.
        metadata = {}
        if len(event.description.text) > 0:
            metadata['description'] = event.description.text

        # Populate the video object.
        video = {
            'title': event.title.text,
            'externalId': 'ruv_' + event.get('event-id'),
            'collectionId': collection_id
        }

        # Only attach the metadata field if we have some metadata.
        if len(metadata) > 0:
            video['metadata'] = json.dumps(metadata)

        # Create the video:
        upsert_video(video)

def upsert_collection(collection):
    external_collection = api.fetch_collection_by_external_id(collection['externalId'])
    if external_collection is None:
        log.info('creating collection: ' + str(collection))
        new_collection = api.create_collection(collection)
        return new_collection['id']
    else:
        # Attach the actual collection ID to the one that we are gonna update.
        collection['id'] = external_collection['id']
        log.info('updating collection: ' + str(collection))
        updated_collection = api.update_collection(collection)
        return updated_collection['id']

def upsert_video(video):
    external_video = api.fetch_video_by_external_id(video['externalId'])
    if external_video is None:
        log.info('creating video: ' + str(video))
        return api.create_video(video)
    else:
        # Attach the actual video ID to the one that we are gonna update.
        video['id'] = external_video['id']
        log.info('video already existed, updating it: ' + str(video))
        return api.update_video(video)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Import EPG and As-Run data from RUV to OZ')
    parser.add_argument('-v', help='turn on verbose mode', action='store_true')
    parser.add_argument('channel', help='The ID of the channel being imported to')
    args = parser.parse_args()
    api.channel_id = args.channel
    if args.v:
        log.setLevel(logging.DEBUG)
        log.info('verbose mode on')

    #try:
    import_epg()
#    except Exception as e:
#        log.error(e)
