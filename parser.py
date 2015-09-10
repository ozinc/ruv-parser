from __future__ import print_function
import os
import logging
import argparse
import json

import requests
import bs4 as bs
import arrow

from oz_core_api import OZCoreApi

EPG_URL    = 'http://muninn.ruv.is/files/rs/ruv/'
AS_RUN_URL = 'http://muninn.ruv.is/files/rstiming/ruv/'
RUV_CATEGORY_MOVIE_VALUE = '7'

username = os.environ['OZ_USERNAME']
password = os.environ['OZ_PASSWORD']

api = OZCoreApi(username, password)

# Logging setup
log = logging.getLogger(__name__)
log.setLevel(logging.WARN)
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)

def import_as_run():
    log.info('importing RUV AS-RUN from: ' + AS_RUN_URL)
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
            log.info('as run video did exist, state is: {0}'.format(event.state.text))
            if external_video['ingestionStatus'] == 'awaitingFile' and event.state.text == '4':
                log.info('Previously unaired episode has aired, vodifying video {0}'.format(external_video['id']))
                # This video has aired and is ready to be vodified.
                updated_video = external_video.copy()
                updated_video['ingestionStatus'] = 'processing'
                updated_video['metadata']['startTime'] = arrow.get(event.start.text).isoformat()
                updated_video['metadata']['endTime'] = arrow.get(event.stop.text).isoformat()
                upsert_video(updated_video)


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
        content_type = 'episode'

        # Very RUV specific: If the category is 'kvikmyndir' we know that this is a movie
        # and we want to set that as the content type.
        if event.category and event.category.get('value') == RUV_CATEGORY_MOVIE_VALUE:
            content_type = 'movie'

        # NOTE: Okay so apparently stuff like movies will often also have a serie_id
        # in RUVs EPG data and that's we have do the following to decide whether a
        # event belongs to a serie or not:
        is_episode = serie_id and content_type != 'movie'

        if is_episode:
            # Populate the collection object.
            collection = {
                'externalId': 'ruv_' + serie_id,
                'type': 'series', # TODO: You shouldn't need to do this.
                'name': event.title.text
            }

            # RUV _sometimes_ has a "details" object associated with a "event" (schedule item)
            # which enlists some info on the series which this episode belongs to.
            series_details = event.findAll('details', { 'id': serie_id })
            if len(series_details) > 0:
                collection['name'] = series_details[0].find('series-title').text
                collection['description'] = series_details[0].find('series-description').text

            # TODO: Deal with the image.

            collection_id = upsert_collection(collection)

        # Populate the metadata object.
        metadata = {}
        if len(event.description.text) > 0:
            metadata['description'] = event.description.text

        # Populate the video object
        if is_episode:
            # So apparently RUV don't have any notion of season numbers in their EPG data.
            #metadata['seasonNumber'] = ?
            metadata['episodeNumber'] = int(event.episode.get('number'))

        video = {
            'videoType': 'recording',
            'contentType': content_type,
            'title': event.title.text,
            'externalId': 'ruv_' + event.get('event-id'),
            'collectionId': collection_id,
            'published': True
        }

        # Include poster
        if event.image:
            video['posterUrl'] = event.image.text


        # Only attach the metadata field if we have some metadata.
        if len(metadata) > 0:
            video['metadata'] = json.dumps(metadata)

        # Create the video:
        video_id = upsert_video(video)

        # Parse the time strings
        start_time = arrow.get(event.get('start-time'))

        # Create a slot to schedule the video to be played
        # at the specified time
        slot = {
            'type': 'content', # All slots have type content for now
            'startTime': start_time.isoformat(),
            'metadata': {
                'videoId': video_id
            }
        }
        upsert_slot(slot)


def upsert_collection(collection):
    return upsert_object('collection', collection)

def upsert_slot(slot):
    return upsert_object('slot', slot)

def upsert_video(video):
    return upsert_object('video', video)

def upsert_object(obj_type, obj):
    external_obj = getattr(api, 'fetch_{}_by_external_id'.format(obj_type))(obj['externalId'])
    if external_obj is None:
        log.info('creating {}: '.format(obj_type) + str(obj))
        new_obj = getattr(api, 'create_{}'.format(obj_type))(obj)
        return new_obj['id']
    else:
        # Attach the actual object ID to the one that we are gonna update.
        obj['id'] = external_obj['id']
        log.info('{} already existed, updating it: '.format(obj_type) + str(obj))
        new_obj = getattr(api, 'update_{}'.format(obj_type))(obj)
        return new_obj['id']


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Import EPG and As-Run data from RUV to OZ')
    parser.add_argument('-v', help='turn on verbose mode', action='store_true')
    parser.add_argument('action', help='epg or asrun')
    parser.add_argument('channel', help='The ID of the channel being imported to')
    args = parser.parse_args()
    api.channel_id = args.channel
    if args.v:
        log.setLevel(logging.DEBUG)
        log.info('verbose mode on')
    # Do this thing!
    if args.action == 'epg':
        import_epg()
    elif args.action == 'asrun':
        import_as_run()
    else:
        raise Exception('unsupported operation')



