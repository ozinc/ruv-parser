#!/usr/bin/env python3
#-*- coding: utf-8 -*-

from __future__ import print_function
import os
import logging
import argparse
import json
from collections import namedtuple

import requests
import bs4 as bs
import arrow
import sys

from oz import OZCoreApi

RUV_CATEGORY_MOVIE_VALUE = '7'
RUV_CATEGORY_NEWS_VALUE = '3'
RUV_CATEGORY_SPORT_VALUE = '5'

DEFAULT_COLLECTION_ID = 'f4003614-238c-4835-b676-cbae4ee52299' # Þættir
MOVIE_COLLECTION_ID = '74ea2aa5-44a1-4114-aeea-f818bf7f7d21' # Kvikmyndir
CATEGORY_COLLECTION_MAPPING = {
    '1': '13a8d77b-0386-4319-9c12-633ff54f8c53', # Börn -> Barnaefni
    '3': '44bc1184-b669-4619-828b-70576c62df4a', # Fréttatengt -> Fréttir
    '5': '79364670-48e4-46c8-8a1f-896f41117536' # Íþróttir -> Íþróttir
}

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

CoreObject = namedtuple('CoreObject', ['type', 'properties'])

def import_as_run():
    log.info('importing asrun for channel: ruv')
    stdin = sys.stdin.buffer.read()
    soup = bs.BeautifulSoup(stdin, 'xml')
    events = soup.findAll('event')
    log.info('found %d as run items', len(events))

    for event in events:
        # Here we do a PATCH on the slot with the only additional data that we
        # get from RUVs as run service; the start and end timestamp of the video
        external_id = 'ruv-' + event.id.text
        external_slot = api.fetch_slot_by_external_id(external_id, include='video')
        if external_slot is None:
            log.warn('as run slot did not exist: {0}'.format(external_id))
        else:
            log.info('as run slot did exist, state is: {0}'.format(event.state.text))

            try:
                start_time = arrow.get(event.start.text).isoformat()
                end_time = arrow.get(event.stop.text).isoformat()
            except Exception as e:
                # Either start or stop were null
                log.warn('Start or end time of event was empty, skipping...')
                continue

            external_video = external_slot['video']
            if external_video['ingestionStatus'] == 'awaitingFile' and event.state.text == '4':
                log.info('Previously unaired episode has aired, vodifying video {0}'
                    .format(external_video['id']))

                # This video has aired and is ready to be vodified.
                updated_slot = {
                    'externalId': external_id,
                    'metadata': external_slot.get('metadata', {})
                }
                updated_slot['metadata']['started'] = start_time
                updated_slot['metadata']['ended'] = end_time
                upsert_slot(CoreObject('slot', updated_slot), vodify='true')

def import_epg(stream_id):
    station = 'ruv' # TODO: Make this configurable
    log.info('importing EPG from: {0}'.format(station))
    stdin = sys.stdin.buffer.read()
    soup = bs.BeautifulSoup(stdin, 'xml')
    events = soup.findAll('event')
    log.info('found %d scheduled items', len(events))

    for event in events:
        # Determine IDs and whether this is an episode or a single
        video_external_id = 'ruv-' + event.reference.get('material')
        collection_external_id = 'ruv-' + event.reference.get('group')
        slot_external_id = 'ruv-' + event.get('event-id')
        is_episode = event.episode.get('multiple-episodes') == 'yes'

        log.info('video_external_id: {0}'.format(video_external_id))
        log.info('episode: {0}'.format(str(event.episode)))
        log.info('is_episode? {0}'.format(str(is_episode)))

        # Category
        content_type = 'episode'
        if event.category:
            category_id = event.category.get('value')
            if category_id == RUV_CATEGORY_MOVIE_VALUE:
                content_type = 'movie'
            elif category_id in [RUV_CATEGORY_NEWS_VALUE, RUV_CATEGORY_SPORT_VALUE]:
                content_type = 'news'

        # Collection
        collection_id = None
        if is_episode:
            # Determine parent collection of this collection
            parent_id = DEFAULT_COLLECTION_ID
            if event.category:
                category_value = event.category.get('value')
                if category_value in CATEGORY_COLLECTION_MAPPING:
                    parent_id = CATEGORY_COLLECTION_MAPPING[category_value]
                log.info('cat -> coll: {0} -> {1}'.format(category_value, parent_id))
            else:
                log.info('no category metadata, setting parent coll to default')

            # Populate the collection object
            collection_props = {
                'externalId': collection_external_id,
                'type': 'general',
                'name': event.title.text,
                'parentId': parent_id
            }

            # RUV sometimes has a "details" object associated with a "event" (schedule item)
            # which enlists some info on the series which this episode belongs to.
            series_details = event.findAll('details', { 'id': event.get('serie-id') })
            if len(series_details) > 0:
                collection_props['name'] = series_details[0].find('series-title').text
                collection_props['description'] = series_details[0].find('series-description').text

            collection = CoreObject('collection', collection_props);
            collection_id = upsert_collection(collection)

        # If this is a movie; put it under the "Kvikmyndir" collection
        if content_type == 'movie':
            collection_id = MOVIE_COLLECTION_ID

        # Populate the metadata object
        metadata = {}
        if event.description != None and len(event.description.text) > 0:
            metadata['description'] = event.description.text

        # Populate the video object
        if is_episode:
            # So apparently RUV don't have any notion of season numbers in their EPG data,
            # according to a tech guy from RUV they are adding this though
            #metadata['seasonNumber'] = ?
            metadata['episodeNumber'] = int(event.episode.get('number'))

        # Parse the time strings
        start_time = arrow.get(event.get('start-time'))

        if content_type == 'news':
            metadata['date'] = start_time.isoformat()

        # For now we assume the same rights as 'vod' type
        rights = event.find('rights', type='vod')

        # We default to having the expiry at the end of the event,
        # but if we see an "allowed vod"-right we use that expiry.
        availability_time = arrow.get(event.get('end-time'))
        if rights is not None and rights.get('action') == 'allowed':
            availability_time = arrow.get(rights.get('expires'))

        # Default to IS geo-restrictions, but opt into no restrictions if they
        # EPG allows it.
        playback_countries = ['IS']
        stream = event.find('stream')
        if stream is not None and stream.get('scope') == 'global':
            playback_countries = ['GLOBAL']

        video_props = {
            'externalId': video_external_id,
            'sourceType': 'stream',
            'contentType': content_type,
            'title': event.title.text,
            'collectionId': collection_id,
            'published': True,
            'playbackCountries': playback_countries
        }

        # Include poster
        if event.image:
            video_props['posterUrl'] = event.image.text

        if availability_time:
            video_props['playableUntil'] = availability_time.isoformat()

        # Only attach the metadata field if we have some metadata.
        if len(metadata) > 0:
            video_props['metadata'] = metadata

        # Create the video:
        video = CoreObject('video', video_props)
        video_id = upsert_video(video)

        # Create a slot to schedule the video to be played
        # at the specified time
        slot = CoreObject('slot', {
            'externalId': slot_external_id,
            'type': 'regular',
            'startTime': start_time.isoformat(),
            # End time left empty as we want this slot to last until the next.
            'videoId': video_id,
            'streamId': stream_id
        })
        upsert_slot(slot)


def upsert_slot(slot, **kwargs):
    return upsert_object(slot, **kwargs);

def upsert_collection(collection, **kwargs):
    return upsert_object(collection, **kwargs)

def upsert_video(video, **kwargs):
    return upsert_object(video, **kwargs)

def upsert_object(obj, **kwargs):
    external_obj = getattr(api, 'fetch_{}_by_external_id'.format(obj.type))(obj.properties['externalId'])
    if external_obj is None:
        log.info('creating {}: '.format(obj.type) + str(obj.properties))
        new_obj = getattr(api, 'create_{}'.format(obj.type))(obj.properties, **kwargs)
        return new_obj['id']
    else:
        # Attach the actual object ID to the one that we are gonna update.
        obj.properties['id'] = external_obj['id']
        log.info('{} already existed, updating it: '.format(obj.type) + str(obj.properties))
        new_obj = getattr(api, 'update_{}'.format(obj.type))(obj.properties, **kwargs)
        return new_obj['id']

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Import EPG and As-Run data from RUV to OZ')
    parser.add_argument('-v', help='turn on verbose mode', action='store_true')
    parser.add_argument('action', help='epg or asrun')
    parser.add_argument('channel', help='The ID of the channel being imported to')
    parser.add_argument('stream', help='The ID of the stream being imported to')
    parser.add_argument('station', help='The external id of the service in RUV\'s system')
    args = parser.parse_args()
    api.channel_id = args.channel
    if args.v:
        log.setLevel(logging.DEBUG)
        log.info('verbose mode on')
    # Do this thing!
    if args.action == 'epg':
        import_epg(args.stream)
    elif args.action == 'asrun':
        import_as_run()
    else:
        raise Exception('unsupported operation')
