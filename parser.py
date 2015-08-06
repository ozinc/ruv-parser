from __future__ import print_function

import requests
import bs4 as bs

from oz_core_api import OZCoreApi

EPG_URL = 'http://muninn.ruv.is/files/rs/ruv/';
AS_RUN_URL = 'http://muninn.ruv.is/files/rstiming/ruv/';

api = OZCoreApi('9f5d3f4900000f5bc8f73db9d677c48478bc09cb', '9f16f362-abad-4042-9e26-a69759347bd9')

def import_as_run():
    print('> importing RUV AS-RUN from:', AS_RUN_URL)
    r = requests.get(AS_RUN_URL)
    if r.status_code is not 200:
        raise Exception('Unable to fetch EPG from RUV, status was: {0}'.format(r.status_code))
    soup = bs.BeautifulSoup(r.content, 'xml')
    events = soup.findAll('event')
    print('> found {0} as run items'.format(len(events)))
    for event in events:
        collection_id = None
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
    print('> importing RUV EPG from:', EPG_URL)
    r = requests.get(EPG_URL)
    if r.status_code is not 200:
        raise Exception('Unable to fetch EPG from RUV, status was: {0}'.format(r.status_code))
    soup = bs.BeautifulSoup(r.content, 'xml')
    events = soup.findAll('event')
    print('> found {0} scheduled items'.format(len(events)))
    for event in events:
        # For some reason RUV declares a "materialIdentifier" which is a
        # combination of serieId and episodeNumber so we have to parse that.
        materialIdentifier = event.find('material-identifier').text
        collection_id = None
        if materialIdentifier:
            # Get rid of leading zero.
            serieId = str(int(materialIdentifier.split('-')[0]))

            # Populate the collection object
            collection = {
                'externalId': 'ruv_' + serieId,
                'type': 'series', # TODO: You shouldn't need to do this.
                'name': event.title.text
            }
            collection_id = import_collection(collection)
            print('collection_id:', collection_id)

        # Create the video:
        import_video({
            'title': event.title.text,
            'externalId': 'ruv_' + event.id.text,
            'collectionId': collection_id
        })


def import_collection(collection):
    external_collection = api.fetch_collection_by_external_id(collection['externalId'])
    if external_collection is None:
        print('creating collection:', collection)
        new_collection = api.create_collection(collection)
        return new_collection['id']
    else:
        print('updating collection:', collection)
        return external_collection['id']

def import_video(video):
    external_video = api.fetch_video_by_external_id(video['externalId'])
    if external_video is None:
        print('creating video:', video)
        return api.create_video(video)
    else:
        print('video already existed, doing nothing...')
        return None

if __name__ == '__main__':
    try:
        import_epg()
    except Exception as e:
        print(e)
