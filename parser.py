from __future__ import print_function

import requests
import bs4 as bs

from oz_core_api import OZCoreApi

EPG_URL = 'http://muninn.ruv.is/files/rstiming/ruv/';
AS_RUN_URL = '' # TODO: This.

api = OZCoreApi('9f5d3f4900000f5bc8f73db9d677c48478bc09cb', '82b990af-f4e6-4f98-849a-21b66ca2f5d3')

def import_epg():
    r = requests.get(EPG_URL)
    if r.status_code is 200:
        soup = bs.BeautifulSoup(r.content, 'xml')
        events = soup.findAll('event')
        print('> found {0} events'.format(len(events)))
        for event in events:
            # For some reason RUV declares a "materialIdentifier" which is a
            # combination of serieId and episodeNumber so we have to parse that.
            materialIdentifier = event.find('material-identifier').text
            if materialIdentifier:
                # Get rid of leading zero.
                serieId = str(int(materialIdentifier.split('-')[0]))

                # Populate the collection object
                collection = {
                    'externalId': serieId,
                    'type': 'series', # TODO: You shouldn't need to do this.
                    'name': event.title.text
                }
                import_collection(collection)

            #import_video(...)
    else:
        raise Exception('Unable to fetch EPG from RUV, status was: {0}'.format(r.status_code))

def import_collection(collection):
    external_collection = api.fetch_collection_by_external_id(collection['externalId'])
    if external_collection is None:
        print(api.create_collection(collection))

# main():
try:
    import_epg()
except Exception as e:
    print(e)