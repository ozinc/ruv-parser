import requests

class OZCoreApi:
    BASE_URL = 'https://core.oz.com'

    def __init__(self, access_token, channel_id):
        self.access_token = access_token
        self.channel_id = channel_id

    def fetch_collection_by_external_id(self, external_id):
        url = '{0}/channels/{1}/collections?externalId={2}&all=true'.format(self.BASE_URL, self.channel_id, external_id)
        r = requests.get(url, headers={'Authorization': 'Bearer ' + self.access_token})
        if r.status_code is 200:
            videos = r.json()['data']
            if len(videos) is 0:
                return None
            else:
                return videos[0]
        else:
            raise Exception('An error occurred when fetching collection, status was: {0}'.format(r.status_code))

    def fetch_video_by_external_id(self, external_id):
        url = '{0}/channels/{1}/videos?externalId={2}&all=true'.format(self.BASE_URL, self.channel_id, external_id)
        r = requests.get(url, headers={'Authorization': 'Bearer ' + self.access_token})
        if r.status_code is 200:
            videos = r.json()['data']
            if len(videos) is 0:
                return None
            else:
                return videos[0]
        else:
            raise Exception('An error occurred when fetching collection, status was: {0}'.format(r.status_code))

    def create_collection(self, collection):
        url = '{0}/channels/{1}/collections'.format(self.BASE_URL, self.channel_id)
        r = requests.post(url, data=collection, headers={'Authorization': 'Bearer ' + self.access_token})
        if r.status_code is 201:
            return r.json()['data']
        else:
            raise Exception('An error occurred when creating collection, status was: {0}'.format(r.status_code))

    def create_video(self, video):
        url = '{0}/channels/{1}/videos'.format(self.BASE_URL, self.channel_id)
        r = requests.post(url, data=video, headers={'Authorization': 'Bearer ' + self.access_token})
        if r.status_code is 201:
            return r.json()['data']
        else:
            raise Exception('An error occurred when creating collection, status was: {0}'.format(r.status_code))
