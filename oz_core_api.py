import os

import requests

class OZCoreApi:
    BASE_URL = 'https://core.oz.com'

    def __init__(self, username, password, channel_id):
        required_env = ['OZ_CLIENT_ID', 'OZ_CLIENT_SECRET']
        for var in required_env:
            if not os.environ.has_key(var):
                raise Exception('The required variable {0} was not set.'.format(var))

        self.client_id, self.client_secret = map(lambda var: os.environ[var], required_env)

        self.access_token = self._authenticate_user(username, password)
        self.channel_id = channel_id

    def fetch_collection_by_external_id(self, external_id):
        url = '{0}/channels/{1}/collections?externalId={2}&all=true'.format(self.BASE_URL, self.channel_id, external_id)
        return self._fetch_object_at_uri(url)

    def fetch_video_by_external_id(self, external_id):
        url = '{0}/channels/{1}/videos?externalId={2}&all=true'.format(self.BASE_URL, self.channel_id, external_id)
        return self._fetch_object_at_uri(url)

    def create_collection(self, collection):
        url = '{0}/channels/{1}/collections'.format(self.BASE_URL, self.channel_id)
        return self._create_object_at_uri(collection, url)

    def create_video(self, video):
        url = '{0}/channels/{1}/videos'.format(self.BASE_URL, self.channel_id)
        return self._create_object_at_uri(video, url)

    def _create_object_at_uri(self, obj, uri):
        r = requests.post(uri, data=obj, headers={'Authorization': 'Bearer ' + self.access_token})
        if r.status_code is 201:
            return r.json()['data']
        else:
            raise Exception('An error occurred when creating collection, status was: {0}'.format(r.status_code))

    def _fetch_object_at_uri(self, uri):
        r = requests.get(uri, headers={'Authorization': 'Bearer ' + self.access_token})
        if r.status_code is 200:
            videos = r.json()['data']
            if len(videos) is 0:
                return None
            else:
                return videos[0]
        else:
            raise Exception('An error occurred when fetching collection, status was: {0}'.format(r.status_code))

    def _authenticate_user(self, username, password):
        url = '{0}/oauth2/token'.format(self.BASE_URL)
        data = {
            'username': username,
            'password': password,
            'grant_type': 'password',
            'client_id': self.client_id,
            'client_secret': self.client_secret
        }
        r = requests.post(url, data=data)
        if r.status_code is 200:
            return r.json()['access_token']
        else:
            raise Exception('An error occurred when fetching collection, status was: {0}'.format(r.status_code))
