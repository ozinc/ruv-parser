import requests

class OZCoreApi:
    BASE_URL = 'https://core.oz.com'

    def __init__(self, access_token, channel_id):
        self.access_token = access_token
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

    def update_collection(self, collection):
        url = '{0}/channels/{1}/collections/{2}'.format(self.BASE_URL, self.channel_id, collection['id'])
        return self._update_object_at_uri(collection, url)

    def update_video(self, video):
        url = '{0}/channels/{1}/videos/{2}'.format(self.BASE_URL, self.channel_id, video['id'])
        return self._update_object_at_uri(video, url)

    def _update_object_at_uri(self, obj, uri):
        r = requests.patch(uri, data=obj, headers={'Authorization': 'Bearer ' + self.access_token})
        if r.status_code is 200:
            return r.json()['data']
        elif r.status_code is 404:
            return None
        else:
            print(r.content)
            raise Exception('an error occurred when updating an object, status was: {0}'.format(r.status_code))

    def _create_object_at_uri(self, obj, uri):
        r = requests.post(uri, data=obj, headers={'Authorization': 'Bearer ' + self.access_token})
        if r.status_code is 201:
            return r.json()['data']
        else:
            raise Exception('an error occurred when creating an object, status was: {0}'.format(r.status_code))

    def _fetch_object_at_uri(self, uri):
        r = requests.get(uri, headers={'Authorization': 'Bearer ' + self.access_token})
        if r.status_code is 200:
            videos = r.json()['data']
            if len(videos) is 0:
                return None
            else:
                return videos[0]
        else:
            raise Exception('an error occurred when fetching collection, status was: {0}'.format(r.status_code))
