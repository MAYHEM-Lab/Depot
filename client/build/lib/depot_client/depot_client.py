import requests


class DepotClient:
    def __init__(self, depot_destination, access_key):
        self.depot_destination = depot_destination
        self.access_key = access_key

    def cluster(self, entity: str = None, cluster: str = None):
        url = f'{self.depot_destination}/api/clusters'
        if entity is not None and cluster is not None:
            url += f'/{entity}/{cluster}'
        r = requests.get(
            url,
            headers={'access_key': self.access_key}
        )
        info = r.json()
        assert r.status_code == 200, f'Unable to access cluster {entity}/{cluster}'
        return info

    def locate_version(self, entity: str, tag: str, version: int):
        r = requests.get(
            f'{self.depot_destination}/api/entity/{entity}/datasets/{tag}/segments/{version}/locate',
            headers={'access_key': self.access_key}
        )
        info = r.json()
        assert r.status_code == 200, f'Unable to access segment {entity}/{tag}@{version}'
        return info

    def locate_dataset(self, entity: str, tag: str):
        r = requests.get(
            f'{self.depot_destination}/api/entity/{entity}/datasets/{tag}/locate',
            headers={'access_key': self.access_key}
        )
        assert r.status_code == 200, f'Unable to access dataset {tag}'
        segment = r.json()
        return segment

    def fail_segment(self, entity: str, tag: str, version: int, cause: str, error_message: str):
        r = requests.post(
            f'{self.depot_destination}/api/entity/{entity}/datasets/{tag}/segments/{version}/fail', json={
                'cause': cause,
                'error_message': error_message
            },
            headers={'access_key': self.access_key}
        )
        assert r.status_code == 201

    def commit_segment(self, entity: str, tag: str, version: int, path: str, rows: int, sample):
        r = requests.post(
            f'{self.depot_destination}/api/entity/{entity}/datasets/{tag}/segments/{version}/commit', json={
                'path': path,
                'rows': rows,
                'sample': sample
            },
            headers={'access_key': self.access_key}
        )
        assert r.status_code == 201

    def get_notebooks(self, owner):
        r =   requests.get(f'{self.depot_destination}/api/entity/${owner}/notebooks/topic/')
        notebooks = r.json()
        return notebooks