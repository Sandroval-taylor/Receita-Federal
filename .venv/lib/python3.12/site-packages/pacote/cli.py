from httpx import get


def cli():
    print(
        get(
            'http://httpbin.org/get?arg=Live%20de%20Python'
        ).json()['args']['arg']
    )
