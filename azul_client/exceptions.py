"""Handle and register api exceptions designed to allow for more explicit message types to be added overtime."""

import httpx


class BadResponse(Exception):
    """The client received a bad http status code from the server."""

    def __init__(self, resp: httpx.Response, *args):
        message = f"{resp.url} - {resp.status_code} - {resp.content}"
        super().__init__(message, *args)


class BadResponse404(Exception):
    """The client received a bad http status code from the server."""

    def __init__(self, resp: httpx.Response, *args):
        message = f"{resp.url} - {resp.status_code} - {resp.content}"
        super().__init__(message, *args)


def bad_response(resp: httpx.Response) -> BadResponse | BadResponse404:
    """Raise a formatted exception based on an http response's status code.

    :return BadResponse | BadResponse404
    """
    # If you want a different status code e.g 500 to be explicitly raised this makes it easy to add later.
    if resp.status_code == 404:
        return BadResponse404(resp)
    return BadResponse(resp)
