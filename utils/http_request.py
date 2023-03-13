import json

from aiohttp import ClientSession


async def http_request(
    method: str,
    url: str,
    headers: str | None = None,
    payload: str | None = None,
    query_params: str | None = None,
    payload_decode: bool = False,
) -> tuple:
    if not headers:
        headers = {}

    if not query_params:
        query_params = {}

    if not payload:
        payload = {}

    if isinstance(payload, dict) and payload_decode:
        payload = json.dumps(payload)

    async with ClientSession() as client:
        match method:
            case "POST":
                async with client.post(
                    url, headers=headers, data=payload, params=query_params
                ) as resp:
                    if resp.headers["Content-Type"] in [
                        "application/json",
                        "application/json; charset=UTF-8",
                    ]:
                        return resp.status, await resp.json(), resp.cookies
                    elif resp.headers["Content-Type"] == "text/html":
                        return resp.status, await resp.text(), resp.cookies

            case "GET":
                async with client.get(
                    url, headers=headers, params=query_params
                ) as resp:
                    if resp.headers["Content-Type"] == "application/json":
                        return resp.status, await resp.json(), resp.cookies
                    elif resp.headers["Content-Type"] == "text/csv":
                        return resp.status, await resp.text(), resp.cookies
            case "PUT":
                async with client.put(
                    url, headers=headers, data=json.dumps(payload)
                ) as resp:
                    return resp.status, await resp.json(), resp.cookies

            case "DELETE":
                async with client.delete(
                    url, headers=headers, params=query_params
                ) as resp:
                    return resp.status, await resp.json(), resp.cookies
