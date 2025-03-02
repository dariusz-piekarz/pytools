from httpx import AsyncClient, Response
from loguru import logger


@logger.catch
async def post_request(aclient: AsyncClient, url: str, parameters: any,
                       auth: tuple[str, str] | None = None) -> Response:
    response: Response

    if auth is None:
        response = await aclient.post(url, json=parameters)
    else:
        response = await aclient.post(url, json=parameters, auth=auth)

    return response


@logger.catch
async def get_request(aclient: AsyncClient, base_url: str, parameters: dict[str: str | list[str]],
                      auth: tuple[str, str] | None = None) -> Response:
    response: Response
    if auth is None:
        response = await aclient.get(base_url, params=parameters)
    else:
        response = await aclient.get(base_url, params=parameters, auth=auth)
    return response


@logger.catch
async def put_request(aclient: AsyncClient, base_url: str, parameters: dict[str: str | list[str]],
                      auth: tuple[str, str] | None = None) -> Response:
    response: Response
    if auth is None:
        response = await aclient.put(base_url, params=parameters)
    else:
        response = await aclient.put(base_url, params=parameters, auth=auth)

    return response


@logger.catch
async def patch_request(aclient: AsyncClient, base_url: str, parameters: dict[str: str | list[str]],
                        auth: tuple[str, str] | None = None) -> Response:
    response: Response
    if auth is None:
        response = await aclient.patch(base_url, params=parameters)
    else:
        response = await aclient.patch(base_url, params=parameters, auth=auth)
    return response


@logger.catch
async def delete_request(aclient: AsyncClient, url: str, params: dict[str: str],
                         auth: tuple[str, str] | None = None) -> Response:
    response: Response
    if auth is None:
        response = await aclient.delete(url, params=params)
    else:
        response = await aclient.delete(url, params=params, auth=auth)
    return response
