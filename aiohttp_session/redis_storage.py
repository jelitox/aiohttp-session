import asyncio
import json
import uuid
from distutils.version import StrictVersion
from typing import Any, Callable, Optional

from aiohttp import web

from . import AbstractStorage, Session

try:
    import aioredis
except ImportError:  # pragma: no cover
    aioredis = None  # type: ignore[assignment]


class RedisStorage(AbstractStorage):
    """Redis storage"""

    def __init__(
        self,
        redis_pool: "aioredis.Redis", *,
        cookie_name: str = "AIOHTTP_SESSION",
        domain: Optional[str] = None,
        max_age: Optional[int] = None,
        path: str = '/',
        secure: Optional[bool] = None,
        httponly: bool = True,
        key_factory: Callable[[], str] = lambda: uuid.uuid4().hex,
        encoder: Callable[[object], str] = json.dumps,
        decoder: Callable[[str], Any] = json.loads
    ) -> None:
        super().__init__(cookie_name=cookie_name, domain=domain,
                         max_age=max_age, path=path, secure=secure,
                         httponly=httponly,
                         encoder=encoder, decoder=decoder)
        if aioredis is None:
            raise RuntimeError("Please install aioredis")
        if StrictVersion(aioredis.__version__).version < (2, 0):
            raise RuntimeError("aioredis<2.0 is not supported")
        self._key_factory = key_factory
        if isinstance(redis_pool, aioredis.ConnectionPool):
            self._redis = aioredis.Redis(connection_pool=redis_pool)
        elif isinstance(redis_pool, aioredis.Redis):
            self._redis = redis_pool
        else:
            raise TypeError("Expected aioredis.Redis got {}".format(type(redis_pool)))

    def __del__(self):
        asyncio.run(self._redis.close())

    async def load_session(self, request: web.Request) -> Session:
        cookie = self.load_cookie(request)
        if cookie is None:
            return Session(None, data=None, new=True, max_age=self.max_age)
        else:
            try:
                async with await self._redis as conn:
                    key = str(cookie)
                    data = await conn.get(self.cookie_name + '_' + key)
                    if data is None:
                        return Session(None, data=None,
                                       new=True, max_age=self.max_age)
                    try:
                        data = self._decoder(data)
                    except ValueError:
                        data = None
                    return Session(key, data=data,
                                   new=False, max_age=self.max_age)
            except Exception as err:
                raise

    async def save_session(
        self,
        request: web.Request,
        response: web.StreamResponse,
        session: Session
    ) -> None:
        key = session.identity
        if key is None:
            key = self._key_factory()
            self.save_cookie(response, key,
                             max_age=session.max_age)
        else:
            if session.empty:
                self.save_cookie(response, '',
                                 max_age=session.max_age)
            else:
                key = str(key)
                self.save_cookie(response, key,
                                 max_age=session.max_age)

        data = self._encoder(self._get_session_data(session))
        await self._redis.set(self.cookie_name + '_' + key, data, ex=session.max_age)  # type: ignore[arg-type] # noqa: B950
