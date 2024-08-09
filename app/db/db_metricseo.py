import asyncio
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio.session import AsyncSession, async_sessionmaker

from app.core.config import settings
from app.core.config_metricseo import settings_seo

from helper.logger_helper import LoggerSimple

logger = LoggerSimple(name=__name__).logger
is_show_echo = False
if settings.DEBUG:
    is_show_echo = True
async_engine_seo = create_async_engine(settings_seo.POSTGRESQL_URI_METRICSEO, echo=is_show_echo, future=True)

async_session_seo = async_sessionmaker(bind=async_engine_seo, class_=AsyncSession, expire_on_commit=False)


async def get_async_session_seo() -> AsyncGenerator[AsyncSession, None]:
    async with async_session_seo() as db:
        # yield db
        try:
            yield db
            await db.commit()
        except Exception as e:
            await db.rollback()
            raise e
        finally:
            await db.close()
            await async_engine_seo.dispose()
            logger.info(f'Close connection to DB')


if __name__ == '__main__':
    from helper.alchemy_async_helper import select_by_query


    async def run_demo():
        query = 'select * from abc limit 10'
        # async for connection in get_async_session_seo():
        async with async_session_seo() as connection:
            lst_record = await select_by_query(query=query, connection=connection)


    asyncio.run(run_demo())
