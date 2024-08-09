from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio.session import AsyncSession, async_sessionmaker

from app.core.config import settings
from helper.logger_helper import LoggerSimple

logger = LoggerSimple(name=__name__).logger
DATABASE_URI = settings.MYSQL_URI
DATABASE_PREFIX = settings.MYSQL_ASYNC_PREFIX
DATABASE_URL = f"{DATABASE_PREFIX}{DATABASE_URI}"

is_show_echo = False
if settings.DEBUG:
    is_show_echo = True
async_engine = create_async_engine(DATABASE_URL, echo=is_show_echo, future=True)

async_session = async_sessionmaker(bind=async_engine, class_=AsyncSession, expire_on_commit=False)


async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    async with async_session() as db:
        # yield db
        try:
            yield db
            await db.commit()
        except Exception as e:
            await db.rollback()
            raise e
        finally:
            await db.close()
            await async_engine.dispose()
            logger.info(f'Close connection to DB')
