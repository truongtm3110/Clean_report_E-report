import uuid
from typing import Optional

import httpx
from fastapi import Depends, Request
from fastapi.openapi.models import Response
from fastapi_users import BaseUserManager, UUIDIDMixin
from custom.fastapi_users import FastAPIUsers
from fastapi_users import schemas
from fastapi_users.authentication import (
    AuthenticationBackend,
    BearerTransport,
    JWTStrategy,
)
from fastapi_users.authentication.strategy import AccessTokenDatabase, DatabaseStrategy
from fastapi_users_db_sqlalchemy import SQLAlchemyUserDatabase
# from fastapi_users.db import SQLAlchemyUserDatabase
# from fastapi_users_db_sqlmodel import SQLModelUserDatabaseAsync
# from fastapi_users_db_sqlmodel.access_token import SQLModelAccessTokenDatabaseAsync
from httpx_oauth.clients.google import GoogleOAuth2
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.core.config import settings
from app.db.database import get_async_session
from app.models.models_ereport.user import User
from app.models.models_ereport.access_token import AccessToken
from app.models.models_ereport.oauth_account import OAuthAccount
from app.schemas.subscription.quota_remain_schema import CurrentPlan
from app.schemas.user.user import UserInfo
from app.service.plan.plan_service import get_user_plan
from app.service.quota.quota_service import get_user_quota
from app.service.user_service.custom_accesstoken_sqlmodel import SQLModelAccessTokenDatabaseAsync
from app.service.user_service.custom_user_sqlmodel import SQLModelUserDatabaseAsync
from helper.error_helper import log_error
from helper.fastapi_helper import get_client_ip

# from app.models.models_ereport.user_sqlmodel import AccessToken, OAuthAccount, User

from helper.logger_helper import LoggerSimple

logger = LoggerSimple(name=__name__).logger
google_oauth_client = GoogleOAuth2(settings.OAUTH_GOOGLE_CLIENT_ID, settings.OAUTH_GOOGLE_CLIENT_SECRET)


class UserRead(schemas.BaseUser[uuid.UUID]):
    pass


class UserCreate(schemas.BaseUserCreate):
    pass


class UserUpdate(schemas.BaseUserUpdate):
    pass


async def get_user_detail_oauth(token: str, oauth_name: str = 'google'):
    if oauth_name == 'google':
        PROFILE_ENDPOINT = "https://people.googleapis.com/v1/people/me"
        params = {
            "personFields": "addresses,ageRanges,biographies,birthdays,calendarUrls,clientData,coverPhotos,emailAddresses,events,externalIds,genders,imClients,interests,locales,locations,memberships,metadata,miscKeywords,names,nicknames,occupations,organizations,phoneNumbers,photos,relations,sipAddresses,skills,urls,userDefined"}
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    url=PROFILE_ENDPOINT,
                    params=params,
                    headers={"Accept": "application/json", "Authorization": f"Bearer {token}"},
                )
                if response.status_code == 200:
                    response_data = response.json()

                    display_name = None
                    family_name = None
                    given_name = None
                    url_thumbnail = None
                    email = None

                    lst_name_primary = [nameObj for nameObj in response_data.get('names') if
                                        nameObj.get('metadata', {}).get('primary')]
                    name_primary = lst_name_primary[0] if lst_name_primary and len(lst_name_primary) > 0 else None
                    if name_primary:
                        display_name = name_primary.get('displayName')
                        family_name = name_primary.get('familyName')
                        given_name = name_primary.get('givenName')
                    lst_photo_primary = [photoObj for photoObj in response_data.get('photos') if
                                         photoObj.get('metadata', {}).get('primary')]
                    photo_primary = lst_photo_primary[0] if len(lst_photo_primary) > 0 else None
                    if photo_primary:
                        url_thumbnail = photo_primary.get('url')
                    lst_email = response_data.get('emailAddresses')
                    email_primary = lst_email[0] if len(lst_email) > 0 else None
                    if email_primary:
                        email = email_primary.get('value')
                    lst_email_alias = [emailObj.get('value') for emailObj in lst_email if
                                       not emailObj.get('metadata', {}).get('primary', False)]
                    profile = {
                        'display_name': display_name,
                        'family_name': family_name,
                        'given_name': given_name,
                        'url_thumbnail': url_thumbnail,
                        'email': email,
                        'lst_email_alias': lst_email_alias
                    }
                    # logger.info(response.text)
                    # logger.info(json.dumps(profile, ensure_ascii=False))
                    return profile
        except Exception as e:
            log_error(e)
    return None


async def get_user_db(session: AsyncSession = Depends(get_async_session)):
    # yield SQLModelUserDatabase(session, User)
    # yield SQLModelUserDatabaseAsync(session, User)
    yield SQLModelUserDatabaseAsync(session, User, OAuthAccount)


async def get_access_token_db(
        session: AsyncSession = Depends(get_async_session),
):
    yield SQLModelAccessTokenDatabaseAsync(session, AccessToken)


SECRET = settings.OAUTH_SECRET_JWT


# class UserManager(UUIDIDMixin, BaseUserManager[User, uuid.UUID]):
class UserManager(BaseUserManager[User, int]):
    reset_password_token_secret = SECRET
    verification_token_secret = SECRET

    def parse_id(self, value) -> int:
        return int(value)

    async def on_after_register(self, user: User, request: Optional[Request] = None):
        print(f"User {user.id} has registered.")

    async def on_after_login(
            self, user: User, request: Optional[Request] = None, response: Optional[Response] = None
    ):
        logger.info(f"Login id={user.id} - {user.email}")
        if user.display_name is None or len(user.display_name) == 0:
            oauth_accounts = user.oauth_accounts
            if oauth_accounts and len(oauth_accounts) > 0:
                oauth_account = oauth_accounts[0]
                profile = await get_user_detail_oauth(token=oauth_account.access_token,
                                                      oauth_name=oauth_account.oauth_name)
                await self.user_db.update(user, {
                    'display_name': profile.get('display_name'),
                    'family_name': profile.get('family_name'),
                    'given_name': profile.get('given_name'),
                    'url_thumbnail': profile.get('url_thumbnail'),
                    'email': profile.get('email'),
                    'lst_email_alias': profile.get('lst_email_alias'),
                    'ip': get_client_ip(request)
                })
                logger.info(f'update user info - {profile}')


async def get_user_manager(user_db: SQLAlchemyUserDatabase = Depends(get_user_db)):
    yield UserManager(user_db)


bearer_transport = BearerTransport(tokenUrl="auth/jwt/login")


def get_jwt_strategy() -> JWTStrategy:
    return JWTStrategy(secret=SECRET, lifetime_seconds=settings.OAUTH_MAX_LIFETIME_TOKEN,
                       token_audience=["metric-users:auth"])


def get_database_strategy(
        access_token_db: AccessTokenDatabase[AccessToken] = Depends(get_access_token_db),
) -> DatabaseStrategy:
    return DatabaseStrategy(access_token_db, lifetime_seconds=settings.OAUTH_MAX_LIFETIME_TOKEN)


auth_backend = AuthenticationBackend(
    name="jwt",
    transport=bearer_transport,
    # get_strategy=get_jwt_strategy,
    get_strategy=get_database_strategy,
)

fastapi_users = FastAPIUsers[User, uuid.UUID](get_user_manager, [auth_backend])

current_active_user = fastapi_users.current_user(active=True)


async def get_user_profile_from_db(user: User, session: AsyncSession) -> UserInfo:
    statement = (select(User).where(User.id == user.id))
    user_profile, = (await session.execute(statement)).first()

    # Get current plan
    user_quota = await get_user_quota(session, user.id)
    plan, subscription = await get_user_plan(user.id, session)

    current_plan = CurrentPlan(
        plan_id=plan.id if plan else None,
        plan_name=plan.name if plan else None,
        plan_code=plan.plan_code if plan else 'e_community',
        expired_at=subscription.subscription_end_at if plan else None,
        remain_claim=user_quota.remain_claim if user_quota else 0,
        remain_claim_basic=user_quota.remain_claim_basic if user_quota else 0,
        remain_claim_pro=user_quota.remain_claim_pro if user_quota else 0,
        remain_claim_expert=user_quota.remain_claim_expert if user_quota else 0
    )

    return UserInfo(
        id=user_profile.id,
        email=user_profile.email,
        display_name=user_profile.display_name,
        first_name=user_profile.given_name,
        last_name=user_profile.family_name,
        avatar=user_profile.url_thumbnail,
        current_plan=current_plan
    )


async def get_customer_info_from_db(email: str, session: AsyncSession) -> UserInfo:
    statement = (select(User).where(User.email == email))
    user_profile, = (await session.execute(statement)).first()

    # Get current plan
    user_quota = await get_user_quota(session, user_profile.id)
    plan, subscription = await get_user_plan(user_profile.id, session)

    current_plan = CurrentPlan(
        plan_id=plan.id if plan else None,
        plan_name=plan.name if plan else None,
        plan_code=plan.plan_code if plan else 'e_community',
        expired_at=subscription.subscription_end_at if plan else None,
        remain_claim=user_quota.remain_claim if user_quota else 0,
        remain_claim_basic=user_quota.remain_claim_basic if user_quota else 0,
        remain_claim_pro=user_quota.remain_claim_pro if user_quota else 0,
        remain_claim_expert=user_quota.remain_claim_expert if user_quota else 0
    )

    return UserInfo(
        id=user_profile.id,
        email=user_profile.email,
        display_name=user_profile.display_name,
        first_name=user_profile.given_name,
        last_name=user_profile.family_name,
        avatar=user_profile.url_thumbnail,
        current_plan=current_plan
    )