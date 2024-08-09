from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.models.models_ereport import User, UserClaimReport, Report, ReportCategory, CacheValue
from app.schemas.report.response_report_list import BasicInfoReport
from app.service.universal.category_service import get_breadcrumb
from helper.error_helper import log_error


async def get_list_claimed_report_by_user(
        session: AsyncSession,
        user: User,
        offset: int = 0,
        limit: int = 10
):
    try:
        statement = (select(Report.id,
                            Report.name,
                            Report.slug,
                            Report.status,
                            UserClaimReport.claimed_at,
                            UserClaimReport.expired_at,
                            Report.url_thumbnail,
                            Report.category_report_id,
                            Report.start_date,
                            Report.end_date,
                            Report.report_type
                            )
                     .join(UserClaimReport)
                     .where(UserClaimReport.user_id == user.id))
        result = await session.execute(statement)
        reports = []
        seen_reports = set()
        for row in result.all():
            if row[0] in seen_reports:
                continue
            seen_reports.add(row[0])

            key = f'{row[0]}_{row[0]}_web'
            statement = select(CacheValue).where(CacheValue.key == key)
            cache_value = (await session.execute(statement)).scalar_one_or_none()
            lst_brand = [brand.get('name') for brand in cache_value.value.get('by_brand', {}).get(
                'lst_top_brand_revenue', [])] if cache_value is not None else []

            statement = select(ReportCategory).where(ReportCategory.id == row[7])
            category_report = (await session.execute(statement)).scalar_one_or_none()
            lst_category = get_breadcrumb(category_base_id=row[7])

            reports.append(BasicInfoReport(id=row[0],
                                           name=row[1],
                                           slug=row[2],
                                           status=row[3],
                                           claimed_at=row[4],
                                           expired_at=row[5],
                                           lst_brand=lst_brand,
                                           url_thumbnail=row[6],
                                           start_date=row[8],
                                           end_date=row[9],
                                           lst_category=lst_category,
                                           report_type=row[10],
                                           category_report_name=category_report.name if category_report else None,
                                           category_report_id=category_report.id if category_report else None))

        return reports[offset:offset + limit]
    except Exception as e:
        log_error(e)
        return []


async def count_list_claimed_report_by_user(
        session: AsyncSession,
        user: User,
):
    try:
        statement = (select(Report.id)
                     .join(UserClaimReport)
                     .where(UserClaimReport.user_id == user.id)
                     .distinct())
        result = await session.execute(statement)
        unique_reports = result.fetchall()
        return len(unique_reports)
    except Exception as e:
        log_error(e)
        return 0


async def get_recommended_reports(
        category_report_id: str = None,
        number_of_reports: int = 5,
        session: AsyncSession = None
):
    statement = (select(Report)
                 .where(Report.category_report_id == category_report_id if category_report_id is not None else True)
                 .order_by(Report.search_volume_shopee.desc())
                 .limit(number_of_reports))
    _recommended_reports = (await session.execute(statement)).all()

    category_report = None
    if category_report_id:
        statement = (select(ReportCategory).where(ReportCategory.id == category_report_id))
        try:
            category_report = (await session.execute(statement)).scalar_one()
        except Exception as e:
            log_error(e)

    recommended_report = []
    for _report in _recommended_reports:
        report, = _report
        recommended_report.append(BasicInfoReport(
            id=report.id,
            name=report.name,
            slug=report.slug,
            search_volume_shopee=report.search_volume_shopee,
            url_thumbnail=report.url_thumbnail,
            start_date=report.start_date,
            category_report_id=category_report.id if category_report else None,
            category_report_name=category_report.name if category_report else None,
        ))
    return recommended_report


async def marketing_report_recommend(
        number_of_reports: int = 5,
        report_type: str = None,
        session: AsyncSession = None
):
    if not report_type:
        statement = (select(Report)
                     .where(Report.source == 'marketing')
                     .order_by(Report.search_volume_shopee.desc())
                     .limit(number_of_reports))
    else:
        statement = (select(Report)
                     .where(Report.source == 'marketing', Report.report_type == report_type)
                     .order_by(Report.search_volume_shopee.desc())
                     .limit(number_of_reports))

    _recommended_reports = (await session.execute(statement)).all()

    recommended_report = []
    for _report in _recommended_reports:
        report, = _report
        recommended_report.append(BasicInfoReport(
            id=report.id,
            name=report.name,
            slug=report.slug,
            search_volume_shopee=report.search_volume_shopee,
            source=report.source,
            url_thumbnail=report.url_thumbnail,
            start_date=report.start_date,
            category_report_name=report.report_type
        ))
    return recommended_report
