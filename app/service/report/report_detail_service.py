import json
from datetime import datetime

from fastapi import HTTPException
from sqlalchemy import select, or_
from starlette.responses import JSONResponse

from app.models.models_ereport import Report, SubReport, UserClaimReport, CacheValue
from app.schemas.report.response_report_detail import ResponseReportDetail, ResponseReportDetailPdf
from app.schemas.report_basic.response_report_basic import ResponseReportDetailBasic
from app.schemas.report_free.response_report_free import ResponseReportDetailFree
from app.schemas.report_marketing.response_report_marketing import ResponseReportDetailMarketing
from app.schemas.report_pro.response_report_pro import ResponseReportDetailPro
from app.service.universal.category_service import get_breadcrumb
from schedule.report_service.build_es_query_service import FilterReport
from helper.logger_helper import LoggerSimple

logger = LoggerSimple(name=__name__).logger

DOWNLOAD_REPORT_SIGN_SECRET = 'secret_key_metric_111'


def __get_query_platform_from_search_body(search_body: dict) -> FilterReport:
    include_query = search_body.get('include_query')
    exclude_query = search_body.get('exclude_query')

    query_platform = FilterReport()

    if search_body.get('durationDay'):
        query_platform.duration_day = search_body.get('durationDay')

    # print('include_query', include_query)
    if len(include_query.get('platforms')) > 1:
        query_platform.lst_bee_category_base_id = include_query.get('bee_categories')
    else:
        query_platform.lst_category_base_id = include_query.get('categories')

    lst_keyword = []
    lst_keyword_required = []
    lst_keyword_exclude = []
    if include_query.get('queries'):
        for keyword in include_query.get('queries'):
            keyword = keyword.strip()
            if keyword.startswith('+'):
                lst_keyword_required.append(keyword[1:])
            elif keyword.startswith('-'):
                lst_keyword_exclude.append(keyword[1:])
            else:
                lst_keyword.append(keyword)

    if exclude_query.get('queries'):
        for keyword in exclude_query.get('queries'):
            keyword = keyword.strip()
            lst_keyword_exclude.append(keyword)

    query_platform.lst_platform_id = include_query.get('platforms')
    query_platform.lst_keyword = lst_keyword
    query_platform.lst_keyword_required = lst_keyword_required
    query_platform.lst_keyword_exclude = lst_keyword_exclude

    if include_query.get('brands'):
        query_platform.or_lst_brand = include_query.get('brands')

    if include_query.get('locations'):
        query_platform.locations = include_query.get('locations')

    if include_query.get('official_types'):
        query_platform.official_types = include_query.get('official_types')

    query_platform.is_smart_queries = include_query.get('is_smart_queries')
    query_platform.is_remove_fake_sale = include_query.get('is_remove_bad_product')
    query_platform.composite_compare_lst = include_query.get('composite_compare_lst')

    range_fields = [
        'order_count_30d_range',
        'price_range',
        'rating_count_range',
        'order_count_range',
        'platform_created_at_range',
        'order_count_7d_range',
        'order_count_90d_range',
        'order_count_180d_range',
        'revenue_range',
        'order_revenue_7d_range',
        'order_revenue_30d_range',
        'order_revenue_90d_range',
        'order_revenue_180d_range',
    ]

    for field in range_fields:
        if include_query.get(field):
            query_platform.__setattr__(field, {
                'begin': include_query.get(field).get('from'),
                'end': include_query.get(field).get('to'),
            })

    return query_platform


# def get_hidden_data_analytic(data_analytic: dict):


async def get_report_detail(
        slug: str = None,
        period: str = '12M',
        user=None,
        is_bot=False,
        connection: any = None
):
    statement = select(Report).filter(Report.slug == slug)
    _find_report = (await connection.execute(statement)).fetchall()

    if not _find_report:
        raise HTTPException(status_code=404, detail="Report not found")

    statement = select(
        SubReport.id.label('id'),
        SubReport.report_id.label('report_id'),
        SubReport.slug.label('slug'),
        SubReport.name.label('name'),
        SubReport.key_object.label('key_object'),
        SubReport.key.label('key'),
        SubReport.period.label('period'),
        SubReport.lst_platform.label('lst_platform'),
        SubReport.price_range.label('price_range'),
        SubReport.url_thumbnail.label('url_thumbnail'),
        SubReport.introduction.label('introduction'),
        SubReport.url_cover.label('url_cover'),
        SubReport.category_report_id.label('category_report_id'),
        SubReport.category_base_id.label('category_base_id'),
        SubReport.shopee_category_id.label('shopee_category_id'),
        SubReport.search_volume_shopee.label('search_volume_shopee'),
        SubReport.report_type.label('report_type'),
        SubReport.report_format.label('report_format'),
        SubReport.report_version.label('report_version'),
        SubReport.status.label('status'),
        SubReport.last_analytic_at.label('last_analytic_at'),
        SubReport.weight.label('weight'),
        SubReport.source.label('source'),
        SubReport.price.label('price'),
        SubReport.price_before_discount.label('price_before_discount'),
        SubReport.is_editor_pick.label('is_editor_pick'),
        SubReport.start_date.label('start_date'),
        SubReport.end_date.label('end_date'),
        SubReport.updated_at.label('updated_at'),
        SubReport.is_unsellable.label('is_unsellable'),
        SubReport.filter_default.label('filter_default'),
        SubReport.filter_custom.label('filter_custom'),
    ).where(
        SubReport.slug == slug and SubReport.period == period
    )

    _find_sub_report = (await connection.execute(statement)).fetchall()

    if not _find_sub_report:
        return JSONResponse(status_code=404, content={'message': 'Not found report detail'})

    sub_report, = _find_sub_report

    tier_report = 'e_community'
    if user:
        statement = select(
            UserClaimReport
        ).filter(
            UserClaimReport.user_id == user.id,
        ).filter(
            UserClaimReport.report_id == sub_report.report_id
        ).filter(
            UserClaimReport.expired_at > datetime.now()
        ).filter(
            or_(
                UserClaimReport.tier_report == 'e_pro',
                UserClaimReport.tier_report == 'e_basic',
                UserClaimReport.tier_report == 'e_trial',
            )
        )

        _find_user_claim_report = (await connection.execute(statement)).fetchall()
    else:
        _find_user_claim_report = None

    if _find_user_claim_report:
        tier_report = _find_user_claim_report[0][0].tier_report

    lst_category = get_breadcrumb(category_base_id=sub_report.category_report_id)

    data_analytic_statement = select(
        CacheValue.value.label('value')
    ).where(
        CacheValue.key == f'{sub_report.report_id}_{sub_report.id}_web'
    )
    _find_data_analytic = (await connection.execute(data_analytic_statement)).fetchall()

    data_analytic = None

    if _find_data_analytic:
        data_analytic = _find_data_analytic[0][0]

    if is_bot:
        ResponseReportDetail = ResponseReportDetailPro
    else:
        report_tier_response = {
            'e_community': ResponseReportDetailFree,
            'e_basic': ResponseReportDetailBasic,
            'e_pro': ResponseReportDetailPro,
            'e_trial': ResponseReportDetailPro
        }

        ResponseReportDetail = report_tier_response.get(tier_report, ResponseReportDetailFree)

    name = sub_report.name[0].upper() + sub_report.name[1:]
    if sub_report.report_type == 'report_category':
        name = f'Ngành hàng {name}'

    return ResponseReportDetail(
        id=sub_report.id,
        slug=sub_report.slug,
        status=sub_report.status,
        name=name,
        price=sub_report.price,
        tier_report=tier_report,
        can_download=False,
        introduction=sub_report.introduction,
        report_type=sub_report.report_type,
        report_format=sub_report.report_format,
        report_version=sub_report.report_version,
        url_thumbnail=sub_report.url_thumbnail,
        url_cover=sub_report.url_cover,
        category_report_id=sub_report.category_report_id,
        lst_category=lst_category,
        data_filter_report=sub_report.filter_custom or sub_report.filter_default,
        optimized_query=bool(sub_report.filter_custom),
        filter_custom={
            "lst_platform_id": [1, 2, 3, ],
            "start_date": sub_report.start_date,
            "end_date": sub_report.end_date
        },
        updated_at=sub_report.updated_at,
        data_analytic=data_analytic,
        is_unsellable=sub_report.is_unsellable
    )


async def get_report_detail_pdf(
        slug: str = None,
        connection: any = None
):
    statement = select(Report).filter(Report.slug == slug)
    _find_report = (await connection.execute(statement)).fetchall()

    if not _find_report:
        raise HTTPException(status_code=404, detail="Report not found")

    statement = select(
        SubReport.id.label('id'),
        SubReport.report_id.label('report_id'),
        SubReport.slug.label('slug'),
        SubReport.name.label('name'),
        SubReport.key_object.label('key_object'),
        SubReport.key.label('key'),
        SubReport.period.label('period'),
        SubReport.lst_platform.label('lst_platform'),
        SubReport.price_range.label('price_range'),
        SubReport.url_thumbnail.label('url_thumbnail'),
        SubReport.introduction.label('introduction'),
        SubReport.url_cover.label('url_cover'),
        SubReport.category_report_id.label('category_report_id'),
        SubReport.category_base_id.label('category_base_id'),
        SubReport.shopee_category_id.label('shopee_category_id'),
        SubReport.search_volume_shopee.label('search_volume_shopee'),
        SubReport.report_type.label('report_type'),
        SubReport.report_format.label('report_format'),
        SubReport.report_version.label('report_version'),
        SubReport.status.label('status'),
        SubReport.last_analytic_at.label('last_analytic_at'),
        SubReport.weight.label('weight'),
        SubReport.source.label('source'),
        SubReport.price.label('price'),
        SubReport.price_before_discount.label('price_before_discount'),
        SubReport.is_editor_pick.label('is_editor_pick'),
        SubReport.start_date.label('start_date'),
        SubReport.end_date.label('end_date'),
        SubReport.updated_at.label('updated_at'),
        SubReport.is_unsellable.label('is_unsellable'),
        SubReport.filter_default.label('filter_default'),
        SubReport.filter_custom.label('filter_custom'),
    ).where(
        SubReport.slug == slug and SubReport.period == '12M'
    )

    _find_sub_report = (await connection.execute(statement)).fetchall()

    if not _find_sub_report:
        return JSONResponse(status_code=404, content={'message': 'Not found report detail'})

    sub_report, = _find_sub_report

    data_analytic_statement = select(
        CacheValue.value.label('value')
    ).where(
        CacheValue.key == f'{sub_report.report_id}_{sub_report.id}_pdf'
    )
    _find_data_analytic = (await connection.execute(data_analytic_statement)).fetchall()

    data_analytic = None

    if _find_data_analytic:
        data_analytic = _find_data_analytic[0][0]

    name = sub_report.name[0].upper() + sub_report.name[1:]
    if sub_report.report_type == 'report_category':
        name = f'Ngành hàng {name}'

    # print(json.dumps(data_analytic, ensure_ascii=False))

    return ResponseReportDetailPdf(
        id=sub_report.id,
        slug=sub_report.slug,
        status=sub_report.status,
        name=name,
        price=sub_report.price,
        can_download=False,
        introduction=sub_report.introduction,
        report_type=sub_report.report_type,
        report_format=sub_report.report_format,
        report_version=sub_report.report_version,
        url_thumbnail=sub_report.url_thumbnail,
        url_cover=sub_report.url_cover,
        category_report_id=sub_report.category_report_id,
        data_filter_report=sub_report.filter_custom or sub_report.filter_default,
        optimized_query=bool(sub_report.filter_custom),
        filter_custom={
            "lst_platform_id": [1, 2, 3, ],
            "start_date": sub_report.start_date,
            "end_date": sub_report.end_date
        },
        updated_at=sub_report.updated_at,
        data_analytic=data_analytic,
        is_unsellable=sub_report.is_unsellable
    )


async def get_report_insight_detail(
        slug: str = None,
        period: str = '12M',
        is_bot=False,
        connection: any = None
):
    statement = select(Report).filter(Report.slug == slug)
    _find_report = (await connection.execute(statement)).fetchall()

    if not _find_report:
        raise HTTPException(status_code=404, detail="Report not found")

    statement = select(
        SubReport.id.label('id'),
        SubReport.report_id.label('report_id'),
        SubReport.slug.label('slug'),
        SubReport.name.label('name'),
        SubReport.key_object.label('key_object'),
        SubReport.key.label('key'),
        SubReport.period.label('period'),
        SubReport.lst_platform.label('lst_platform'),
        SubReport.price_range.label('price_range'),
        SubReport.url_thumbnail.label('url_thumbnail'),
        SubReport.introduction.label('introduction'),
        SubReport.url_cover.label('url_cover'),
        SubReport.category_report_id.label('category_report_id'),
        SubReport.category_base_id.label('category_base_id'),
        SubReport.shopee_category_id.label('shopee_category_id'),
        SubReport.search_volume_shopee.label('search_volume_shopee'),
        SubReport.report_type.label('report_type'),
        SubReport.report_format.label('report_format'),
        SubReport.report_version.label('report_version'),
        SubReport.status.label('status'),
        SubReport.last_analytic_at.label('last_analytic_at'),
        SubReport.weight.label('weight'),
        SubReport.source.label('source'),
        SubReport.price.label('price'),
        SubReport.price_before_discount.label('price_before_discount'),
        SubReport.is_editor_pick.label('is_editor_pick'),
        SubReport.start_date.label('start_date'),
        SubReport.end_date.label('end_date'),
        SubReport.updated_at.label('updated_at'),
        SubReport.is_unsellable.label('is_unsellable'),
        SubReport.filter_default.label('filter_default'),
        SubReport.filter_custom.label('filter_custom'),
        SubReport.url_report_pdf.label('url_report_pdf'),
    ).where(
        SubReport.slug == slug and SubReport.period == period
    )

    _find_sub_report = (await connection.execute(statement)).fetchall()

    if not _find_sub_report:
        return JSONResponse(status_code=404, content={'message': 'Not found report detail'})

    sub_report, = _find_sub_report

    cache_value_type = 'metadata'

    data_analytic_statement = select(
        CacheValue.value.label('value')
    ).where(
        CacheValue.key == f'{sub_report.report_id}_{sub_report.id}_{cache_value_type}'
    )
    _find_data_analytic = (await connection.execute(data_analytic_statement)).fetchall()

    data_analytic = None

    if _find_data_analytic:
        data_analytic = _find_data_analytic[0][0]

    return ResponseReportDetailMarketing(
        id=sub_report.id,
        slug=sub_report.slug,
        status=sub_report.status,
        name=sub_report.name[0].upper() + sub_report.name[1:],
        price=sub_report.price,
        introduction=sub_report.introduction,
        report_type=sub_report.report_type,
        report_format=sub_report.report_format,
        report_version=sub_report.report_version,
        url_thumbnail=sub_report.url_thumbnail,
        url_cover=sub_report.url_cover,
        category_report_id=sub_report.category_report_id,
        data_filter_report=sub_report.filter_custom or sub_report.filter_default,
        optimized_query=bool(sub_report.filter_custom),
        updated_at=sub_report.updated_at,
        url_report_pdf=sub_report.url_report_pdf,
        data_analytic=data_analytic,
        is_unsellable=sub_report.is_unsellable
    )
