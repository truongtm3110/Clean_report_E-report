import telegram

from helper.error_helper import log_error
from helper.project_helper import get_project_path


def send_message(bot_token='1908229975:AAHJK6ALI2nL7F8d0lfloCsb0VyEqeUEeow', chat_id=None, message=None):
    # "id": -507180751, "title": "[Bee] Product Report",
    # "id": -576740403, "title": "[Bee] Notification Market",
    # "id": -551226488, "title": "[Bee] Data Notification",
    status = True
    try:
        bot = telegram.Bot(token=bot_token)
        bot.sendMessage(chat_id=chat_id, text=message, timeout=15)
    except Exception as e:
        status = False
        log_error(e)
    return status


def send_message_html(bot_token='1908229975:AAHJK6ALI2nL7F8d0lfloCsb0VyEqeUEeow', chat_id=None, text=None):
    # "id": -507180751, "title": "[Bee] Product Report",
    # "id": -576740403, "title": "[Bee] Notification Market",
    # "id": -551226488, "title": "[Bee] Data Notification",
    status = True
    try:
        bot = telegram.Bot(token=bot_token)
        bot.sendMessage(chat_id=chat_id, text=text, parse_mode=telegram.ParseMode.HTML, timeout=60)
    except Exception as e:
        status = False
        log_error(e)
    return status


if __name__ == '__main__':
    # hướng dẫn lấy channel id ở: https://manage.hostvn.net/hostvn-knowledgebase.php?action=article&id=633
    from dotenv import load_dotenv

    load_dotenv(get_project_path() + '/.env')
    default = "1908229975:AAHJK6ALI2nL7F8d0lfloCsb0VyEqeUEeow"
    from app.service.setting.setting_service import get_value_setting

    # TELEGRAM_BOT_TOKEN = get_value_setting(key='TELEGRAM_BOT_TOKEN', default=default)
    TELEGRAM_BOT_TOKEN = default
    TELEGRAM_CHANNEL_DEFAULT = get_value_setting(key='TELEGRAM_CHANNEL_DEFAULT', default="-550005601")
    # send_message(bot_token=TELEGRAM_BOT_TOKEN, chat_id='-1001709918554', message='Hi channel nhé :D')
    print(TELEGRAM_BOT_TOKEN, 'aaa')
    send_message_html(bot_token=TELEGRAM_BOT_TOKEN, chat_id='-1001709918554',
                      text=f"""
    Cập nhật số liệu Market:
        - Ngày cập nhật: Shopee dd/MM/yyyy, Tiki dd/MM/yyyy, Lazada dd/MM/yyyy, Sendo dd/MM/yyyy.
        - <a href='https://bit.ly/BeeQnASkus'>Độ phủ của danh sách sản phẩm (SKUs)</a> (Shopee 95%, Tiki 90%, Laz 85%, Sendo 90%).
        - Tính chính xác của Số liệu (Theo số đơn): (Shopee 90%, Tiki 85%, Laz 90%, Sendo 90%).
            - <a href='https://market.beecost.vn/blog/tinh-chinh-xac-so-lieu-cua-beecost-market/#THE_NAO_LA_8220CHINH_XAC8221'>"Chính xác" là như thế nào?</a>
            - <a href='https://market.beecost.vn/blog/tinh-chinh-xac-so-lieu-cua-beecost-market/#Tinh_chinh_xac_cua_8220San_pham_da_ban8221'>Tính chính xác của số liệu "Sản phẩm đã bán"?</a>
            - <a href='https://market.beecost.vn/blog/tinh-chinh-xac-so-lieu-cua-beecost-market/#Tinh_chinh_xac_cua_8220Doanh_so8221'>Tính chính xác của số liệu "Doanh số"?</a>
            - <a href='https://bit.ly/BeeStatCompute'>Phương pháp tính toán số liệu</a>
            - Xem thêm: <a href='https://market.beecost.vn/blog/tinh-chinh-xac-so-lieu-cua-beecost-market/#CAC_CAU_HOI_PHO_BIEN'>Các câu hỏi phổ biến</a>:
                + <a href='https://market.beecost.vn/blog/tinh-chinh-xac-so-lieu-cua-beecost-market/#So_lieu_BeeCost_co_cap_nhat_Realtime_khong'>Số liệu BeeCost Market có realtime không?</a>
                + <a href='https://market.beecost.vn/blog/tinh-chinh-xac-so-lieu-cua-beecost-market/#Co_bo_duoc_doanh_so_ao_buff_don_khoi_so_lieu_khong'>Cách loại bỏ sản phẩm bị buff đơn?</a>
                + <a href='https://market.beecost.vn/blog/tinh-chinh-xac-so-lieu-cua-beecost-market/#Vi_sao_doanh_so_30_ngay_Shop_cua_toi_bi_lech_tren_BeeCost'>Vì sao doanh số 30 ngày bị lệch so với BeeCost?</a>
                + <a href='https://market.beecost.vn/blog/tinh-chinh-xac-so-lieu-cua-beecost-market/#Cac_kieu_Shop_hay_bi_lech_doanh_so_so_voi_BeeCost_nhieu_hon_20'>Các kiểu Shop hay bị lệch doanh số (hơn 20%)?</a>
                …
                + Xem full: <a href='https://bit.ly/BeeCustomerSupportQnA'>LIST CÂU HỎI HỖ TRỢ KHÁCH HÀNG</a>
    """)
