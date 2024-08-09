import requests

from helper.error_helper import log_error


class DiscordWebhook:
    def __init__(self, webhook_url: str):
        self.DISCORD_WEBHOOK_URL = webhook_url

    def send_text_message(self, content: str) -> bool:
        try:
            response = requests.post(self.DISCORD_WEBHOOK_URL,
                                     json={
                                         'content': content
                                     })
            if response.status_code == 204:
                return True
            return False
        except Exception as e:
            log_error(e)
            return False


class DiscordBotHelper:
    DISCORD_BOT_API_URL = 'https://dc-bot.muadee.vn'

    def __init__(self):
        pass

    @classmethod
    def get_webhook(cls, webhook_url: str) -> DiscordWebhook:
        return DiscordWebhook(webhook_url)

    def send_text_message_to_channel(self, channel_id: int, content: str, bot_name: str = 'global_bot') -> bool:
        try:
            response = requests.post(f'{self.DISCORD_BOT_API_URL}/bot/send-message?subject=channel',
                                     json={
                                         'subject_id': channel_id,
                                         'content': content,
                                         'bot_name': bot_name
                                     })
            if response.status_code == 200:
                return True
            return False
        except Exception as e:
            log_error(e)
            return False

    def send_text_message_to_user(self, user_id: int, content: str, bot_name: str = 'global_bot') -> bool:
        try:
            response = requests.post(f'{self.DISCORD_BOT_API_URL}/bot/send-message?subject=user',
                                     json={
                                         'subject_id': user_id,
                                         'content': content,
                                         'bot_name': bot_name
                                     })
            if response.status_code == 200:
                return True
            return False
        except Exception as e:
            log_error(e)
            return False


discord_bot_helper = DiscordBotHelper()


if __name__ == '__main__':
    # discord_webhook = ('https://discord.com/api/webhooks/1242319475823480944/'
    #                    'WrZZdrDF_GRZCA6lGLQ9B7c5fdip3Am1-XYZja0xTsd3cHNlg5UETiUl5OIfZsuW3m1j')
    # discord_bot_helper.get_webhook(discord_webhook).send_text_message('Hello')
    discord_bot_helper.send_text_message_to_channel(
        channel_id=1242319422928977961,
        content='Hello from DiscordBotHelper'
    )
