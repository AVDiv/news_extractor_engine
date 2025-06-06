from datetime import datetime
import os

from discord_webhook import DiscordWebhook, DiscordEmbed

from news_extractor_engine.utils.devtools import only_dev_mode


class DiscordLogger:
    __webhook_url: str

    @staticmethod
    def set_webhook_url(url) -> None:
        DiscordLogger.__webhook_url = url

    @staticmethod
    def send_message(msg: str, **kwargs: dict) -> DiscordWebhook:
        if kwargs.get("webhook") and isinstance(kwargs["webhook"], DiscordWebhook):
            webhook = kwargs["webhook"]
            webhook.content = msg
            response = webhook.edit()
        else:
            webhook = DiscordWebhook(url=DiscordLogger.__webhook_url, content=msg)
            response = webhook.execute()
        return webhook

    @staticmethod
    def send_embed(
        *,
        title: str,
        description: str,
        color: int = 0xDDCFEE,
        url: str | None = None,
        author_name: str | None = None,
        author_url: str | None = None,
        author_icon_url: str | None = None,
        thumbnail_url: str | None = None,
        image_url: str | None = None,
        footer_text: str | None = None,
        footer_icon_url: str | None = None,
        timestamp: float | int | datetime | None = None,
        provider_name: str | None = None,
        provider_url: str | None = None,
        video_url: str | None = None,
        video_height: int | None = None,
        video_width: int | None = None,
        **kwargs,
    ) -> DiscordWebhook:
        embed: DiscordEmbed = DiscordEmbed(
            title=title, description=description, color=color
        )
        if url is not None:
            embed.set_url(url)
        if author_name is not None:
            embed.set_author(name=author_name, url=author_url, icon_url=author_icon_url)
        if thumbnail_url is not None:
            embed.set_thumbnail(url=thumbnail_url)
        if image_url is not None:
            embed.set_image(url=image_url)
        if footer_text is not None:
            embed.set_footer(text=footer_text, icon_url=footer_icon_url)
        if timestamp is not None:
            embed.set_timestamp(timestamp)
        if provider_name is not None:
            embed.set_provider(name=provider_name, url=provider_url)
        if (
            video_url is not None
            and video_height is not None
            and video_width is not None
        ):
            embed.set_video(url=video_url, height=video_height, width=video_width)
        if kwargs.get("webhook") and isinstance(kwargs["webhook"], DiscordWebhook):
            webhook = kwargs["webhook"]
            webhook.remove_embeds()
            webhook.add_embed(embed)
            response = webhook.edit()
        else:
            webhook = DiscordWebhook(url=DiscordLogger.__webhook_url)
            webhook.add_embed(embed)
            response = webhook.execute()
        return webhook

    @staticmethod
    def send_file(file_path: str, content: str = "", **kwargs) -> DiscordWebhook:
        if kwargs.get("webhook") and isinstance(kwargs["webhook"], DiscordWebhook):
            webhook = kwargs["webhook"]
            webhook.remove_files()
            with open(file_path, "rb") as f:
                webhook.add_file(file=f.read(), filename=os.path.basename(file_path))
            response = webhook.edit()
        else:
            webhook = DiscordWebhook(url=DiscordLogger.__webhook_url, content=content)
            with open(file_path, "rb") as f:
                webhook.add_file(file=f.read(), filename=os.path.basename(file_path))
            response = webhook.execute()
        return webhook

    @staticmethod
    def send_error(error: Exception, **kwargs) -> DiscordWebhook:
        return DiscordLogger.send_embed(
            title="Error",
            description=f"An error occurred: {error}",
            color=0xFF0000,
            webhook=kwargs["webhook"] if kwargs.get("webhook") else None,
        )
