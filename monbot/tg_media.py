from __future__ import annotations

from typing import Optional

from telegram import InlineKeyboardMarkup, InputMediaPhoto
from telegram.error import BadRequest
from telegram.ext import ExtBot

FALLBACK_EDIT_ERRORS = (
  "message to edit not found",
  "message cannot be edited",
  "message can't be edited",
  "message is not a media message",
  "message content type cannot be edited",
)


def _should_fallback_send(s: str) -> bool:
  s = s.lower()
  return any(substr in s for substr in FALLBACK_EDIT_ERRORS)


async def edit_or_send_graph(
    bot: ExtBot,
    chat_id: int,
    message_id: Optional[int],
    file_id: Optional[str],
    image_bytes: Optional[bytes],
    reply_markup: Optional[InlineKeyboardMarkup],
    caption: Optional[str] = None,
    parse_mode: Optional[str] = None,
) -> tuple[int, Optional[str]]:
  """Edit existing media or send new. Supports caption on edit/send."""
  if not (file_id or image_bytes):
    raise RuntimeError("Neither file_id nor image_bytes provided")

  media = InputMediaPhoto(media=file_id or image_bytes, caption=caption, parse_mode=parse_mode)
  new_file_id: Optional[str] = None

  try:
    if message_id is not None:
      msg = await bot.edit_message_media(
        chat_id=chat_id,
        message_id=message_id,
        media=media,
        reply_markup=reply_markup,
      )
    else:
      msg = await bot.send_photo(
        chat_id=chat_id,
        photo=image_bytes if image_bytes is not None else file_id,
        caption=caption,
        parse_mode=parse_mode,
        reply_markup=reply_markup,
      )
    if msg and msg.photo:
      sizes = sorted(msg.photo, key=lambda p: p.file_size or 0)
      if sizes:
        new_file_id = sizes[-1].file_id
    return msg.message_id, new_file_id

  except BadRequest as e:
    s = str(e)
    if "message is not modified" in s.lower():
      return message_id or 0, None
    if message_id is not None and _should_fallback_send(s):
      # Fallback: previous message is text or gone -> send new photo
      msg = await bot.send_photo(
        chat_id=chat_id,
        photo=image_bytes if image_bytes is not None else file_id,
        caption=caption,
        parse_mode=parse_mode,
        reply_markup=reply_markup,
      )
      if msg and msg.photo:
        sizes = sorted(msg.photo, key=lambda p: p.file_size or 0)
        if sizes:
          new_file_id = sizes[-1].file_id
      return msg.message_id, new_file_id
    raise


async def edit_caption_only(
    bot: ExtBot,
    chat_id: int,
    message_id: int,
    caption: str,
    reply_markup: Optional[InlineKeyboardMarkup],
    parse_mode: Optional[str] = None,
):
  await bot.edit_message_caption(
    chat_id=chat_id, message_id=message_id, caption=caption, parse_mode=parse_mode, reply_markup=reply_markup
  )
