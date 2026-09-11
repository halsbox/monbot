import ast
import unittest
from pathlib import Path

from monbot.handlers.texts import (
  HELP_ADMIN,
  INVGEN_USAGE,
  MM_HELP_ADMIN,
  MM_INVGEN_USAGE,
)


ROOT = Path(__file__).resolve().parents[1]


class CommandSurfaceTests(unittest.TestCase):
  def test_help_texts_use_their_platform_prefix(self):
    self.assertIn("/invite", HELP_ADMIN)
    self.assertNotIn("/monbot", HELP_ADMIN)
    self.assertIn("/monbot invite", MM_HELP_ADMIN)
    self.assertIn("/invite", INVGEN_USAGE)
    self.assertIn("/monbot invite", MM_INVGEN_USAGE)

  def test_telegram_registers_both_invite_names(self):
    tree = ast.parse((ROOT / "monbot" / "bot.py").read_text())
    commands = {
      call.args[0].value
      for call in ast.walk(tree)
      if isinstance(call, ast.Call)
      and isinstance(call.func, ast.Name)
      and call.func.id == "CommandHandler"
      and call.args
      and isinstance(call.args[0], ast.Constant)
    }
    self.assertIn("invite", commands)
    self.assertIn("invgen", commands)


if __name__ == "__main__":
  unittest.main()
