import logging
import sys


def setup_logging(level=logging.INFO):
  handler = logging.StreamHandler(sys.stdout)
  formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
  handler.setFormatter(formatter)

  root = logging.getLogger()
  root.setLevel(level)
  root.handlers.clear()
  root.addHandler(handler)
