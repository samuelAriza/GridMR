import logging
from typing import Optional

def get_logger(name: Optional[str] = None) -> logging.Logger:
	logger = logging.getLogger(name or "GridMR.Worker")
	if not logger.hasHandlers():
		handler = logging.StreamHandler()
		formatter = logging.Formatter(
			'[%(asctime)s] %(levelname)s %(name)s: %(message)s',
			datefmt='%Y-%m-%d %H:%M:%S'
		)
		handler.setFormatter(formatter)
		logger.addHandler(handler)
	logger.setLevel(logging.INFO)
	return logger

def setup_logger():
	logger = get_logger()
	logger.info("Logger GridMR Worker inicializado")
	return logger
