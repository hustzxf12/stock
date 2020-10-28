import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(thread)s - %(funcName)s - %(message)s')
logger = logging.getLogger(__name__)
logging = logging