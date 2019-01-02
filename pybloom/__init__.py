import logging
from logging.handlers import SysLogHandler

name = 'BloomFilterPy'
__version__ = '1.0.1'

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

handler = SysLogHandler(address='/dev/log')
formatter = logging.Formatter('%(module)s.py: [%(levelname)s] => %(message)s')
handler.setFormatter(formatter)

log.addHandler(handler)
