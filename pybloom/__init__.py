name = 'BloomFilterPy'

import logging
from logging.handlers import SysLogHandler

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

handler = SysLogHandler(address='/dev/log')
formatter = logging.Formatter('%(module)s.py: [%(levelname)s] => %(message)s')
handler.setFormatter(formatter)

log.addHandler(handler)
