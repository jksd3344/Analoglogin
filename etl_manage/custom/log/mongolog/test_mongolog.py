    
import logging
from mongolog.handlers import MongoHandler
log = logging.getLogger('demo')
log.setLevel(logging.DEBUG)
log.addHandler(MongoHandler.to(db='mongolog', collection='log'))

log.debug("1 - debug message")
log.info("2 - info message")
log.warn("3 - warn message")
log.error("4 - error message")
log.critical("5 - critical message")