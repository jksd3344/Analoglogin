#!/bin/bash
find /data/log_data/mostsdk/info/ -mtime +3 -exec -rf {} \;
find /data/log_data/basic/info/ -mtime +3 -exec -rf {} \;
find /data/log_data/wanpay/info/ -mtime +3 -exec -rf {} \;