#!/bin/sh
# Dump environment variables so cron jobs can access them
printenv >> /etc/environment
exec cron -f
