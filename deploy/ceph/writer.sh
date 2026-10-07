#!/bin/sh
# Test traffic: upload a small random object to the bucket every
# WRITE_INTERVAL seconds so the notification path has something to carry.
set -eu

ENDPOINT="${S3_ENDPOINT:-http://ceph:8080}"
INTERVAL="${WRITE_INTERVAL:-5}"

n=0
while true; do
  n=$((n + 1))
  head -c 1024 /dev/urandom | aws --endpoint-url "$ENDPOINT" s3 cp - "s3://$BUCKET/obj-$(date +%s)-$n.bin" >/dev/null
  echo "wrote obj #$n"
  sleep "$INTERVAL"
done
