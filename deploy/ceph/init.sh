#!/bin/sh
# One-shot: create the bucket, an RGW topic that pushes to the STOMP server's
# /s3-events receiver, and a bucket notification bound to that topic. Safe to
# re-run: topic/bucket creation are idempotent.
set -eu

ENDPOINT="${S3_ENDPOINT:-http://ceph:8080}"
REGION="${AWS_DEFAULT_REGION:-default}"
TOPIC="${TOPIC_NAME:-s3-events}"
PUSH_ENDPOINT="${PUSH_ENDPOINT:-http://server:8082/s3-events}"

echo "waiting for RGW at $ENDPOINT"
until aws --endpoint-url "$ENDPOINT" s3 ls >/dev/null 2>&1; do sleep 2; done

aws --endpoint-url "$ENDPOINT" s3 mb "s3://$BUCKET" 2>/dev/null || true

# Persistent topic: RGW queues events and retries until the server acks (2xx).
TOPIC_ARN=$(aws --endpoint-url "$ENDPOINT" sns create-topic --name "$TOPIC" \
  --attributes "{\"push-endpoint\":\"$PUSH_ENDPOINT\",\"persistent\":\"true\"}" \
  --query TopicArn --output text)
echo "topic: $TOPIC_ARN"

aws --endpoint-url "$ENDPOINT" s3api put-bucket-notification-configuration \
  --bucket "$BUCKET" \
  --notification-configuration "{\"TopicConfigurations\":[{\"Id\":\"all-events\",\"TopicArn\":\"$TOPIC_ARN\",\"Events\":[\"s3:ObjectCreated:*\",\"s3:ObjectRemoved:*\"]}]}"
echo "bucket $BUCKET notifies $PUSH_ENDPOINT"
