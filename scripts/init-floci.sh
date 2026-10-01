#!/usr/bin/env bash
# Floci ready-hook — provisions a demo messaging topology for local
# development of platform-events (reference CLI, smoke tests, manual
# publish/consume). Runs automatically on container start via the volume
# mount to /etc/floci/init/ready.d/ (see docker-compose.yml).
#
# Creates (region us-east-1, account 000000000000):
#   SNS topic  platform-events-demo
#   SQS queue  platform-events-demo-q  (RedrivePolicy → -dlq, maxReceiveCount=5)
#   SQS queue  platform-events-demo-q-dlq
#   SNS→SQS subscription with RawMessageDelivery=true (required by the
#   consumer — without it every message is an SNS notification wrapper)
#
# The integration and e2e suites do NOT use this: each test starts its own
# floci container via testcontainers (test/fixtures/floci.go) and creates
# exactly the resources it needs.

set -euo pipefail

AWS_REGION=us-east-1
AWS_ACCOUNT=000000000000
TOPIC=platform-events-demo
QUEUE=platform-events-demo-q
DLQ="${QUEUE}-dlq"

# The AWS CLI in the floci compat image defaults to us-east-1 regardless of
# FLOCI_DEFAULT_REGION; floci treats region as an isolation boundary, so pin it.
export AWS_DEFAULT_REGION="$AWS_REGION"
export AWS_REGION="$AWS_REGION"

queue_arn() { printf 'arn:aws:sqs:%s:%s:%s' "$AWS_REGION" "$AWS_ACCOUNT" "$1"; }

aws sns create-topic --name "$TOPIC" >/dev/null

aws sqs create-queue --queue-name "$DLQ" >/dev/null
attrs=$(mktemp)
cat > "$attrs" <<JSON
{
  "RedrivePolicy": "{\"deadLetterTargetArn\":\"$(queue_arn "$DLQ")\",\"maxReceiveCount\":\"5\"}"
}
JSON
aws sqs create-queue --queue-name "$QUEUE" --attributes "file://$attrs" >/dev/null
rm -f "$attrs"

aws sns subscribe \
  --topic-arn "arn:aws:sns:${AWS_REGION}:${AWS_ACCOUNT}:${TOPIC}" \
  --protocol sqs \
  --notification-endpoint "$(queue_arn "$QUEUE")" \
  --attributes RawMessageDelivery=true >/dev/null

echo "init-floci: topic ${TOPIC}, queue ${QUEUE} (+ ${DLQ}) ready"
