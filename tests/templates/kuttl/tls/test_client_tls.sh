#!/usr/bin/env bash
# Usage: test_tls.sh namespace

NAMESPACE=$1

# to be safe
unset TOPIC
unset BAD_TOPIC

SERVER="test-kafka-broker-default-0.test-kafka-broker-default.${NAMESPACE}.svc.cluster.local:9093"

echo "Start TLS testing..."
############################################################################
# Test the secured connection
############################################################################
# create random topics
TOPIC=$(tr -dc A-Za-z0-9 </dev/urandom | head -c 20 ; echo '')
BAD_TOPIC=$(tr -dc A-Za-z0-9 </dev/urandom | head -c 20 ; echo '')

# write client config
echo $'security.protocol=SSL\nssl.keystore.location=/stackable/certificates/keystore.p12\nssl.keystore.password=changeit\nssl.truststore.location=/stackable/certificates/truststore.p12\nssl.truststore.password=changeit' > /tmp/client.config

if /stackable/kafka/bin/kafka-topics.sh --create --topic $TOPIC --bootstrap-server $SERVER --command-config /tmp/client.config
then
  echo "[SUCCESS] Secure client topic created!"
else
  echo "[ERROR] Secure client topic creation failed!"
  exit 1
fi

if /stackable/kafka/bin/kafka-topics.sh --list --topic $TOPIC --bootstrap-server $SERVER --command-config /tmp/client.config | grep $TOPIC
then
  echo "[SUCCESS] Secure client topic read!"
else
  echo "[ERROR] Secure client topic read failed!"
  exit 1
fi

############################################################################
# Test the connection without certificates
############################################################################
if /stackable/kafka/bin/kafka-topics.sh --create --topic $BAD_TOPIC --bootstrap-server $SERVER &> /dev/null
then
  echo "[ERROR] Secure client topic created without certificates!"
  exit 1
else
  echo "[SUCCESS] Secure client topic creation failed without certificates!"
fi

############################################################################
# Test the connection with bad host name
############################################################################
if /stackable/kafka/bin/kafka-topics.sh --create --topic $BAD_TOPIC --bootstrap-server localhost:9093 --command-config /tmp/client.config &> /dev/null
then
  echo "[ERROR] Secure client topic created with bad host name!"
  exit 1
else
  echo "[SUCCESS] Secure client topic creation failed with bad host name!"
fi

echo "All TLS tests successful!"
exit 0
