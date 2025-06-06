#!/usr/bin/env bash
# Usage: test_client_tls.sh namespace

# to be safe
unset TOPIC
unset BAD_TOPIC

echo "Connecting to boostrap address $KAFKA"

echo "Start client TLS testing..."
############################################################################
# Test the secured connection
############################################################################
# create random topics
TOPIC=$(tr -dc A-Za-z0-9 </dev/urandom | head -c 20 ; echo '')
BAD_TOPIC=$(tr -dc A-Za-z0-9 </dev/urandom | head -c 20 ; echo '')

# write client config
echo $'security.protocol=SSL\nssl.truststore.location=/stackable/tls_keystore_server/truststore.p12\nssl.truststore.password=' > /tmp/client.config

if /stackable/kafka/bin/kafka-topics.sh --create --topic "$TOPIC" --bootstrap-server "$KAFKA" --command-config /tmp/client.config
then
  echo "[SUCCESS] Secure client topic created!"
else
  echo "[ERROR] Secure client topic creation failed!"
  exit 1
fi

if /stackable/kafka/bin/kafka-topics.sh --list --topic "$TOPIC" --bootstrap-server "$KAFKA" --command-config /tmp/client.config | grep "$TOPIC"
then
  echo "[SUCCESS] Secure client topic read!"
else
  echo "[ERROR] Secure client topic read failed!"
  exit 1
fi

############################################################################
# Test the connection without certificates
############################################################################
if /stackable/kafka/bin/kafka-topics.sh --create --topic "$BAD_TOPIC" --bootstrap-server "$KAFKA" &> /dev/null
then
  echo "[ERROR] Secure client topic created without certificates!"
  exit 1
else
  echo "[SUCCESS] Secure client topic creation failed without certificates!"
fi

############################################################################
# Test the connection with bad host name
############################################################################
if /stackable/kafka/bin/kafka-topics.sh --create --topic "$BAD_TOPIC" --bootstrap-server localhost:9093 --command-config /tmp/client.config &> /dev/null
then
  echo "[ERROR] Secure client topic created with bad host name!"
  exit 1
else
  echo "[SUCCESS] Secure client topic creation failed with bad host name!"
fi

echo "All client TLS tests successful!"
exit 0
