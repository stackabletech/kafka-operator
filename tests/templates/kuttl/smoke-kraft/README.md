
# Kraft test bundle

To reduce the number of tests, this one ("smoke-kraft") bundles multiple tests into one:

* smoke
* logging
* tls (always enabled)

This test doesn't install any zookeeper servers and only runs in Kraft mode (as the name implies).
