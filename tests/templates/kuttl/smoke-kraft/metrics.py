import sys
import logging
import requests

if __name__ == "__main__":
    LOG_LEVEL = "DEBUG"  # if args.debug else 'INFO'
    logging.basicConfig(
        level=LOG_LEVEL,
        format="%(asctime)s %(levelname)s: %(message)s",
        stream=sys.stdout,
    )

    response = requests.get("http://test-kafka-broker-default-metrics:9606/metrics")

    assert response.status_code == 200, (
        f"Expected HTTP return code 200 from the metrics endpoint but got [{response.status_code}]"
    )

    assert "jmx_scrape_error" in response.text, (
        "Expected metric [jmx_scrape_error] not found"
    )
