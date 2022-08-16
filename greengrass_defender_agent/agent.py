# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from threading import Timer, Thread

from AWSIoTDeviceDefenderAgentSDK import collector
from greengrass_defender_agent import config
from greengrass_defender_agent import ipc_utils


def set_configuration(configuration):
    config.logger.debug("set_configuration() called.")
    """
    Set up a configuration object given input configuration and apply constraints and defaults.

    :param configuration: a dictionary object of configuration
    """
    new_config = {}
    if config.SAMPLE_INTERVAL_CONFIG_KEY in configuration:
        try:
            sample_interval_seconds = int(configuration[config.SAMPLE_INTERVAL_CONFIG_KEY])
        except (ValueError, TypeError):
            config.logger.warning(
                "Invalid sample interval. Using default sample interval: {}".format(
                    config.MIN_INTERVAL_SECONDS
                )
            )
            sample_interval_seconds = config.MIN_INTERVAL_SECONDS
        if sample_interval_seconds < config.MIN_INTERVAL_SECONDS:
            config.logger.warning(
                "Using minimum sample interval: {}".format(
                    config.MIN_INTERVAL_SECONDS
                )
            )
            new_config[config.SAMPLE_INTERVAL_NEW_CONFIG_KEY] = config.MIN_INTERVAL_SECONDS
        else:
            new_config[config.SAMPLE_INTERVAL_NEW_CONFIG_KEY] = sample_interval_seconds
    else:
        new_config[config.SAMPLE_INTERVAL_NEW_CONFIG_KEY] = config.MIN_INTERVAL_SECONDS
        config.logger.warning(
            "Using default sample interval: {}".format(
                config.MIN_INTERVAL_SECONDS
            )
        )

    return new_config


def set_configuration_and_publish(ipc_client, configuration):
    """
    Call publish_metrics() with the new configuration object.

    :param ipc_client: Ipc client
    :param configuration: a dictionary object of configuration
    :param metrics_collector: metrics collector
    """
    
    config.logger.debug(
        "Set config EnableGPUMetrics: {}".format(
            configuration["EnableGPUMetrics"]
        )
    )
    # Initialize metrics collector
    metrics_collector = collector.Collector(short_metrics_names=False, use_custom_metrics=configuration["EnableGPUMetrics"])
    
    new_config = set_configuration(configuration)
    sample_interval_seconds = new_config[config.SAMPLE_INTERVAL_NEW_CONFIG_KEY]
    config.logger.debug("Collector running on device: {}".format(config.THING_NAME))
    config.logger.debug("Metrics topic: {}".format(config.TOPIC))
    config.logger.debug("Sampling interval: {} seconds".format(sample_interval_seconds))
    publish_metrics(ipc_client, new_config, metrics_collector, sample_interval_seconds)
    return metrics_collector


def wait_for_config_changes(ipc_client):
    """
    Wait for configuration changes.

    :param ipc_client: Ipc client
    :param metrics_collector: metrics collector
    """
    config.logger.debug("wait_for_config_changes() entered.")
    with config.condition:
        config.logger.debug("Configuration wait started")
        config.condition.wait()
        config.logger.debug("Configuration update received.")
        set_configuration_and_publish(ipc_client, ipc_client.get_configuration())
        config.logger.debug("set_configuration_and_publish() called after configuration update.")
    wait_for_config_changes(ipc_client)


def publish_metrics(ipc_client, config_changed, metrics_collector, sample_interval_seconds):
    """
    Collect and publish metrics.

    :param ipc_client: Ipc client
    :param config_changed: boolean whether the configuration has changed
    :param metrics_collector: metrics collector
    :param sample_interval_seconds: sampling metrics interval in seconds
    """
    try:
        if config_changed and config.SCHEDULED_THREAD is not None:
            config.SCHEDULED_THREAD.cancel()
            config_changed = False

        metric = metrics_collector.collect_metrics()
        config.logger.debug("Publishing metrics: {}".format(metric.to_json_string()))
        ipc_client.publish_to_iot_core(config.TOPIC, metric.to_json_string())

        config.SCHEDULED_THREAD = Timer(
            float(sample_interval_seconds), publish_metrics,
            [ipc_client, config_changed, metrics_collector, sample_interval_seconds]
        )
        config.SCHEDULED_THREAD.start()

    except Exception as e:
        config.logger.error("Error collecting and publishing metrics: {}".format(e))
        raise e


def main():
    # Get the ipc client
    ipc_client = ipc_utils.IPCUtils()
    try:
        ipc_client.connect()
    except Exception as e:
        config.logger.error(
            "Exception occurred during the creation of an IPC client: {}".format(e)
        )
        exit(1)

    # Get initial configuration from the recipe
    configuration = ipc_client.get_configuration()

    # Subscribe to accepted/rejected topics for status report of publish
    ipc_client.subscribe_to_iot_core(config.TOPIC + "/accepted")
    ipc_client.subscribe_to_iot_core(config.TOPIC + "/rejected")

    # Start collecting and publishing metrics
    metrics_collector = set_configuration_and_publish(ipc_client, configuration)
    config.logger.debug("metrics_collector returned")
    # Subscribe to the subsequent configuration changes
    ipc_client.subscribe_to_config_updates()
    config.logger.debug("ipc_client subscribed to updates")
    Thread(
        target=wait_for_config_changes,
        args=(ipc_client, ),
    ).start()
