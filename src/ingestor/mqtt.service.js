const mqtt = require("mqtt");
const axios = require("axios");
const config = require("../common/config");
const logger = require("../common/logger");
const { normalizePayload } = require("./processor");

/**
 * Helper to convert an MQTT wildcard topic pattern into a RegExp.
 * E.g., 'machines/+/+/test' -> /^machines\/[^/]+\/[^/]+\/test$/
 */
function mqttTopicToRegex(pattern) {
  if (!pattern) return null;
  const escaped = pattern.replace(/[-\/\\^$*+?.()|[\]{}]/g, (match) => {
    if (match === '+' || match === '#') return match;
    return '\\' + match;
  });
  const regexStr = '^' + escaped
    .split('+').join('[^/]+')
    .split('#').join('.*') + '$';
  return new RegExp(regexStr);
}

function initMQTT(onMessage) {
  const client = mqtt.connect(config.mqtt.brokerUrl, {
    clientId: config.mqtt.clientId,
    clean: false, // Persistent session
    reconnectPeriod: 5000,
  });

  const testTopicRegex = mqttTopicToRegex(config.mqtt.testTopic);

  client.on("connect", () => {
    logger.info(`Connected to MQTT broker at ${config.mqtt.brokerUrl}`);
    
    // Subscribe to standard telemetry topic
    client.subscribe(config.mqtt.topic, { qos: 1 }, (err) => {
      if (err) {
        logger.error({ err }, `Subscription failed for topic: ${config.mqtt.topic}`);
      } else {
        logger.info(`Subscribed to topic: ${config.mqtt.topic}`);
      }
    });

    // Some vendor connectors (e.g. Mitsubishi Smart Connect) auto-append the
    // registered machine name as the final topic segment instead of ending
    // in a literal "telemetry" segment. Subscribe to that shape too.
    const altTopic = 'machines/+/telemetry/+';
    client.subscribe(altTopic, { qos: 1 }, (err) => {
      if (err) {
        logger.error({ err }, `Subscription failed for topic: ${altTopic}`);
      } else {
        logger.info(`Subscribed to topic: ${altTopic}`);
      }
    });

    // Subscribe to test topic
    if (config.mqtt.testTopic) {
      client.subscribe(config.mqtt.testTopic, { qos: 1 }, (err) => {
        if (err) {
          logger.error({ err }, `Subscription failed for test topic: ${config.mqtt.testTopic}`);
        } else {
          logger.info(`Subscribed to test topic: ${config.mqtt.testTopic}`);
        }
      });
    }
  });

  client.on("message", (topic, payload) => {
    // Check if this is a test message
    if (testTopicRegex && testTopicRegex.test(topic)) {
      const rawPayload = payload.toString();
      logger.info(`====== [TEST TOPIC MESSAGE RECEIVED] ======`);
      logger.info(`Topic: ${topic}`);
      logger.info(`Raw Payload: ${rawPayload}`);
      
      try {
        const parsed = JSON.parse(rawPayload);
        logger.info('Parsed JSON Payload:');
        console.dir(parsed, { depth: null, colors: true });

        // Forward to backend test ingest API — but only if there are meaningful records
        if (parsed.records && Array.isArray(parsed.records)) {
          // Filter out records whose `record` field is an empty array []
          const meaningfulRecords = parsed.records.filter(
            r => Array.isArray(r.record) && r.record.length > 0
          );

          if (meaningfulRecords.length === 0) {
            logger.warn(`[TEST] All ${parsed.records.length} record(s) have empty record[] — skipping backend API call for topic: ${topic}`);
          } else {
            logger.info(`[TEST] Forwarding ${meaningfulRecords.length} of ${parsed.records.length} record(s) to backend from topic: ${topic}`);
            axios.post(`${config.service.backendUrl}/admin/telemetry/test/ingest`, {
              topic,
              payload: { ...parsed, records: meaningfulRecords }
            }).then(res => {
              logger.info({ status: res.status, ingested: res.data?.ingested }, 'Successfully forwarded test telemetry to backend');
            }).catch(err => {
              logger.error({ err: err.message, response: err.response?.data }, 'Failed to forward test telemetry to backend');
            });
          }
        }
      } catch (err) {
        logger.warn('Test payload is not valid JSON.');
      }
      logger.info(`==========================================`);
      return; // Bypass database storage and buffer
    }

    const data = normalizePayload(topic, payload);
    if (data) {
      onMessage(data);
    }
  });

  client.on("error", (err) => {
    logger.error({ err }, "MQTT client error");
  });

  client.on("reconnect", () => {
    logger.warn("MQTT client reconnecting...");
  });

  return client;
}

module.exports = {
  initMQTT
};
