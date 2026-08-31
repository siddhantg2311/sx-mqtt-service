const logger = require("../common/logger");

/**
 * Parses the MQTT topic to extract factory_id/machine_id if not present in payload.
 *
 * Handles two topic shapes:
 *  - machines/{factory_id}/{machine_id}/telemetry   (standard)
 *  - machines/{factory_id}/telemetry/{machine_id}   (Smart Connect auto-appends
 *    the registered machine name as the final segment of whatever topic is
 *    configured, so a base of "machines/{factory_id}/telemetry" ends up here)
 */
function parseTopic(topic) {
  const parts = topic.split('/');
  if (parts.length === 4 && parts[2] === 'telemetry' && parts[3] !== 'telemetry') {
    return { factory_id: parts[1], machine_id: parts[3] };
  }
  if (parts.length >= 3) {
    return {
      factory_id: parts[1],
      machine_id: parts[2]
    };
  }
  return {};
}

/**
 * Detects the Mitsubishi Electric NC Machine Tool Connector payload shape.
 * That connector's drag-and-drop field binding produces its own field names
 * (PascalCase / spaced) instead of our canonical snake_case schema.
 */
function isMitsubishiPayload(data) {
  return data.MachineName !== undefined || data.Ax1Pos !== undefined;
}

/**
 * Maps a Mitsubishi NC Machine Tool Connector payload onto our canonical
 * telemetry field names. Fields the connector doesn't currently bind
 * (spindle_speed, feed_rate, feed_override, tool_number, tool_name,
 * cutting_time, production_time) are left unset rather than guessed.
 */
function normalizeMitsubishiPayload(data) {
  const mapped = {};

  if (data.MachineName !== undefined) mapped.machine_id = data.MachineName;
  if (data.Ax1Pos !== undefined) mapped.axis_x = data.Ax1Pos;
  if (data.Ax2Pos !== undefined) mapped.axis_y = data.Ax2Pos;
  // No Ax3Pos observed from this connector — axis_z intentionally left unset.
  if (data['Part Count'] !== undefined) mapped.production_count = data['Part Count'];
  if (data['Program Name'] !== undefined) mapped.program_name = data['Program Name'];
  if (data['Machine State'] !== undefined) mapped.machine_state = data['Machine State'];
  if (data['Machine Mode'] !== undefined) mapped.machine_mode = data['Machine Mode'];
  if (data.Spin1Load !== undefined) mapped.spindle_load = data.Spin1Load;
  if (data.CycleTimeInt !== undefined) mapped.cycle_time = data.CycleTimeInt;
  if (data['Rapid Override'] !== undefined) mapped.rapid_override = data['Rapid Override'];
  if (data.Timestamp !== undefined) mapped.timestamp = data.Timestamp;

  // Alarm1 is a free-text alarm message; treat any non-blank text as an
  // active alarm. Confirm with the vendor if this ever needs to distinguish
  // "currently active" from "last logged alarm."
  if (data.Alarm1 !== undefined) {
    mapped.alarm_active = data.Alarm1.trim().length > 0 ? 1 : 0;
  }

  return mapped;
}

/**
 * Normalizes the incoming payload, ensuring all required fields are present.
 */
function normalizePayload(topic, payload) {
  let data;
  try {
    data = JSON.parse(payload.toString());
  } catch (err) {
    logger.error({ topic, payload: payload.toString() }, 'Malformed JSON payload');
    return null;
  }

  const topicMeta = parseTopic(topic);
  const vendorData = isMitsubishiPayload(data) ? normalizeMitsubishiPayload(data) : data;

  // Merge topic metadata with payload data
  const normalized = {
    ...topicMeta,
    ...vendorData,
    // Ensure timestamp exists, fallback to now
    timestamp: vendorData.timestamp || new Date().toISOString()
  };

  return normalized;
}

module.exports = {
  normalizePayload
};
