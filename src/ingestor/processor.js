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
  if (!data || typeof data !== 'object') return false;
  return (
    data.MachineName !== undefined ||
    data.Ax1Pos !== undefined ||
    data.Ax2Pos !== undefined ||
    data.Ax3Pos !== undefined ||
    data['Active Tool No'] !== undefined ||
    data['Actual Feedrate'] !== undefined ||
    data['Spindle Speed'] !== undefined ||
    data['Block No'] !== undefined ||
    data['Cutting Time'] !== undefined ||
    data['Spindle Override'] !== undefined ||
    data['Part Count'] !== undefined ||
    data['Program Name'] !== undefined ||
    data['Machine State'] !== undefined ||
    data['Machine Mode'] !== undefined ||
    data['Rapid Override'] !== undefined ||
    data.Spin1Load !== undefined ||
    data.CycleTimeInt !== undefined ||
    data.Alarm1 !== undefined
  );
}

/**
 * Maps a Mitsubishi NC Machine Tool Connector payload onto our canonical
 * telemetry field names matching the InfluxDB schema.
 */
function normalizeMitsubishiPayload(data) {
  const mapped = {};

  // Helper to retrieve field value supporting spaced and unspaced key variations
  const getField = (...keys) => {
    for (const key of keys) {
      if (data[key] !== undefined) return data[key];
    }
    return undefined;
  };

  const machineId = getField('MachineName', 'Machine Name', 'machine_id');
  if (machineId !== undefined) mapped.machine_id = machineId;

  const ax1Pos = getField('Ax1Pos', 'Ax1 Pos', 'axis_x');
  if (ax1Pos !== undefined) mapped.axis_x = ax1Pos;

  const ax2Pos = getField('Ax2Pos', 'Ax2 Pos', 'axis_y');
  if (ax2Pos !== undefined) mapped.axis_y = ax2Pos;

  const ax3Pos = getField('Ax3Pos', 'Ax3 Pos', 'axis_z');
  if (ax3Pos !== undefined) mapped.axis_z = ax3Pos;

  const partCount = getField('Part Count', 'PartCount', 'production_count');
  if (partCount !== undefined) mapped.production_count = partCount;

  const programName = getField('Program Name', 'ProgramName', 'program_name');
  if (programName !== undefined) mapped.program_name = programName;

  const machineState = getField('Machine State', 'MachineState', 'machine_state');
  if (machineState !== undefined) mapped.machine_state = machineState;

  const machineMode = getField('Machine Mode', 'MachineMode', 'machine_mode');
  if (machineMode !== undefined) mapped.machine_mode = machineMode;

  const spin1Load = getField('Spin1Load', 'Spin 1 Load', 'Spindle1Load', 'spindle_load');
  if (spin1Load !== undefined) mapped.spindle_load = spin1Load;

  const cycleTime = getField('CycleTimeInt', 'Cycle Time Int', 'CycleTime', 'cycle_time');
  if (cycleTime !== undefined) mapped.cycle_time = cycleTime;

  const rapidOverride = getField('Rapid Override', 'RapidOverride', 'rapid_override');
  if (rapidOverride !== undefined) mapped.rapid_override = rapidOverride;

  const activeToolNo = getField('Active Tool No', 'ActiveToolNo', 'tool_number');
  if (activeToolNo !== undefined) mapped.tool_number = activeToolNo;

  const actualFeedrate = getField('Actual Feedrate', 'ActualFeedrate', 'feed_rate');
  if (actualFeedrate !== undefined) mapped.feed_rate = actualFeedrate;

  const spindleSpeed = getField('Spindle Speed', 'SpindleSpeed', 'spindle_speed');
  if (spindleSpeed !== undefined) mapped.spindle_speed = spindleSpeed;

  const blockNo = getField('Block No', 'BlockNo', 'block_number');
  if (blockNo !== undefined) mapped.block_number = blockNo;

  const cuttingTime = getField('Cutting Time', 'CuttingTime', 'cutting_time');
  if (cuttingTime !== undefined) {
    if (typeof cuttingTime === 'string' && cuttingTime.includes(':')) {
      const parts = cuttingTime.trim().split(':').map(Number);
      if (parts.every(p => !isNaN(p))) {
        if (parts.length === 3) mapped.cutting_time = parts[0] * 3600 + parts[1] * 60 + parts[2];
        else if (parts.length === 2) mapped.cutting_time = parts[0] * 60 + parts[1];
        else mapped.cutting_time = cuttingTime;
      } else {
        mapped.cutting_time = cuttingTime;
      }
    } else {
      mapped.cutting_time = cuttingTime;
    }
  }

  const spindleOverride = getField('Spindle Override', 'SpindleOverride', 'spindle_override');
  if (spindleOverride !== undefined) mapped.spindle_override = spindleOverride;

  const timestamp = getField('Timestamp', 'timestamp');
  if (timestamp !== undefined) mapped.timestamp = timestamp;

  // Alarm1 is a free-text alarm message; treat any non-blank text as an active alarm (1 or 0)
  const alarm = getField('Alarm1', 'Alarm 1', 'alarm_active');
  if (alarm !== undefined) {
    mapped.alarm_active = typeof alarm === 'string' ? (alarm.trim().length > 0 ? 1 : 0) : (alarm ? 1 : 0);
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
