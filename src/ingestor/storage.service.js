const { InfluxDB, Point } = require('@influxdata/influxdb-client');
const { SendMessageCommand } = require("@aws-sdk/client-sqs");
const { sqsClient } = require("../common/aws.client");
const config = require("../common/config");
const logger = require("../common/logger");

const influxDB = new InfluxDB({ url: config.influx.url, token: config.influx.token });
// Create a SINGLE global writeApi instance
const writeApi = influxDB.getWriteApi(config.influx.org, config.influx.bucket, 'ns');

async function writeToInflux(batch) {
  if (!batch || batch.length === 0) return;

  let recordsWritten = 0;

  try {
    for (const msg of batch) {
      const timestamp = msg.timestamp ? new Date(msg.timestamp) : new Date();
      const machineId = msg.machine_id || 'UNKNOWN_MACHINE';
      const factoryId = msg.factory_id || 'UNKNOWN_FACTORY';
      const programName = msg.program_name || 'UNKNOWN_PROGRAM';

      const point = new Point('cnc_telemetry')
        .tag('machine_id', String(machineId))
        .tag('factory_id', String(factoryId))
        .tag('program_name', String(programName))
        .timestamp(timestamp);

      if (msg.spindle_speed !== undefined) point.floatField('spindle_speed', parseFloat(msg.spindle_speed));
      if (msg.spindle_override !== undefined) point.floatField('spindle_override', parseFloat(msg.spindle_override));
      if (msg.feed_rate !== undefined) point.floatField('feed_rate', parseFloat(msg.feed_rate));
      if (msg.feed_override !== undefined) point.floatField('feed_override', parseFloat(msg.feed_override));
      if (msg.rapid_override !== undefined) point.floatField('rapid_override', parseFloat(msg.rapid_override));
      if (msg.tool_number !== undefined) point.floatField('tool_number', parseFloat(msg.tool_number));
      if (msg.tool_name !== undefined) point.stringField('tool_name', String(msg.tool_name));
      if (msg.cutting_time !== undefined) point.floatField('cutting_time', parseFloat(msg.cutting_time));
      // if (msg.program_name !== undefined) point.stringField('program_name', String(msg.program_name));
      if (msg.block_number !== undefined) point.stringField('block_number', String(msg.block_number));
      if (msg.alarm_active !== undefined) point.floatField('alarm_active', parseFloat(msg.alarm_active));
      if (msg.program_runtime !== undefined) point.floatField('program_runtime', parseFloat(msg.program_runtime));
      if (msg.axis_x !== undefined) point.floatField('axis_x', parseFloat(msg.axis_x));
      if (msg.axis_y !== undefined) point.floatField('axis_y', parseFloat(msg.axis_y));
      if (msg.axis_z !== undefined) point.floatField('axis_z', parseFloat(msg.axis_z));
      if (msg.spindle_load !== undefined) point.floatField('spindle_load', parseFloat(msg.spindle_load));
      if (msg.production_count !== undefined) point.floatField('production_count', parseFloat(msg.production_count));
      if (msg.production_time !== undefined) point.floatField('production_time', parseFloat(msg.production_time));
      if (msg.machine_state !== undefined) point.floatField('machine_state', parseFloat(msg.machine_state));
      if (msg.machine_mode !== undefined) point.floatField('machine_mode', parseFloat(msg.machine_mode));
      if (msg.cycle_time !== undefined) point.floatField('cycle_time', parseFloat(msg.cycle_time));
      if (msg.program !== undefined) point.stringField('program', String(msg.program));

      writeApi.writePoint(point);
      recordsWritten++;
    }

    // Use flush() to send data, DO NOT close() here
    await writeApi.flush();
    logger.info(`Successfully flushed ${recordsWritten} records to InfluxDB`);
  } catch (err) {
    logger.error({ err }, 'Error writing to InfluxDB');
    
    // Fallback to SQS DLQ for manual inspection/retry
    if (config.aws.sqs.dlqUrl) {
      await sendToDLQ(batch, err.message);
    }
    
    throw err; // Propagate for buffer handling
  }
}

async function sendToDLQ(batch, reason) {
  try {
    const command = new SendMessageCommand({
      QueueUrl: config.aws.sqs.dlqUrl,
      MessageBody: JSON.stringify({ batch, error: reason, timestamp: new Date().toISOString() })
    });
    await sqsClient.send(command);
    logger.info('Sent failed batch to DLQ');
  } catch (dlqErr) {
    logger.error({ dlqErr }, 'Critical: Failed to send to DLQ');
  }
}

async function closeInflux() {
  try {
    logger.info("Closing InfluxDB writeApi...");
    await writeApi.close(); // Flushes remaining data and closes
    logger.info("InfluxDB writeApi closed successfully.");
  } catch (err) {
    logger.error({ err }, "Error closing InfluxDB writeApi");
  }
}

module.exports = {
  writeToInflux,
  closeInflux
};
