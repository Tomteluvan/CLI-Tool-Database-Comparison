const { InfluxDB, Point } = require('@influxdata/influxdb-client');
const { generateDeviceData, generateMeasurementData, generateOrganisationData } = require('../generate_data');

const url = 'http://localhost:8086';
const org = 'quandify';
const token = 'M2comoATFn6FV5SkSwTlBeV4_g-Ecb9xb6v4LyzIKOy29k9Qze7rA-UPPvAerAqT0KSzrEwmO03hUq65BO76ow==';
const bucket = 'measurements';

async function writeData(measurements, devices, assignments) {
    console.time('influx write');

    const deviceMap = new Map(devices.map(device => [device.device_id, device]));
    const assignmentMap = new Map(assignments.map(assign => [assign.device_id, assign]));

    const BATCH_SIZE = 20000;
    let points = [];

    for (let i = 0; i < measurements.length; i++) {
        const measurement = measurements[i];
        const device = deviceMap.get(measurement.device_id);
        const assignment = assignmentMap.get(measurement.device_id);

        if (device && assignment) {
            const point = new Point('measurement')
                .tag('device_id', device.device_id)
                .tag('device_type', device.type)
                .tag('device_subtype', device.subType)
                .tag('organisation_id', assignment.organisation_id.toString())
                .tag('measurement_type', `type${measurement.type}`)
                .floatField('value', measurement.value)
                .timestamp(measurement.timestamp);

            points.push(point);
        }

        // Write the batch and reset points array
        if (points.length === BATCH_SIZE || i === measurements.length - 1) {
            const writeApi = new InfluxDB({ url, token, timeout: 60000 }).getWriteApi(org, bucket, 'ms');
            await writeApi.writePoints(points);
            await writeApi.close();
            points = []; // Reset the points array
        }
    }

    console.log('Data points written successfully.');
    console.timeEnd('influx write');
}

// Generate and write example data
const devices = generateDeviceData(500); // Adjust this as needed
const measurements = generateMeasurementData(devices, 8); // Adjust this as needed
const assignments = generateOrganisationData(devices, 3); // Adjust this as needed

writeData(measurements, devices, assignments);
