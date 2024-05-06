const { oneMonthInflux } = require('../queries/queries_for_influx/query_for_multipleDevices_in_influx_1_month');
const { InfluxDB, Point, FluxTableMetaData } = require('@influxdata/influxdb-client');
const { generateDeviceData, generateMeasurementData, generateOrganisationData } = require('../generate_data');
const { exec } = require('child_process');
const { promisify } = require('util');
const execAsync = promisify(exec);
const { de } = require('@faker-js/faker');

const url = 'http://localhost:8086';
const org = 'quandify';
const token = 'M2comoATFn6FV5SkSwTlBeV4_g-Ecb9xb6v4LyzIKOy29k9Qze7rA-UPPvAerAqT0KSzrEwmO03hUq65BO76ow==';
const bucket = 'measurements';

const flushBatchSize = 25000;

const writeOptions = {
    batchSize: 50000,
    flushInterval:0
}

// async function writeData(measurements, devices, assignments) {
async function writeData() {
    // these function calls should be done through the CLI in the main program later.
    const devices = generateDeviceData(1000); // Adjust this as needed
    const measurements = await generateMeasurementData(devices, 1); // Adjust this as needed
    const assignments = generateOrganisationData(devices); // Adjust this as needed

    console.time('influx write');

    const deviceMap = new Map(devices.map(device => [device.device_id, device]));
    const assignmentMap = new Map(assignments.map(assign => [assign.device_id, assign]));

    const writeApi = new InfluxDB({ url, token, timeout: 3000000 }).getWriteApi(org, bucket, 'ms', writeOptions);


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
                .tag('measurement_type', measurement.type)
                .floatField('value', measurement.value)
                .timestamp(measurement.timestamp);

            // writePoint only writes the point so that it is prepared for insertion to the database
            writeApi.writePoint(point)

            // We later "flush" all points that have been written after reaching a batch size so that it is transferred to the database
            if ((i + 1) % flushBatchSize === 0) {
                await writeApi.flush();
            }
        }
    }

    // Ensure all writes are completed before closing the API
    await writeApi.close();
    console.log('Data points written successfully.');
    console.timeEnd('influx write');
}

async function createAndPopulateInflux (devicesData, measurementsData, organisationsData) {
    try {
        const queryApi = new InfluxDB({ url, token, timeout: 3000000 }).getQueryApi(org, bucket, 'ms', writeOptions);

        

        const deviceMap = new Map(devicesData.map(device => [device.device_id, device]));
        const assignmentMap = new Map(organisationsData.map(assign => [assign.device_id, assign]));

        const writeApi = new InfluxDB({ url, token, timeout: 3000000 }).getWriteApi(org, bucket, 'ms', writeOptions);


        console.time('Influx insertion');
        for (let i = 0; i < measurementsData.length; i++) {
            const measurement = measurementsData[i];
            const device = deviceMap.get(measurement.device_id);
            const assignment = assignmentMap.get(measurement.device_id);


            if (device && assignment) {
                const point = new Point('measurement')
                    .tag('device_id', device.device_id)
                    .tag('device_type', device.type)
                    .tag('device_subtype', device.subType)
                    .tag('organisation_id', assignment.organisation_id.toString())
                    .tag('measurement_type', measurement.type)
                    .floatField('value', measurement.value)
                    .timestamp(measurement.timestamp);

                // writePoint only writes the point so that it is prepared for insertion to the database
                writeApi.writePoint(point)

                // We later "flush" all points that have been written after reaching a batch size so that it is transferred to the database
                if ((i + 1) % flushBatchSize === 0) {
                    await writeApi.flush();
                }
            }
        }

        // Ensure all writes are completed before closing the API
        await writeApi.close();
        console.timeEnd('Influx insertion');
    } catch(error) {
        console.log('An error occured while inserting data into influx: ', error);
    }
}

async function performQueryInflux1Month() {
    let k = 10;

    try {
        const queryApi = new InfluxDB({url, token,}).getQueryApi(org);

        for (let i = 0; i < k; i++) {
            const dockerCommand = 'docker exec influxdb2 /bin/bash -c "time influx query \'from(bucket: \\"measurements\\") |> range(start: time(v: \\"2024-01-01T11:00:00Z\\"), stop: time(v: \\"2024-01-31T11:00:00Z\\")) |> filter(fn: (r) => r._measurement == \\"measurement\\" and r.organisation_id == \\"1\\" and r.measurement_type == \\"5\\") |> group(columns: [\\"device_subtype\\"]) |> sum(column: \\"_value\\") |> group() |> sort(columns: [\\"_time\\"])\'"';
    
    
            const { stdout, stderr } = await execAsync(dockerCommand);
            // console.log(`Execution Time: ${stderr * 1000} ms`);
            //console.log(stdout)//logs the query results
                    // Extracting real time from stderr
        const realTimeIndex = stderr.indexOf('real');
        const realTimeString = stderr.substring(realTimeIndex, stderr.indexOf('s', realTimeIndex) + 1);
        console.log(realTimeString);
    
            // const data = await queryApi.collectRows(oneMonthInflux)
            // data.forEach((x) => console.log(JSON.stringify(x)))
        }

    } catch(error) {
        console.log('error while performing influx query 1 month: ', error)
    }
}

// Generate and write example data
// const devices = generateDeviceData(1000); // Adjust this as needed
// const measurements = generateMeasurementData(devices, 1); // Adjust this as needed
// const assignments = generateOrganisationData(devices); // Adjust this as needed

// writeData(measurements, devices, assignments);
//writeData();

module.exports = {
    createAndPopulateInflux,
    performQueryInflux1Month
};
