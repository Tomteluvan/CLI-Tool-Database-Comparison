const { oneMonthInflux } = require('../queries/queries_for_influx/query_for_multipleDevices_in_influx');
const {faker} = require("@faker-js/faker")
const { InfluxDB, Point } = require('@influxdata/influxdb-client');
const { generateDeviceData, generateMeasurementData, generateOrganisationData } = require('../generate_data');
const { exec } = require('child_process');
const { promisify } = require('util');
const execAsync = promisify(exec);

const url = 'http://localhost:8086';
const org = 'quandify';
const token = 'pWHCI9tBjbt0yrkczKMnlKklfIhHClkt6rYK3VbyrcLiPUw9b1J_mOnq-DbCTCXDht4cfDYcLOjHvhMiP8LiPA==';
// M2comoATFn6FV5SkSwTlBeV4_g-Ecb9xb6v4LyzIKOy29k9Qze7rA-UPPvAerAqT0KSzrEwmO03hUq65BO76ow==
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

async function generateAndWriteMeasurements(devicesData, organisationsData) {
    const endDate = new Date('2025-01-01T12:00:00Z');
    const deviceMap = new Map(devicesData.map(device => [device.device_id, device]));
    const assignmentMap = new Map(organisationsData.map(assign => [assign.device_id, assign]));

    const writeApi = new InfluxDB({ url, token, timeout: 3000000 }).getWriteApi(org, bucket, 'ms', writeOptions);

    console.time('Influx insertion');
    for (const device of devicesData) {
        let genHour = new Date('2024-01-01T12:00:00Z');

        while (genHour <= endDate) {
            for (let typeId = 1; typeId <= 8; typeId++) {
                const value = faker.number.float({ min: 0.0, max: 100.0 });
                const measurement = {
                    device_id: device.device_id,
                    value,
                    type: typeId,
                    timestamp: genHour
                };

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

                    writeApi.writePoint(point);
                }
            }

            genHour = new Date(genHour.getTime() + 60 * 60 * 1000);
        }

        // Flush after processing each device, or based on another logic that suits the batch size
        await writeApi.flush();
    }

    // Ensure all writes are completed before closing the API
    await writeApi.close();
    console.timeEnd('Influx insertion');
}


async function createAndPopulateInflux (devicesData, measurementsData, organisationsData) {
    try {
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
    const numbers = [];
    let k = 10;
    let sum = 0;

    try {
        const queryApi = new InfluxDB({url, token,}).getQueryApi(org);

        console.log('One Month Influx');

        for (let i = 0; i < k; i++) {
            const dockerCommand = 'docker exec influxdb2 /bin/bash -c "time influx query \'from(bucket: \\"measurements\\") |> range(start: time(v: \\"2024-01-01T11:00:00Z\\"), stop: time(v: \\"2024-01-31T11:00:00Z\\")) |> filter(fn: (r) => r._measurement == \\"measurement\\" and r.organisation_id == \\"1\\" and r.measurement_type == \\"5\\") |> group(columns: [\\"device_subtype\\"]) |> sum(column: \\"_value\\") |> group() |> sort(columns: [\\"_time\\"])\'"';
    
            const { stdout, stderr } = await execAsync(dockerCommand);
            // console.log(stdout)//logs the query results
            // Extracting real time from stderr
            const realTimeMatch = stderr.match(/real\s+([\d.]+)m([\d.]+)s/);
            if (realTimeMatch && realTimeMatch.length === 3) {
                const minutes = parseFloat(realTimeMatch[1]);
                const seconds = parseFloat(realTimeMatch[2]);
                const realTimeMs = (minutes * 60 + seconds) * 1000; // Convert to milliseconds
                console.log(i,' time: ', realTimeMs,'ms');
                numbers.push(realTimeMs); // Store real time for this run
                sum += realTimeMs;
            } else {
                console.log('Failed to parse real time');
            }
    
            // This returns JSON data using influx api. 
            // const data = await queryApi.collectRows(oneMonthInflux)
            // data.forEach((x) => console.log(JSON.stringify(x)))
        }

        const mean = sum / k;
        calculateStatistics(mean, numbers);

    } catch(error) {
        console.log('error while performing influx query 1 month: ', error)
    }
}

async function performQueryInflux1Year() {
    const numbers = [];
    let k = 10;
    let sum = 0;

    try {
        const queryApi = new InfluxDB({url, token,}).getQueryApi(org);

        console.log('One Year Influx');

        for (let i = 0; i < k; i++) {
            const dockerCommand = 'docker exec influxdb2 /bin/bash -c "time influx query \'from(bucket: \\"measurements\\") |> range(start: time(v: \\"2024-01-01T11:00:00Z\\"), stop: time(v: \\"2024-12-31T11:00:00Z\\")) |> filter(fn: (r) => r._measurement == \\"measurement\\" and r.organisation_id == \\"1\\" and r.measurement_type == \\"5\\") |> group(columns: [\\"device_subtype\\"]) |> sum(column: \\"_value\\") |> group() |> sort(columns: [\\"_time\\"])\'"';
    
            const { stdout, stderr } = await execAsync(dockerCommand);
            // console.log(stdout)//logs the query results
            // Extracting real time from stderr
            const realTimeMatch = stderr.match(/real\s+([\d.]+)m([\d.]+)s/);
            if (realTimeMatch && realTimeMatch.length === 3) {
                const minutes = parseFloat(realTimeMatch[1]);
                const seconds = parseFloat(realTimeMatch[2]);
                const realTimeMs = (minutes * 60 + seconds) * 1000; // Convert to milliseconds
                console.log(i,' time: ', realTimeMs,'ms');
                numbers.push(realTimeMs); // Store real time for this run
                sum += realTimeMs;
            } else {
                console.log('Failed to parse real time');
            }
    
            // This returns JSON data using influx api. 
            // const data = await queryApi.collectRows(oneMonthInflux)
            // data.forEach((x) => console.log(JSON.stringify(x)))
        }

        const mean = sum / k;
        calculateStatistics(mean, numbers);

    } catch(error) {
        console.log('error while performing influx query 1 month: ', error)
    }
}

function calculateStatistics(mean, numbers) {

    console.log("");

    console.log("Mean:", mean + " ms");

    // Step 1: Calculate the difference between each number and the mean
    const differences = numbers.map(number => number - mean);

    // Step 2: Square each of these differences
    const squaredDifferences = differences.map(diff => diff * diff);

    // Step 3: Find the mean of the squared differences
    const squaredDifferencesMean = squaredDifferences.reduce((sum, diff) => sum + diff, 0) / squaredDifferences.length;

    // Step 4: Take the square root of the mean of the squared differences
    const standardDeviation = Math.sqrt(squaredDifferencesMean);

    console.log("Standard Deviation:", standardDeviation.toFixed(2) + " ms");
    const cv = (standardDeviation / mean) * 100;

    console.log("Coefficient of Variation:", cv.toFixed(2) + "%");

    const minTimeAverage = mean - standardDeviation;
    const maxTimeAverage = mean + standardDeviation;

    console.log("Min Time:", minTimeAverage.toFixed(2) + " ms");
    console.log("Max Time:", maxTimeAverage.toFixed(2) + " ms");
}

module.exports = {
    createAndPopulateInflux,
    performQueryInflux1Month,
    performQueryInflux1Year,
    generateAndWriteMeasurements
};
