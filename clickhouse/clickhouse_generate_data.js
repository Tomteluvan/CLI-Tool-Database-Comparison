const { ClickHouse } = require('clickhouse');
const { query } = require('express');
const { URL } = require('url');
const { exec } = require('child_process');
const { promisify } = require('util');
const execAsync = promisify(exec);

let clickhouse;

async function initializeDatabaseClickHouse() {
    const clickhouseUrl = new URL('http://localhost');
    clickhouseUrl.port = 18123;

    clickhouse = new ClickHouse({
        url: clickhouseUrl.toString(),
        debug: false,
        basicAuth: null,
        format: "json",
    });
}

async function createAndPopulateDevicesClickHouse(devices) {
    try {

        // Drop table if exists
        await clickhouse.query(`DROP TABLE IF EXISTS devices;`).toPromise();

        //Create devices table
        await clickhouse.query(`
        CREATE TABLE devices (
            id UUID,
            serial String,
            type String,
            sub_type String
        ) ENGINE = MergeTree()
        ORDER BY id
        `).toPromise();

        console.log('Devices table created');

        console.time('insertTime');

        // Define batch size
        const batchSize = 1000; // Adjust based on your system's capacity
        
        for (let i = 0; i < devices.length; i += batchSize) {
            // Slice the devices array to get a batch
            const batch = devices.slice(i, i + batchSize);
        
            // Build values string for insertion
            const valuesStr = batch.map(({device_id, serial, type, subType}) => 
                `('${device_id}', '${serial}', '${type}', '${subType}')`
            ).join(',');
        
            // Perform batch insert
            await clickhouse.query(`
                INSERT INTO devices (id, serial, type, sub_type)
                VALUES ${valuesStr}
            `).toPromise();
        }
        
        console.timeEnd('insertTime');
        
        console.log(`${devices.length} devices inserted into devices table.`);

        console.log('Devices have been inserted successfully.');

    } catch (error) {
        console.error('An error occurred:', error);
    }
}

async function createAndPopulateMeasurementsClickHouse() {
    try {
        
        // Drop table if exists
        await clickhouse.query(`DROP TABLE IF EXISTS measurements;`).toPromise();;

        // Create measurements table
        await clickhouse.query(`
        CREATE TABLE measurements (
            device_id UUID,
            value Float64,
            type Int16,
            timestamp DateTime('UTC')
        ) ENGINE = MergeTree() 
        ORDER BY timestamp;
        `).toPromise();

        console.log('Measurements table created');
    } catch (error) {
        console.error('An error occurred:', error);
    }
}

async function saveData(measurements) {

    console.time('insertTime');

    const batchSize = 500;

    for (let i = 0; i < measurements.length; i += batchSize) {

        const batch = measurements.slice(i, i + batchSize);

        // Build values string for insertion
        const valuesStr = batch.map(({ device_id, value, type, timestamp }) => {
            const adjustedTimestamp = timestamp.toISOString().replace('T', ' ').replace(/\.\d+Z$/, '');
            return `('${device_id}', '${value}', '${type}', '${adjustedTimestamp}')`;
        }).join(',');
    
        // Perform batch insert
        await clickhouse.query(`
            INSERT INTO measurements (device_id, value, type, timestamp)
            VALUES ${valuesStr}
        `).toPromise();
    }

    console.timeEnd('insertTime');

    console.log(`${measurements.length} measurements inserted into measurements table.`);
}

async function createAndPopulateOrganisationsClickHouse(assignments) {
    try {

        // Drop table if exists
        await clickhouse.query(`DROP TABLE IF EXISTS organisations;`).toPromise();

        // Create organisations table
        await clickhouse.query(`
        CREATE TABLE organisations (
            organisation_id Int32,
            device_id UUID
        ) ENGINE = MergeTree() 
        ORDER BY organisation_id;
        `).toPromise();

        console.log('Organisations table created');

        console.time("insertTime");

        // Define batch size
        const batchSize = 1000; // Adjust based on your system's capacity

        // Insert assignments in batches
        for (let i = 0; i < assignments.length; i += batchSize) {
            const batch = assignments.slice(i, i + batchSize);

            const valuesStr = batch.map(({organisation_id, device_id}) => 
                `('${organisation_id}', '${device_id}')`
            ).join(',');

            await clickhouse.query(`
                INSERT INTO organisations (organisation_id, device_id)
                VALUES ${valuesStr}
            `).toPromise();
        }

        console.timeEnd('insertTime');

        console.log(`${assignments.length} devices inserted into organisations table.`);

        console.log('Organisations have been inserted successfully.');

    } catch (error) {
        console.error('An error occurred:', error);
    }
}

async function performQueryForClickHouse() {
    const numbers = [];
    let sum = 0;
    let k = 100;

    try {
        const dockerCommand = `docker exec clickhouse-server clickhouse-client --time -q "SELECT toUnixTimestamp(date_trunc('month', toDateTime(m.timestamp, 'Europe/Berlin'))) AS ts, SUM(value) AS value, d.sub_type AS type FROM measurements AS m JOIN devices AS d ON d.id = m.device_id JOIN organisations AS o ON d.id = o.device_id WHERE o.organisation_id = '2' AND m.type = 5 AND m.timestamp >= toDateTime(1704106800) AND m.timestamp < toDateTime(1706698800) GROUP BY ts, d.sub_type ORDER BY ts ASC, d.sub_type ASC;"`;

        for (let i = 0; i < k; i++) {
            console.log(`Running iteration ${i + 1}`);

            const { stdout, stderr } = await execAsync(dockerCommand);

            console.log(`Execution Time: ${stderr * 1000} ms`);

            numbers.push(stderr * 1000);
            sum += stderr * 1000;
        }

        const mean = sum / k;

        calculateStatistics(mean, numbers);

    } catch (error) {
        console.error('Error during the execution:', error);
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
    initializeDatabaseClickHouse,
    createAndPopulateDevicesClickHouse,
    createAndPopulateMeasurementsClickHouse,
    createAndPopulateOrganisationsClickHouse,
    performQueryForClickHouse,
    saveData
};