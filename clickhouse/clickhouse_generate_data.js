const { ClickHouse } = require('clickhouse');
const { query } = require('express');
const { URL } = require('url');

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

async function createAndPopulateMeasurementsClickHouse(measurements) {
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

        console.time('insertTime');

        // Define batch size
        const batchSize = 500; // Adjust based on your system's capacity

        // Insert measurements in batches
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

        console.log('Measurements have been inserted successfully.');

    } catch (error) {
        console.error('An error occurred:', error);
    }
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

async function performQueryForClickHouse(option) {
    switch(option) {
        case '1':
            try {
                const result = await clickhouse.query(`
                    SELECT
                        toUnixTimestamp(dateTrunc('month', m.timestamp)) AS ts,
                        sum(m.value) AS value,
                        d.sub_type AS type
                    FROM measurements AS m
                    INNER JOIN devices AS d ON d.id = m.device_id
                    WHERE (d.id = 'c28ace1e-cf28-4d2f-a7d3-9bc8631cd379') AND (m.type = 4) AND (m.timestamp >= '2024-01-01 12:00:00') AND (m.timestamp < '2024-02-01 12:00:00')
                    GROUP BY
                        ts,
                        d.sub_type
                    ORDER BY
                        ts ASC,
                        d.sub_type ASC;
                `).toPromise();

                const queryDuration = await clickhouse.query(`
                    SELECT query_duration_ms
                    FROM system.query_log 
                    WHERE query LIKE '%SUM(value) AS value%' -- Replace with a unique part of your query 
                    AND type = 'QueryFinish'
                    ORDER BY event_time DESC
                    LIMIT 1
                `).toPromise();

                console.log("Query duration in ms: ", queryDuration[0].query_duration_ms);

                console.log("Resultat: ", result);
            } catch (error) {
                console.error('Error during the execution:', error);
            }
            break;
        case '2':
            try {
                const result = await clickhouse.query(`
                    SELECT
                        toUnixTimestamp(date_trunc('month', toDateTime(m.timestamp, 'Europe/Berlin'))) AS ts,
                        SUM(value) AS value,
                        d.sub_type AS type
                    FROM
                        measurements AS m
                    JOIN
                        devices AS d ON d.id = m.device_id
                    JOIN
                        organisations AS o ON d.id = o.device_id
                    WHERE
                        o.organisation_id = '1'
                        AND m.type = 5
                        AND m.timestamp >= toDateTime(1704106800)
                        AND m.timestamp < toDateTime(1706698800)
                    GROUP BY
                        ts,
                        d.sub_type
                    ORDER BY
                        ts ASC,
                        d.sub_type ASC;
                `).toPromise();

                const queryDuration = await clickhouse.query(`
                    SELECT query_duration_ms
                    FROM system.query_log 
                    WHERE query LIKE '%SUM(value) AS value%' -- Replace with a unique part of your query 
                    AND type = 'QueryFinish'
                    ORDER BY event_time DESC
                    LIMIT 1
                `).toPromise();

                console.log("Query duration in ms: ", queryDuration[0].query_duration_ms);
                
                console.log("Resultat: ", result);

            } catch (error) {
                console.error('Error during the execution:', error);
            }
    }
}

module.exports = {
    initializeDatabaseClickHouse,
    createAndPopulateDevicesClickHouse,
    createAndPopulateMeasurementsClickHouse,
    createAndPopulateOrganisationsClickHouse,
    performQueryForClickHouse
};