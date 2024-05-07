const Sequelize = require('sequelize');
const { exec } = require('child_process');
const util = require('util');
const fs = require('fs');
const readFileAsync = util.promisify(fs.readFile);
const execProm = util.promisify(exec);

const sequelize = new Sequelize('postgres://numoh:PASSWORD123@localhost:5432/postgres_for_test', {
    dialect: 'postgres',
    protocol: 'postgres',
    logging: false
});

let devices, measurements, organisations;

async function initializeDatabasePostgres() {

    try {
        await sequelize.authenticate();
        console.log('Connection has been established successfully.');

        devices = sequelize.define('devices', {
            id: {type: Sequelize.UUID, primaryKey: true },
            serial: {type: Sequelize.TEXT, allowNull: false },
            type: {type: Sequelize.TEXT, allowNull: false },
            sub_type: {type: Sequelize.TEXT, allowNull: false }
        }, { timestamps: false });
    
        measurements = sequelize.define('measurements', {
            device_id: { type: Sequelize.UUID, allowNull: false },
            value: { type: Sequelize.DOUBLE, allowNull: false },
            type: { type: Sequelize.SMALLINT, allowNull: false },
            timestamp: { type: Sequelize.DATE, allowNull: false }
        }, { timestamps: false });
        measurements.removeAttribute("id");
    
        organisations = sequelize.define('organisations', {
            organisation_id: { type: Sequelize.INTEGER, allowNull: false },
            device_id: { type: Sequelize.UUID, allowNull: false }
        }, { timestamps: false });
        organisations.removeAttribute("id");

        await sequelize.sync(); // Sync models with database

        console.log('Database initialized successfully.');
    } catch (error) {
        console.error('Unable to connect to the database:', error);
    }
}

async function createAndPopulateDevicesPostgres(data) {
    try {

        // Drop table if exists
        await sequelize.query(`DROP TABLE IF EXISTS devices;`);
        
        // Create devices table
        await sequelize.query(`
            CREATE TABLE devices (
                id UUID PRIMARY KEY,
                serial TEXT NOT NULL,
                type TEXT NOT NULL,
                sub_type TEXT NOT NULL
        )`);

        // Single-column index on id
        await sequelize.query(`CREATE INDEX idx_devices_id ON devices (id);`);

        // Single-column index on sub_type
        await sequelize.query(`CREATE INDEX idx_devices_sub_type ON devices (sub_type);`);

        console.time('insertTime');

        // Define batch size
        const batchSize = 500; // Adjust based on your system's capacity
        
        for (let i = 0; i < data.length; i += batchSize) {
            // Slice the devices array to get a batch
            const batch = data.slice(i, i + batchSize);
            
            // Perform batch insert
            try {
                await devices.bulkCreate(batch.map(({ device_id, serial, type, subType }) => ({
                    id: device_id,
                    serial: serial,
                    type: type,
                    sub_type: subType
                })));
                console.log(`Batch from ${i} to ${i + batchSize} inserted successfully.`);
            } catch (error) {
                console.error('Error inserting batch: ', error);
            }
        }
        
        console.timeEnd('insertTime');
        
        console.log(`${data.length} devices inserted.`);

        console.log('Devices table created and populated');

    } catch (error) {
        console.error('An error occurred:', error);
    }
}

async function createAndPopulateMeasurementsPostgres() {
    try {

        // Drop table if exists
        await sequelize.query(`DROP TABLE IF EXISTS measurements;`);
        
        // Create measurements table
        await sequelize.query(`
            CREATE TABLE measurements (
                device_id UUID NOT NULL,
                value double precision NOT NULL,
                type smallint NOT NULL,
                timestamp timestamp with time zone NOT NULL
        )`);

        // Create single-column index on device_id
        await sequelize.query(`CREATE INDEX idx_measurements_device_id ON measurements (device_id);`);

        // Create single-column index on type
        await sequelize.query(`CREATE INDEX idx_measurements_type ON measurements (type);`);

        // Create single-column index on timestamp
        await sequelize.query(`CREATE INDEX idx_measurements_timestamp ON measurements (timestamp);`);
    } catch (error) {
        console.error('Failed to insert measurements: ', error);
    }
}

async function createAndPopulateOrganisationsPostgres(data) {
    try {
        
         // Drop table if exists
         await sequelize.query(`DROP TABLE IF EXISTS organisations;`);
        
         // Create organisations table
         await sequelize.query(`
             CREATE TABLE organisations (
                 organisation_id INTEGER NOT NULL,
                 device_id UUID NOT NULL
         )`);

        // Single-column index on id
        await sequelize.query(`CREATE INDEX idx_organisations_organisation_id ON organisations (organisation_id);`);
 
         // Define batch size
        const batchSize = 1000; // Adjust based on your system's capacity
        
        for (let i = 0; i < data.length; i += batchSize) {
            // Slice the devices array to get a batch
            const batch = data.slice(i, i + batchSize);
            
            // Perform batch insert
            try {
                await organisations.bulkCreate(batch.map(({ organisation_id, device_id }) => ({
                    organisation_id: organisation_id,
                    device_id: device_id,
                })));
                console.log(`Batch from ${i} to ${i + batchSize} inserted successfully.`);
            } catch (error) {
                console.error('Error inserting batch: ', error);
            }
        }
    } catch (error) {
        console.log('Error inserting data to organisations table.')
    }
}

function run_pgbench(command) {
    return new Promise((resolve, reject) => {
        exec(command, (error, stdout, stderr) => {
            if (error) {
                reject(error);
                return;
            }
            if (stderr) {
                reject(stderr);
                return;
            }
            resolve(stdout);
        });
    });
}

async function performQueryPostgres() {
    try {
        const command = `docker exec postgres_container pgbench -U numoh -d postgres_for_test -f /queries/query_for_multipleDevices_in_postgres.sql --transactions=10 --log`;
        const result = await run_pgbench(command);

        console.log("Benchmarked 10 queries successfully!");

        console.log(result);
    } catch (error) {
        console.error('Error occurred:', error);
    }
}

async function findAndExtractDataPostgres() {
    try {
        // Step 1: Find the file inside the Docker container
        const findCommand = 'docker exec postgres_container sh -c "find / -type f -name \'pgbench_log.*\' 2>/dev/null"';
        const result = await execProm(findCommand).catch(err => {
            if (!err.stdout.trim()) {
                throw err;  // If there's no output, rethrow the error
            }
            return err;  // If there is output, treat it as a success case
        });
        const fileList = result.stdout.split('\n').filter(Boolean);
        
        if (fileList.length === 0) {
            console.log('No files found.');
            return;
        }
        
        const file = fileList[0].trim();
        console.log('File found:', file);

        const command = `docker cp postgres_container:${file} .`;

        // Execute the command
        await execProm(command);
        console.log('File copied successfully to the current directory.');

        await extractExecutionTime(file.slice(1));

        // Delete the file after processing
        await execProm(`docker exec postgres_container sh -c "rm '${file}'"`);
        console.log('File deleted successfully:', file);

    } catch (err) {
        console.error('Error during file operation:', err);
    }
}

async function extractExecutionTime(file) {
    return readFileAsync(file, 'utf8')
        .then(data => {
            // Split the data by lines
            const lines = data.split('\n');

            // Extract the third column from each line and calculate the sum
            let sum = 0;

            const numbers = [];

            lines.forEach(line => {
                const columns = line.trim().split(/\s+/);
                if (columns.length >= 3) {
                    numbers.push(parseInt(columns[2]) * 0.001);
                    sum += parseInt(columns[2] * 0.001);
                }
            });

            const mean = sum / 10;

            console.log("Mean", mean + " ms");

            // Step 1: Calculate the difference between each number and the mean
            const differences = numbers.map(number => number - mean);

            // Step 2: Square each of these differences
            const squaredDifferences = differences.map(diff => diff * diff);

            // Step 3: Find the mean of the squared differences
            const squaredDifferencesMean = squaredDifferences.reduce((sum, diff) => sum + diff, 0) / squaredDifferences.length;

            // Step 4: Take the square root of the mean of the squared differences
            const standardDeviation = Math.sqrt(squaredDifferencesMean);

            console.log("Standard Deviation:", standardDeviation + " ms");

            const cv = (standardDeviation / mean) * 100;

            console.log("Coefficient of Variation:", cv.toFixed(2) + "%");

            minMeanTime = mean - standardDeviation;
            maxMeanTime = mean + standardDeviation;

            console.log("Min Time:", minMeanTime + " ms");
            console.log("Max Time:", maxMeanTime + " ms");
        })
        .catch(err => {
            console.error('Error reading file: ', err);
        });
}

async function saveDataForPostgreSQL(data) {

    try {
        // Define batch size
        const batchSize = 1000;
        
        for (let i = 0; i < data.length; i += batchSize) {
            // Slice the devices array to get a batch
            const batch = data.slice(i, i + batchSize);
            
            // Perform batch insert
            try {
                await measurements.bulkCreate(batch.map(({ device_id, value, type, timestamp }) => ({
                    device_id: device_id,
                    value: value,
                    type: type,
                    timestamp: timestamp
                })));
                // console.log(`Batch from ${i} to ${i + batchSize} inserted successfully.`);
            } catch (error) {
                console.error('Error inserting batch: ', error);
            }
        }
    } catch (error) {
        console.error('An error occurred:', error);
    }
}

module.exports = {
    initializeDatabasePostgres,
    createAndPopulateDevicesPostgres,
    createAndPopulateMeasurementsPostgres,
    createAndPopulateOrganisationsPostgres,
    performQueryPostgres,
    findAndExtractDataPostgres,
    saveDataForPostgreSQL
};
