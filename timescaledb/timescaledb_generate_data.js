const Sequelize = require('sequelize')

const sequelize = new Sequelize('postgres://numoh:PASSWORD123@localhost:5432/timescaledb_for_test', {
    dialect: 'postgres',
    protocol: 'postgres'
});

let devices, measurements, organisations;

async function initializeDatabase() {

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

async function createAndPopulateDevices(data) {
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

async function createAndPopulateMeasurements(data) {
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

        await sequelize.query(`SELECT create_hypertable('measurements', by_range('timestamp'));`);

        console.time('insertTime');

        // Define batch size
        const batchSize = 1000; // Adjust based on your system's capacity
        
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
                console.log(`Batch from ${i} to ${i + batchSize} inserted successfully.`);
            } catch (error) {
                console.error('Error inserting batch: ', error);
            }
        }
        
        console.timeEnd('insertTime');

        console.log('Measurements inserted successfully.');
    } catch (error) {
        console.error('Failed to insert measurements: ', error);
    }
}

async function createAndPopulateOrganisations(data) {
    try {
        
         // Drop table if exists
         await sequelize.query(`DROP TABLE IF EXISTS organisations;`);
        
         // Create organisations table
         await sequelize.query(`
             CREATE TABLE organisations (
                 organisation_id INTEGER NOT NULL,
                 device_id UUID NOT NULL
         )`);
 
         console.time('insertTime');
 
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

         console.timeEnd('insertTime')
    } catch (error) {
        console.log('Error inserting data to organisations table.')
    }
}

async function performQuery(option) {
    switch (option) {
        case '1':
            try {
                console.time('queryTime'); // Starta mätningen av exekveringstiden
                const result = await sequelize.query(`
                    SELECT
                        extract(epoch from timezone('Europe/Berlin', date_trunc('month', timezone('Europe/Berlin', m.timestamp))))::integer AS ts,
                        SUM(value) AS value,
                        d.sub_type AS type
                    FROM
                        measurements AS m
                    JOIN
                        devices AS d ON d.id = m.device_id
                    WHERE
                        d.id = 'c28ace1e-cf28-4d2f-a7d3-9bc8631cd379'
                        AND m.type = 4
                        AND m.timestamp >= to_timestamp(1704106800)
                        AND m.timestamp < to_timestamp(1706698800)
                    GROUP BY
                        date_trunc('month', timezone('Europe/Berlin', m.timestamp)),
                        d.sub_type
                    ORDER BY
                        date_trunc('month', timezone('Europe/Berlin', m.timestamp)),
                        d.sub_type;
                `);
                console.log("Resultat:", result[0]); // Skriv ut resultatet
                console.timeEnd('queryTime'); // Sluta mäta exekveringstiden och skriv ut
            } catch (error) {
                console.error('Error during the execution:', error);
            }
            break;
        case '2':
            try {
                console.time('queryTime');
                const result = await sequelize.query(`
                    SELECT
                        EXTRACT(EPOCH FROM timezone('Europe/Berlin', date_trunc('month', timezone('Europe/Berlin', m.timestamp))))::integer AS ts,
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
                        AND m.timestamp >= TO_TIMESTAMP(1704106800) 
                        AND m.timestamp < TO_TIMESTAMP(1706698800)
                    GROUP BY
                        date_trunc('month', timezone('Europe/Berlin', m.timestamp)),
                        d.sub_type
                    ORDER BY
                        ts,
                        type;
                `);

                console.log('Result:', result[0]);

                console.timeEnd('queryTime');
            } catch (error) {
                console.error('Error during the execution:', error);
            }
            break;
        default:
            break;
    }
}

module.exports = {
    initializeDatabase,
    createAndPopulateDevices,
    createAndPopulateMeasurements,
    createAndPopulateOrganisations,
    performQuery
};