const Sequelize = require('sequelize')

async function initializeDatabase() {

    let sequelize, devices, measurements, organisations;

    sequelize = new Sequelize('postgres://numoh:PASSWORD123@localhost:5432/timescaledb_for_test',
    {
        dialect: 'postgres',
        protocol: 'postgres'
    });

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

    return {
        sequelize: sequelize,
        devices: devices,
        measurements: measurements,
        organisations: organisations
    };
}

async function createAndPopulateDevices(sequelize, devices, data) {
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

async function createAndPopulateMeasurements(sequelize, measurements, data) {
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

async function createAndPopulateOrganisations(sequelize, organisations, data) {
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

module.exports = {
    initializeDatabase,
    createAndPopulateDevices,
    createAndPopulateMeasurements,
    createAndPopulateOrganisations
};