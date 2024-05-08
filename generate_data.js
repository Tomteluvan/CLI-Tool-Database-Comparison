const {faker} = require("@faker-js/faker")
const { saveData, createAndPopulateMeasurementsClickHouse, initializeDatabaseClickHouse } = require('./clickhouse/clickhouse_generate_data');
const { saveDataForPostgreSQL, createAndPopulateMeasurementsPostgres, initializeDatabasePostgres } = require('./postgres/postgreSQL_generate_data');
const { saveDataForTimescaleDB, createAndPopulateMeasurementsTimescale, initializeDatabaseTimescale } = require('./timescaledb/timescaledb_generate_data');

faker.seed(12345);

function generateDeviceData(numOfDevices) {
    const types = ['type1', 'type2', 'type3', 'type4'];
    const subTypes = ['kallvatten', 'varmvatten'];
    const _devices = [];

    console.time("faker time")
    for (let i = 0; i < numOfDevices; i++) {
      _devices.push({
        device_id: faker.string.uuid(),
        serial: 'SERIAL-' + faker.number.int({min: 10000000, max: 99999999}).toString(),
        type: types[faker.number.int({min: 0, max: 3})],
        subType: subTypes[faker.number.int({min: 0, max: 1})]
      });
    }
    console.timeEnd("faker time")
  
    return _devices;
}

async function generateMeasurementData(devicesData, periodTime) {

    await initializeDatabaseClickHouse();

    // await initializeDatabasePostgres();

    // await initializeDatabaseTimescale();

    await createAndPopulateMeasurementsClickHouse();

    // await createAndPopulateMeasurementsPostgres();

    // await createAndPopulateMeasurementsTimescale();

    const BATCH_SIZE = 500;

    const measurementsBatch = [];

    console.time('faker time');

    let endDate = null;

    // Initialize the start date and end date
    if (periodTime === 1) {
        endDate = new Date('2024-02-01T00:00:00Z');   
    } else {
        endDate = new Date('2025-01-01T12:00:00Z');
    }

    let i = 0;

    for (const device of devicesData) {

        let genHour = new Date('2024-01-01T12:00:00Z')

        while (genHour <= endDate) {
            for (let typeId = 1; typeId <= 8; typeId++) {

                measurementsBatch.push({
                    device_id: device.device_id,
                    value: faker.number.float({ min: 0.0, max: 100.0 }),
                    type: typeId,
                    timestamp: genHour
                });

                if (measurementsBatch.length >= BATCH_SIZE) {
                    await saveData(measurementsBatch);
                    // await saveDataForPostgreSQL(measurementsBatch);
                    // await saveDataForTimescaleDB(measurementsBatch);
                    measurementsBatch.length = 0;
                    if (i === 2000) {
                        console.log("2000 insertion!!!");
                        i = 0;
                    }
                    i++;
                }
            }

            genHour = new Date(genHour.getTime() + 60 * 60 * 1000);
        }
    }

    if (measurementsBatch.length > 0) {
        await saveData(measurementsBatch);
        // await saveDataForPostgreSQL(measurementsBatch);
        // await saveDataForTimescaleDB(measurementsBatch);
    }

    console.timeEnd('faker time');

    return measurementsBatch;
}

function generateOrganisationData(devices) {
    
     // Assigns all devices to a single organisation (organisation_id = 1).
     const assignments = [];

     console.time("assignment time");
     devices.forEach((device) => {
         assignments.push({
             organisation_id: 1,
             device_id: device.device_id,
         });
     });

     console.timeEnd("assignment time");
 
     return assignments;
}

module.exports = {
    generateDeviceData,
    generateMeasurementData, 
    generateOrganisationData
};