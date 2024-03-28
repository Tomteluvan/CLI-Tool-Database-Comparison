const { ClickHouse } = require('clickhouse');
const { faker } = require('@faker-js/faker');
const readline = require('readline');

faker.seed(12345);

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });


const clickhouse = new ClickHouse({
    url: 'http://localhost',
    port: 18123,
    debug: false,
    basicAuth: null,
    format: "json",
})

function generateDeviceData(numRecords) {
    const types = ['type1', 'type2', 'type3', 'type4'];
    const subTypes = ['kallvatten', 'varmvatten'];
    const devices = [];
  
    for (let i = 0; i < numRecords; i++) {
      devices.push({
        device_id: faker.string.uuid(),
        serial: 'SERIAL-' + faker.number.int({min: 10000000, max: 99999999}).toString(),
        type: types[faker.number.int({min: 0, max: 3})],
        subType: subTypes[faker.number.int({min: 0, max: 1})]
      });
    }
  
    return devices;
  }
  


async function createAndPopulateDevices(clickhouse, devices) {
    try {
        //Create devices table
        await clickhouse.query(`
        CREATE TABLE IF NOT EXISTS devices (
          id UUID,
          serial String,
          type String,
          sub_type String
        ) ENGINE = MergeTree()
        ORDER BY id
        `).toPromise();

        //Clear rows before new entry if table exists
        await clickhouse.query(`
        TRUNCATE TABLE devices;
        `).toPromise();

        // await clickhouse.query(`
        // INSERT INTO devices (id, serial, type, sub_type) VALUES
        // (generateUUIDv4(), 'SERIAL-12345678', 'type1', 'kallvatten')
        // `).toPromise();

        for (const {device_id, serial, type, subType} of devices) {
            await clickhouse.query(`
                INSERT INTO devices (id, serial, type, sub_type)
                VALUES ('${device_id}', '${serial}', '${type}', '${subType}')
            `).toPromise();
          }
        
        console.log(`${devices.length} devices inserted into ClickHouse.`);

        console.log('Devices table created and populated');

        //Create measurements table
        await clickhouse.query(`
        CREATE TABLE IF NOT EXISTS measurements (
            device_id UUID,
            value Float64,
            type Int16,
            timestamp DateTime('UTC')
        ) ENGINE = MergeTree() 
        ORDER BY timestamp;
        `).toPromise();

        //Clear rows before new entry if table exists
        await clickhouse.query(`
        TRUNCATE TABLE measurements;
        `).toPromise();

        console.log('Measurements table created')

        //Create measurements table
        await clickhouse.query(`
        CREATE TABLE IF NOT EXISTS organisations (
            organisation_id Int32,
            device_id UUID
        ) ENGINE = MergeTree() 
        ORDER BY organisation_id;
        `).toPromise();

        //Clear rows before new entry if table exists
        await clickhouse.query(`
        TRUNCATE TABLE organisations;
        `).toPromise();

        console.log('Organisations table created')

    } catch (error) {
        console.error('An error occurred:', error);
    }
}

rl.question('How many devices would you like to generate and insert? ', (numDevices) => {
    const num = parseInt(numDevices, 10);
    
    if (isNaN(num)) {
      console.log('Please enter a valid number.');
      rl.close();
      return;
    }
  
    const devicesData = generateDeviceData(num);
    // Assuming you have functions ready for inserting the data into the databases
    // and setup for both clickhouse and postgres or any database client instance you use.
    createAndPopulateDevices(clickhouse, devicesData); // Insert into ClickHouse
    
    rl.close(); // Don't forget to close the readline interface
  });

// const devicesData = generateDeviceData(500); // Generate once
// createAndPopulateDevices(clickhouse, devicesData); // Insert into ClickHouse

//createAndPopulateDevices();