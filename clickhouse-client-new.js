const { createClient } = require('@clickhouse/client')
const {faker} = require("@faker-js/faker")
const readline = require('readline');

// type Device = {
//     device_id: string;
//     serial:string;
//     type: string;
//     subType: string;
// }

faker.seed(12345);

const client =  createClient({
    url: 'http://localhost:18123',
})

function generateDeviceData(numOfDevices) {
    const types = ['type1', 'type2', 'type3', 'type4'];
    const subTypes = ['kallvatten', 'varmvatten'];
    // const devices: Device[] = [];
    const devices = [];

    console.time("faker time")
    for (let i = 0; i < numOfDevices; i++) {
      devices.push({
        device_id: faker.string.uuid(),
        serial: 'SERIAL-' + faker.number.int({min: 10000000, max: 99999999}).toString(),
        type: types[faker.number.int({min: 0, max: 3})],
        subType: subTypes[faker.number.int({min: 0, max: 1})]
      });
    }
    console.timeEnd("faker time")
  
    return devices;
  }


  async function createAndPopulateDevices(clickhouse, devices) {
    try {
        //Create devices table
        await clickhouse.query({
          query: `
            CREATE TABLE IF NOT EXISTS devices (
              id UUID,
              serial String,
              type String,
              sub_type String
            ) ENGINE = MergeTree()
            ORDER BY id
          `,
          // You might not need to specify a format for DDL operations.
        })

        //Clear rows before new entry if table exists
        await clickhouse.query({
          query: `TRUNCATE TABLE devices;`,
        })

        // await clickhouse.query(`
        // INSERT INTO devices (id, serial, type, sub_type) VALUES
        // (generateUUIDv4(), 'SERIAL-12345678', 'type1', 'kallvatten')
        // `).toPromise();

        console.time('insertTime');

        // Define batch size
        const batchSize = 10000; // Adjust based on your system's capacity
        
        for (let i = 0; i < devices.length; i += batchSize) {
            // Slice the devices array to get a batch
            const batch = devices.slice(i, i + batchSize);
        
            // Build values string for insertion
            const valuesStr = batch.map(({device_id, serial, type, subType}) => 
                `('${device_id}', '${serial}', '${type}', '${subType}')`
            ).join(',');
        
            // Perform batch insert
            await clickhouse.insert({
              table: 'devices',
              values: batch.map(({device_id, serial, type, subType}) => ({
                  id: device_id, // Ensure this matches the column name in your table
                  serial: serial,
                  type: type,
                  sub_type: subType // Ensure this matches the column name in your table
              })),
              format: 'JSONEachRow',
          });
        }
        
        console.timeEnd('insertTime');
        
        
        console.log(`${devices.length} devices inserted into ClickHouse.`);

        console.log('Devices table created and populated');

    } catch (error) {
        console.error('An error occurred:', error);
    }
}

const deviceData = generateDeviceData(1000000)
createAndPopulateDevices(client,deviceData)
