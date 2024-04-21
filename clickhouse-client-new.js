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

    console.time("faker time devices")
    for (let i = 0; i < numOfDevices; i++) {
      devices.push({
        device_id: faker.string.uuid(),
        serial: 'SERIAL-' + faker.number.int({min: 10000000, max: 99999999}).toString(),
        type: types[faker.number.int({min: 0, max: 3})],
        subType: subTypes[faker.number.int({min: 0, max: 1})]
      });
    }
    console.timeEnd("faker time devices")
  
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

function generateMeasurements(devices, numOfMeasurements, numOfTypes) {
  const measurements = [];

  
  console.time("faker time measurements")
  devices.forEach(device => {
    for (let typeIndex = 1; typeIndex <= numOfTypes; typeIndex++) {
      for (let measurementIndex = 0; measurementIndex < numOfMeasurements; measurementIndex++) {
        measurements.push({
          device_id: device.device_id, // Use the device_id from the devices array
          value: faker.number.float({min: 0.0, max: 100.0}), // Assuming value range, adjust as needed
          type: typeIndex, // Assuming type is a simple integer from 1 to numOfTypes
          timestamp: faker.date.recent() // this needs to be made so its realistic, j
        });
      }
    }
  });
  console.timeEnd("faker time measurements")

  return measurements;
}

async function createAndPopulateMeasurements(clickhouse, measurements) {
  try {
    //Create measurments table
    await clickhouse.query({
      query: `
        CREATE TABLE IF NOT EXISTS measurements (
          device_id UUID,
          value Float64,
          type Int16,
          timestamp DateTime('UTC')
      ) ENGINE = MergeTree() 
      ORDER BY timestamp;
      `,
      // You might not need to specify a format for DDL operations.
    })

    //Clear rows before new entry if table exists
    await clickhouse.query({
      query: `TRUNCATE TABLE measurements;`,
    })

    // Define batch size
    const batchSize = 10000; // Adjust based on your system's capacity

    console.time("measurement insert")
    // Insert measurements in batches
    for (let i = 0; i < measurements.length; i += batchSize) {
      const batch = measurements.slice(i, i + batchSize);

      // Insert batch using the clickhouse.insert method and JSONEachRow format
      await clickhouse.insert({
        table: 'measurements',
        values: batch.map(({device_id, value, type, timestamp}) => ({
            device_id: device_id,
            value: value,
            type: type,
            timestamp: timestamp.toISOString().replace('T', ' ').replace(/\.\d+Z$/, '') // Adjusted format
        })),
        format: 'JSONEachRow', // This tells ClickHouse how to parse the data
      });
    }
    console.timeEnd("measurement insert")

    console.log(`${measurements.length} measurements inserted into ClickHouse.`);

    console.log('Measurements have been inserted successfully.');
  } catch (error) {
    console.error('Error inserting measurements data:', error);
  }
    

}

async function createAndPopulateOrganisations(clickhouse, assignments) {
  try {
    await clickhouse.query({
      query: `
        CREATE TABLE IF NOT EXISTS organisations (
          organisation_id Int32,
          device_id UUID
        ) ENGINE = MergeTree()
        ORDER BY organisation_id;
      `,
    });

    // Clear rows before new entry if table exists
    await clickhouse.query({
      query: `TRUNCATE TABLE organisations;`,
    });

    console.time("organisation insert");
    // Define batch size
    const batchSize = 10000; // Adjust based on your system's capacity

    // Insert assignments in batches
    for (let i = 0; i < assignments.length; i += batchSize) {
      const batch = assignments.slice(i, i + batchSize);

      // Insert batch using the clickhouse.insert method and JSONEachRow format
      await clickhouse.insert({
        table: 'organisations',
        values: batch,
        format: 'JSONEachRow',
      });
    }
    console.timeEnd("organisation insert");



    console.log('Organisations table created.');
  } catch (error) {
    console.error('Error creating organisations table:', error);
  }
}

function generateOrganisations(devices, numOfOrganisations) {
  //creates organisation ids and also assigns the device uuids to those 
  //assigning 50% of the devices to the first organisations then the rest equally
  const assignments = [];
  const devicesPerOrg = Math.floor(devices.length / numOfOrganisations);
  let currentOrg = 1;
  let currentOrgDeviceCount = 0;

  devices.forEach((device, index) => {
    if (currentOrg === 1 && index >= devices.length / 2) {
      // Move to the next organisation after assigning half to the first
      currentOrg++;
      currentOrgDeviceCount = 0;
    } else if (currentOrgDeviceCount >= devicesPerOrg && currentOrg < numOfOrganisations) {
      // Move to the next organisation after equal distribution, except for the last one
      currentOrg++;
      currentOrgDeviceCount = 0;
    }

    assignments.push({
      organisation_id: currentOrg,
      device_id: device.device_id,
    });

    currentOrgDeviceCount++;
  });

  return assignments;
}

async function getAggregatedMeasurementsByDeviceAndType(clickhouse, deviceId, measurementType, fromTimestamp, toTimestamp, granularity) {
  // Format timestamps to ClickHouse's expected format if needed
  // Ensure fromTimestamp and toTimestamp are in 'YYYY-MM-DD HH:MM:SS' format

  // The timezone adjustment is handled by converting timestamps to the desired timezone.
  // Since the data is in UTC and ClickHouse stores timestamps in UTC by default, 
  // the conversion can be done via date functions if necessary. Here it's assumed you want results in UTC.

  try {
    await clickhouse.query({
      query: ` SELECT
          toUnixTimestamp(dateTrunc('${granularity}', m.timestamp)) AS ts,
          sum(m.value) AS value,
          d.sub_type AS type
      FROM
          measurements AS m
      JOIN
          devices AS d ON d.id = m.device_id
      WHERE
          d.id = '${deviceId}' AND
          m.type = ${measurementType} AND
          m.timestamp >= '${fromTimestamp}' AND
          m.timestamp < '${toTimestamp}'
      GROUP BY
          ts,
          d.sub_type
      ORDER BY
          ts,
          d.sub_type
      `

    });

  } catch (error) {
      console.error("Error fetching aggregated measurements:", error);
      throw error;
  }
}



const deviceData = generateDeviceData(10)
// const measurements = generateMeasurements(deviceData, 20, 5)
// const organisations = generateOrganisations(deviceData, 2)

console.log(deviceData[0])
console.log(deviceData[1])
console.log(deviceData[2])


// createAndPopulateDevices(client,deviceData)
// createAndPopulateMeasurements(client, measurements)
// createAndPopulateOrganisations(client, organisations)

// getAggregatedMeasurementsByDeviceAndType(client, "71315ca1-53c2-4cad-82f4-17e155d5ec01",2,"2024-04-09 09:00:00","2024-04-09 10:00:00","hour")
