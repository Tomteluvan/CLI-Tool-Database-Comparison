const readline = require('readline');
const { generateDeviceData, generateMeasurementData, generateOrganisationData } = require('./generate_data');
const { createAndPopulateDevicesTimecale, createAndPopulateMeasurementsTimescale, createAndPopulateOrganisationsTimescale, initializeDatabaseTimescale, performQuery } = require('./timescaledb/timescaledb_generate_data');
const { createAndPopulateDevicesPostgres, createAndPopulateMeasurementsPostgres, createAndPopulateOrganisationsPostgres, initializeDatabasePostgres } = require('./postgres/postgreSQL_generate_data');
const { createAndPopulateDevicesClickHouse, createAndPopulateMeasurementsClickHouse, createAndPopulateOrganisationsClickHouse, initializeDatabaseClickHouse } = require('./clickhouse/clickhouse_generate_data');

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

let numDevices, numMeasurements, numOrganisations;
let devicesData, measurementsData, organisationsData;

function displayMainMenu() {
    console.log('\nMain Menu:');
    console.log('1. Generate Data');
    console.log('2. Update databases');
    console.log('3. Perform Queries');
    console.log('4. Extract logs')
    console.log('5. Exit');
    rl.question('Select an option: ', handleMainMenuSelection);
}

function displayDatabases() {
    console.log("\nChoose the database you want to use:");
    console.log("1. PostgreSQL");
    console.log("2. TimescaleDB");
    console.log("3. InfluxDB");
    console.log("4. ClickHouse");
    console.log("5. Go back")
    rl.question('Select an option: ', handleChoosenDatabase);
}

function displayQuery() {
    console.log("\nChoose the database you want to perform query in:");
    console.log("1. PostgreSQL or TimescaleDB");
    console.log("2. InfluxDB");
    console.log("3. ClickHouse");
    console.log("4. Go back")
    rl.question('Select an option: ', handleChoosenQuery);
} 

async function handleChoosenDatabase(option) {
    switch (option.trim()) {
        case '1':
            await initializeDatabasePostgres();
            await createAndPopulateDevicesPostgres(devicesData);
            await createAndPopulateMeasurementsPostgres(measurementsData);
            await createAndPopulateOrganisationsPostgres(organisationsData);
            displayDatabases();
            break;
        case '2':
            await initializeDatabaseTimescale();
            await createAndPopulateDevicesTimecale(devicesData);
            await createAndPopulateMeasurementsTimescale(measurementsData);
            await createAndPopulateOrganisationsTimescale(organisationsData);
            displayDatabases();
            break;
        case '3':
            // InfluxDB
            break;
        case '4':
            await initializeDatabaseClickHouse();
            await createAndPopulateDevicesClickHouse(devicesData);
            await createAndPopulateMeasurementsClickHouse(measurementsData);
            await createAndPopulateOrganisationsClickHouse(organisationsData);
            displayDatabases();
            break;    
        case '5':
            displayMainMenu();
            break;
        default:
            break;
    }
}

function handleMainMenuSelection(option) {
  switch (option.trim()) {
    case '1':
      rl.question('Enter the number of devices: ', (devices) => {
        numDevices = parseInt(devices);
        devicesData = generateDeviceData(numDevices)
        rl.question('Enter the number of measurement types: ', (measurements) => {
          numMeasurements = parseInt(measurements);
          measurementsData = generateMeasurementData(devicesData, numMeasurements)
          rl.question('Enter the number of organisations: ', (organisations) => {
            numOrganisations = parseInt(organisations);
            organisationsData = generateOrganisationData(devicesData, numOrganisations)
            console.log("\nNow, update the database with the generated data.");
            displayMainMenu();
          });
        });
      });
      break;
    case '2':
        displayDatabases();
        break;
    case '3':
        displayQuery();
        break;
    case '4':
        
        break;
    case '5':
        rl.close();
        break;
    default:
        console.log('Invalid option. Please select again.');
        displayMainMenu();
        break;
  }
}

async function handleChoosenQueryForTimescaleAndPostgres() {
    console.log("1. Retrieve data for one specific device.");
    console.log("2. Retrieve data for several devices within an organization.");

    return new Promise((resolve) => {
        rl.question('Select an option: ', async (option) => {
            await performQuery(option);
            resolve();
        });
    });
}


async function handleChoosenQuery(option) {
    switch (option) {
        case '1':
            await handleChoosenQueryForTimescaleAndPostgres();
            displayMainMenu();
            break;
        case '2':
            // InfluxDB
            break;
        case '3':
            // ClickHouse
            break;

        default:
            break;
    }
}

rl.on('close', () => {
    console.log('Exiting...');
    process.exit(0);
});

displayMainMenu();