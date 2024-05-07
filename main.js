const readline = require('readline');
const { generateDeviceData, generateMeasurementData, generateOrganisationData } = require('./generate_data');
const { createAndPopulateDevicesTimescale, createAndPopulateOrganisationsTimescale, initializeDatabaseTimescale, performQueryTimescale, findAndExtractDataTimescale } = require('./timescaledb/timescaledb_generate_data');
const { createAndPopulateDevicesPostgres, createAndPopulateOrganisationsPostgres, initializeDatabasePostgres, performQueryPostgres, findAndExtractDataPostgres } = require('./postgres/postgreSQL_generate_data');
const { createAndPopulateDevicesClickHouse, createAndPopulateOrganisationsClickHouse, initializeDatabaseClickHouse, performQueryForClickHouse } = require('./clickhouse/clickhouse_generate_data');
const { resolve } = require('path');

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

let numDevices, numPeriodOfTime;
let devicesData, measurementsData, organisationsData;

function displayMainMenu() {
    console.log('\nMain Menu:');
    console.log('1. Generate Data');
    console.log('2. Update databases');
    console.log('3. Perform Queries');
    console.log('4. Exit');
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
    console.log("1. PostgreSQL");
    console.log("2. TimescaleDB")
    console.log("3. InfluxDB");
    console.log("4. ClickHouse");
    console.log("5. Go back")
    rl.question('Select an option: ', handleChoosenQuery);
} 

async function handleChoosenDatabase(option) {
    switch (option.trim()) {
        case '1':
            await initializeDatabasePostgres();
            await createAndPopulateDevicesPostgres(devicesData);
            await createAndPopulateOrganisationsPostgres(organisationsData);
            displayDatabases();
            break;
        case '2':
            await initializeDatabaseTimescale();
            await createAndPopulateDevicesTimescale(devicesData);
            await createAndPopulateOrganisationsTimescale(organisationsData);
            displayDatabases();
            break;
        case '3':
            // InfluxDB
            break;
        case '4':
            await initializeDatabaseClickHouse();
            await createAndPopulateDevicesClickHouse(devicesData);
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

async function handleMainMenuSelection(option) {
    switch (option.trim()) {
      case '1':
        rl.question('Enter the number of devices: ', async (devices) => {
          numDevices = parseInt(devices);
          devicesData = generateDeviceData(numDevices);
          rl.question('For a one-month period, type (1). For a one-year period, type (2): ', async (periodTime) => {
            numPeriodOfTime = parseInt(periodTime);
            measurementsData = await generateMeasurementData(devicesData, numPeriodOfTime);
            organisationsData = generateOrganisationData(devicesData);
            console.log("\nNow, update the database with the generated data.");
            displayMainMenu();
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
        rl.close();
        break;
      default:
        console.log('Invalid option. Please select again.');
        displayMainMenu();
        break;
    }
}

async function handleChoosenQuery(option) {
    switch (option) {
        case '1':
            await performQueryPostgres();
            await findAndExtractDataPostgres();
            displayMainMenu();
            break;
        case '2':
            await performQueryTimescale();
            await findAndExtractDataTimescale();
            displayMainMenu();
            break;
        case '3':
            // InfluxDB
            break;
        case '4':
            await performQueryForClickHouse();
            displayMainMenu();
            break;
        case '5':
            displayMainMenu();
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