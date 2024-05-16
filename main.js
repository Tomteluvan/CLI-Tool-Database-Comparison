const readline = require('readline');
const { generateDeviceData, generateMeasurementData, generateOrganisationData } = require('./generate_data');
const { createAndPopulateDevicesTimescale, createAndPopulateOrganisationsTimescale, createAndPopulateMeasurementsTimescale, initializeDatabaseTimescale, performQueryTimescaleForMonth, performQueryTimescaleForYear, findAndExtractDataTimescale } = require('./timescaledb/timescaledb_generate_data');
const { createAndPopulateDevicesPostgres, createAndPopulateOrganisationsPostgres, createAndPopulateMeasurementsPostgres, initializeDatabasePostgres, performQueryPostgresForMonth, performQueryPostgresForYear, findAndExtractDataPostgres } = require('./postgres/postgreSQL_generate_data');
const { createAndPopulateDevicesClickHouse, createAndPopulateOrganisationsClickHouse, createAndPopulateMeasurementsClickHouse, initializeDatabaseClickHouse, performQueryForClickHouseForMonth, performQueryForClickHouseForYear } = require('./clickhouse/clickhouse_generate_data');
const { performQueryInflux1Month, performQueryInflux1Year, generateAndWriteMeasurements } = require('./influxdb/influx-create')

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

let numDevices;
let devicesData, organisationsData;

function displayMainMenu() {
    console.log('\nMain Menu:');
    console.log('1. Generate data and update database');
    console.log('2. Perform Queries and get statistics');
    console.log('3. Exit');
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

// Function to wrap rl.question in a promise
function askQuestion(query) {
    return new Promise((resolve) => rl.question(query, resolve));
}

async function handleChoosenDatabase(option) {
    switch (option.trim()) {
        case '1':
            // PostgreSQL
            await initializeDatabasePostgres();
            await createAndPopulateMeasurementsPostgres();

            // Wait for the user to enter the number of devices
            const devices = await askQuestion('Enter the number of devices: ');
        
            numDevices = parseInt(devices);
            devicesData = generateDeviceData(numDevices);
            measurementsData = await generateMeasurementData(devicesData, 1);
            organisationsData = generateOrganisationData(devicesData);
        
            await createAndPopulateDevicesPostgres(devicesData);
            await createAndPopulateOrganisationsPostgres(organisationsData);
            
            displayMainMenu();
            break;

        case '2':
            // TimescaleDB
            await initializeDatabaseTimescale();
            await createAndPopulateMeasurementsTimescale();
            rl.question('Enter the number of devices: ', async (devices) => {
                numDevices = parseInt(devices);
                devicesData = generateDeviceData(numDevices);
                measurementsData = await generateMeasurementData(devicesData, 2);
                organisationsData = generateOrganisationData(devicesData);
            });
            await createAndPopulateDevicesTimescale(devicesData);
            await createAndPopulateOrganisationsTimescale(organisationsData);
            displayMainMenu();
            break;

        case '3':
            // InfluxDB
            rl.question('Enter the number of devices: ', (devices) => {
                numDevices = parseInt(devices);
                devicesData = generateDeviceData(numDevices)
                organisationsData = generateOrganisationData(devicesData)
            });
            await generateAndWriteMeasurements(devicesData, organisationsData)
            displayMainMenu();
            break;

        case '4':
            // ClickHouse
            await initializeDatabaseClickHouse();
            await createAndPopulateMeasurementsClickHouse();
            rl.question('Enter the number of devices: ', async (devices) => {
                numDevices = parseInt(devices);
                devicesData = generateDeviceData(numDevices);
                measurementsData = await generateMeasurementData(devicesData, 3);
                organisationsData = generateOrganisationData(devicesData);
            });
            await createAndPopulateDevicesClickHouse(devicesData);
            await createAndPopulateOrganisationsClickHouse(organisationsData);
            displayMainMenu();
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
        displayDatabases();
        break;
      case '2':
        displayQuery();
        break;
      case '3':
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
            await performQueryPostgresForMonth();
            await findAndExtractDataPostgres();
            await performQueryPostgresForYear();
            await findAndExtractDataPostgres();
            displayMainMenu();
            break;
        case '2':
            await performQueryTimescaleForMonth();
            await findAndExtractDataTimescale();
            await performQueryTimescaleForYear();
            await findAndExtractDataTimescale();
            displayMainMenu();
            break;
        case '3':
            // InfluxDB
            await performQueryInflux1Month();
            await performQueryInflux1Year();
            displayMainMenu();
            break;
        case '4':
            await performQueryForClickHouseForMonth();
            await performQueryForClickHouseForYear();
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