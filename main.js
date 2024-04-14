const readline = require('readline');
const { generateDeviceData, generateMeasurementData, generateOrganisationData } = require('./generate_data');
const { createAndPopulateDevices, createAndPopulateMeasurements, createAndPopulateOrganisations, initializeDatabase } = require('./timescaledb/timescaledb_generate_data');

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
  console.log('4. Exit');
  rl.question('Select an option: ', handleMainMenuSelection);
}

function displayDatabases() {
    console.log("\n Choose the database you want to use:");
    console.log("1. PostgreSQL");
    console.log("2. TimescaleDB");
    console.log("3. Go back")
    rl.question('Select an option: ', handleChoosenDatabase);
}

async function handleChoosenDatabase(option) {
    switch (option.trim()) {
        case '1':
            
            break;
        case '2':
            const { sequelize, devices, measurements, organisations } = await initializeDatabase();
            await createAndPopulateDevices(sequelize, devices, devicesData);
            await createAndPopulateMeasurements(sequelize, measurements, measurementsData);
            await createAndPopulateOrganisations(sequelize, organisations, organisationsData);
            displayDatabases();
            break;
        case '3':
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
            displayMainMenu();
          });
        });
      });
      break;
    case '2':
        displayDatabases();
      break;
    case '3':
      performQueries();
      displayMainMenu();
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

rl.on('close', () => {
  console.log('Exiting...');
  process.exit(0);
});

displayMainMenu();