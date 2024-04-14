const {faker} = require("@faker-js/faker")

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

function generateMeasurementData(devicesData, numOfTypes) {
    const measurementsBatch = [];

    console.time('faker time')

    for (const device of devicesData) {

        let genHour = new Date('2024-01-01T12:00:00Z')

        while (genHour <= new Date('2024-02-01T12:00:00Z')) {
            for (let typeId = 1; typeId <= numOfTypes; typeId++) {
                
                measurementsBatch.push({
                    device_id: device.device_id,
                    value: faker.number.float({ min: 0.0, max: 100.0 }),
                    type: typeId,
                    timestamp: genHour
                });
            }

            genHour = new Date(genHour.getTime() + 60 * 60 * 1000);
        }
    }
    console.timeEnd('faker time')
    return measurementsBatch;
}

function generateOrganisationData(devices, numOfOrganisations) {
    // Creates organisation ids and also assigns the device uuids to those 
    // Assigning 50% of the devices to the first organisations then the rest equally
    const assignments = [];
    const devicesPerOrg = Math.floor(devices.length / numOfOrganisations);
    let currentOrg = 1;
    let currentOrgDeviceCount = 0;

    console.time("faker time")
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

    console.timeEnd("faker time")

    return assignments;
}

module.exports = {
    generateDeviceData,
    generateMeasurementData, 
    generateOrganisationData
};