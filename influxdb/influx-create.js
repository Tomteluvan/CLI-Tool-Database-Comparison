const {InfluxDB, Point} = require('@influxdata/influxdb-client')

// You can find your InfluxDB URL, token, and org in your InfluxDB UI or configuration.
const url = 'http://localhost:8086'
const org = 'quandify'
const token = 'M2comoATFn6FV5SkSwTlBeV4_g-Ecb9xb6v4LyzIKOy29k9Qze7rA-UPPvAerAqT0KSzrEwmO03hUq65BO76ow=='
const bucket = 'measurements'

const client = new InfluxDB({url, token})
const writeApi = client.getWriteApi(org, bucket)

// Create a new point (measurement) with the name 'device_measurement'
const point = new Point('device_measurement')
  .tag('device_id', '12345678-90ab-cdef-1234-567890abcdef') // Example device_id
  .tag('type', '1') // Example type
  .floatField('value', 22.5) // Example value
  .timestamp()
  // The timestamp is automatically set to the current time. If you need a custom timestamp, use .timestamp()

writeApi.writePoint(point)

writeApi
  .close()
  .then(() => {
    console.log('Data point written successfully.')
  })
  .catch(e => {
    console.error('Error writing data point:', e)
  })