const {flux} = require('@influxdata/influxdb-client');

const oneMonthInflux = flux `from(bucket: "measurements") |> range(start: 2024-01-01T11:00:00Z, stop: 2024-01-31T11:00:00Z) |> filter(fn: (r) => r._measurement == "measurement" and r.organisation_id == "1" and r.measurement_type == "5") |> group(columns: ["device_subtype"]) |> sum(column: "_value") |> group() |> sort(columns: ["_time"])`;

module.exports = {
    oneMonthInflux: oneMonthInflux
}
3
