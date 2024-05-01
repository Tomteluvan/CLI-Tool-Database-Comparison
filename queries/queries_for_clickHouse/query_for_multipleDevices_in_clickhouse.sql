SELECT
    toUnixTimestamp(date_trunc('month', toDateTime(m.timestamp, 'Europe/Berlin'))) AS ts,
    SUM(value) AS value,
    d.sub_type AS type
FROM
    measurements AS m
JOIN
    devices AS d ON d.id = m.device_id
JOIN
    organisations AS o ON d.id = o.device_id
WHERE
    o.organisation_id = '1'
    AND m.type = 5
    AND m.timestamp >= toDateTime(1704106800)
    AND m.timestamp < toDateTime(1706698800)
GROUP BY
    ts,
    d.sub_type
ORDER BY
    ts ASC,
    d.sub_type ASC;