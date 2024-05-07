SELECT
    EXTRACT(EPOCH FROM timezone('Europe/Berlin', date_trunc('year', timezone('Europe/Berlin', m.timestamp))))::integer AS ts,
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
    AND m.timestamp >= TO_TIMESTAMP(1704106800) 
    AND m.timestamp < TO_TIMESTAMP(1735642800)
GROUP BY
    date_trunc('year', timezone('Europe/Berlin', m.timestamp)),
    d.sub_type
ORDER BY
    date_trunc('year', timezone('Europe/Berlin', m.timestamp)),
    d.sub_type;
