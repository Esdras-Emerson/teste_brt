SELECT
    bus_id,
    CONCAT(latitude::VARCHAR, ', ', longitude::VARCHAR) AS position,
    speed
FROM {{ source('raw', 'raw_data') }}  