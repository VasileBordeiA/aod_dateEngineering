CREATE TABLE IF NOT EXISTS taxi.zones
(
    location_id  UInt16                 COMMENT 'TLC taxi zone id, 1-265. Join target for trips.pu_location_id / do_location_id.',
    borough      LowCardinality(String) COMMENT 'Manhattan, Brooklyn, Queens, Bronx, Staten Island, EWR (Newark airport), Unknown, N/A.',
    zone         String                 COMMENT 'Zone name, e.g. ''JFK Airport'', ''Midtown Center'', ''Williamsburg (North Side)''.',
    service_zone LowCardinality(String) COMMENT 'Yellow Zone = Manhattan core; Boro Zone = outer boroughs; Airports; EWR.',
    lat          Nullable(Float64)      COMMENT 'Zone centroid latitude (WGS84). NULL for 264/265 and the few zones without their own shape.',
    lon          Nullable(Float64)      COMMENT 'Zone centroid longitude (WGS84). NULL for 264/265.'
)
ENGINE = MergeTree ORDER BY location_id
COMMENT 'The 265 NYC TLC taxi zones with borough and centroid coordinates.';
