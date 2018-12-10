# geocoder-pfs

[PFS function](https://pivotal.io/platform/pivotal-function-service) that takes in events from the [USGS Event Source](https://github.com/gswk/usgs-event-source) and uses the Google Maps API to reverse-geocode the coordinates, and then write them to a Postgres database.

Expected Environment Variables
---
- **PGHOST** = Hostname for Postgres server
- **PGPORT** = Port for Postgres server
- **PGDATABASE** = Name of database in Postgres server
- **PGUSER** = Postgres username
- **PGPASSWORD** = Postgres password
- **GOOGLE_API_KEY** = Google API Key with access to the Maps API
