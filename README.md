# Analytics

## Database

We use [dbmate](https://github.com/amacneil/dbmate) to migrate the database
schema. See the migrations in /db/migrations/. It stores the current schema in
/db/schema.sql.

The raw table is one giant document store. To make it more query-friendly, we create views for each event type we need, such as `event_navigate`.
