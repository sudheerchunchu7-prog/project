# Data Dictionary and Business Rules

## Entities and grain

- `customers`: one row per customer; key `customer_id`.
- `loan_applications`: one row per loan application; key `application_id`; many applications may belong to one customer.

## Source columns

| Column | Type | Nullable | Validation |
|---|---|---:|---|
| application_id | string | No | non-empty; unique in batch and target |
| customer_id | string | No | non-empty; must exist in customers |
| age | integer | Yes | 18–100 inclusive if present |
| loan_amount | decimal | Yes | > 0 and finite if present |
| status | string | No | approved / rejected / pending |
| event_timestamp | ISO-8601 | No | parseable timestamp with timezone recommended |

## Derived/operational fields

- `source_file`: original CSV filename.
- `run_id`: generated unique identifier for one pipeline execution.
- `batch_hash`: SHA-256 content fingerprint for idempotent reruns.
- `reject_reason`: one or more validation failures.
- `null_rate`: null_count / row_count.
- `distribution`: percentage share per category or fixed numeric bucket.

## Referential integrity

Every accepted `loan_applications.customer_id` must exist in `customers.customer_id`. Rejected rows retain raw input JSON and source row number.

## Naming

Tables and columns use `snake_case`; timestamps are stored as ISO-8601 UTC strings; identifiers are stable source keys where available.
