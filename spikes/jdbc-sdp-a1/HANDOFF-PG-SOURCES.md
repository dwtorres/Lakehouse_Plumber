# PG Sources Handoff — Spike A1

## Foreign Catalog

| Key | Value |
|-----|-------|
| Catalog | `pg_supabase` |
| Connection | `supabase` (POSTGRESQL, read-only) |
| Connection host | `aws-0-us-west-2.pooler.supabase.com:5432` |
| Create option | `OPTIONS (database 'postgres')` |
| DDL | `CREATE FOREIGN CATALOG IF NOT EXISTS pg_supabase USING CONNECTION supabase OPTIONS (database 'postgres')` |
| Status | SUCCEEDED — confirmed in catalog list |

## Schema Inventory (pg_supabase)

17 schemas total. AW base-table schemas:

| Schema | Base Tables | Views |
|--------|-------------|-------|
| humanresources | 6 | 3 |
| person | 13 | 1 |
| production | 26 | 2 |
| purchasing | 5 | 2 |
| sales | 23 | 4 |
| **Total AW** | **73** | **12** |

Non-AW schemas (not targets): `auth`, `extensions`, `graphql`, `graphql_public`, `information_schema`, `pg_catalog`, `pgbouncer`, `public`, `realtime`, `storage`, `topology`, `vault`.

## Federation Sanity Check

| Table | COUNT(*) | Status |
|-------|----------|--------|
| `sales.salesorderheader` | 31,465 | OK |
| `production.transactionhistory` | 113,443 | OK |
| `person.person` | 19,972 | OK |

Predicate pushdown functional. `DESCRIBE TABLE` returns full column metadata via federation.

## Overflow Risk

**None.** Postgres `NUMERIC`/`DECIMAL` columns all have explicit precision: `decimal(19,4)`. No unbounded `NUMERIC` columns found. The Oracle `NUMBER` → `DECIMAL(38,10)` overflow issue does **not** repeat here. No decimal-column-skip workaround needed.

## Table Specs (73 base tables)

`source_schema | source_table | watermark_column | notes`

| source_schema | source_table | watermark_column | notes |
|---------------|-------------|------------------|-------|
| humanresources | department | ModifiedDate | |
| humanresources | employee | ModifiedDate | |
| humanresources | employeedepartmenthistory | ModifiedDate | |
| humanresources | employeepayhistory | ModifiedDate | also has RateChangeDate |
| humanresources | jobcandidate | ModifiedDate | |
| humanresources | shift | ModifiedDate | |
| person | address | ModifiedDate | |
| person | addresstype | ModifiedDate | |
| person | businessentity | ModifiedDate | |
| person | businessentityaddress | ModifiedDate | |
| person | businessentitycontact | ModifiedDate | |
| person | contacttype | ModifiedDate | |
| person | countryregion | ModifiedDate | |
| person | emailaddress | ModifiedDate | |
| person | password | ModifiedDate | contains hashed creds — hash-skip or exclude in devtest |
| person | person | ModifiedDate | |
| person | personphone | ModifiedDate | |
| person | phonenumbertype | ModifiedDate | |
| person | stateprovince | ModifiedDate | |
| production | billofmaterials | ModifiedDate | |
| production | culture | ModifiedDate | |
| production | document | ModifiedDate | has xml/binary doc columns |
| production | illustration | ModifiedDate | has xml/binary columns |
| production | location | ModifiedDate | |
| production | product | ModifiedDate | |
| production | productcategory | ModifiedDate | |
| production | productcosthistory | ModifiedDate | |
| production | productdescription | ModifiedDate | |
| production | productdocument | ModifiedDate | |
| production | productinventory | ModifiedDate | |
| production | productlistpricehistory | ModifiedDate | |
| production | productmodel | ModifiedDate | has xml columns |
| production | productmodelillustration | ModifiedDate | |
| production | productmodelproductdescriptionculture | ModifiedDate | |
| production | productphoto | ModifiedDate | has binary photo columns — may need skip |
| production | productproductphoto | ModifiedDate | |
| production | productreview | ModifiedDate | |
| production | productsubcategory | ModifiedDate | |
| production | scrapreason | ModifiedDate | |
| production | transactionhistory | ModifiedDate | high-volume (113k rows) |
| production | transactionhistoryarchive | ModifiedDate | archive table |
| production | unitmeasure | ModifiedDate | |
| production | workorder | ModifiedDate | |
| production | workorderrouting | ModifiedDate | |
| purchasing | productvendor | ModifiedDate | |
| purchasing | purchaseorderdetail | ModifiedDate | |
| purchasing | purchaseorderheader | ModifiedDate | |
| purchasing | shipmethod | ModifiedDate | |
| purchasing | vendor | ModifiedDate | |
| sales | countryregioncurrency | ModifiedDate | |
| sales | creditcard | ModifiedDate | masked card data |
| sales | currency | ModifiedDate | |
| sales | currencyrate | ModifiedDate | |
| sales | customer | ModifiedDate | |
| sales | personcreditcard | ModifiedDate | |
| sales | salesorderdetail | ModifiedDate | |
| sales | salesorderheader | ModifiedDate | high-volume (31k rows) |
| sales | salesorderheadersalesreason | ModifiedDate | |
| sales | salesperson | ModifiedDate | |
| sales | salespersonquotahistory | ModifiedDate | |
| sales | salesreason | ModifiedDate | |
| sales | salestaxrate | ModifiedDate | |
| sales | salesterritory | ModifiedDate | |
| sales | salesterritoryhistory | ModifiedDate | |
| sales | shoppingcartitem | ModifiedDate | |
| sales | specialoffer | ModifiedDate | |
| sales | specialofferproduct | ModifiedDate | |
| sales | store | ModifiedDate | |

**73 base tables total.** Short of the 100-table target. Options to close the gap:
1. Include the 12 views above (not recommended — no reliable watermark on views).
2. Load the AW HR/Sales sample data into a second Supabase schema (e.g. `adventureworks2`) for duplicate coverage.
3. Supplement with `freesql_catalog` Oracle tables (already proven in A1).
4. Accept 73 as the scale ceiling and adjust spike target to `N=73`.

## Recommended Constants for `tasks/fixtures_discovery.py`

```python
SOURCE_CATALOG = "pg_supabase"
SOURCE_SCHEMAS = ["humanresources", "person", "production", "purchasing", "sales"]
WATERMARK_COLUMN = "ModifiedDate"   # present on all 73 base tables
```

## Blockers for Phase 2 Agents

1. **73 not 100 tables.** AW on Supabase has 73 base tables with watermarks. Scale test must be parameterized to `N=73` or augmented (see options above).
2. **Binary/XML columns.** `production.document`, `production.illustration`, `production.productmodel`, `production.productphoto` contain XML or binary columns. If the spike SELECT * query hits these, Spark may fail on type mapping. Recommend explicit column projection or skip those 4 tables initially.
3. **`person.password` table.** Contains hashed credentials. Even though hashed, include in the HIPAA-sensitive-column skip list for devtest per LHP policy.
4. **Pooler connection.** `supabase` connection points to the PgBouncer pooler (`aws-0-us-west-2.pooler.supabase.com`) not direct Postgres. Session-mode queries work; avoid transaction-mode-incompatible DDL.
5. **No new tables to create.** Foreign catalog is read-only by connection definition (`read_only: true` on connection). Bronze targets must remain under `devtest_edp_bronze.jdbc_spike` as in A1.
