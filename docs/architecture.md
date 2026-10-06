# Architecture

```mermaid
flowchart LR
    A[Loan Applications CSV] --> B[Bronze]
    B --> C[Silver: Clean + Standardize]
    C --> D[Quality Gate]
    D --> E[Gold: Monthly KPIs]
    D --> F[Gold: Loan Type Analytics]
    D --> G[Gold: Customer Loan Profile]
    D --> H[Gold: Risk Analytics]
```

## Layers

- **Bronze:** raw loan application records.
- **Silver:** typed, deduplicated and validated applications with derived risk and affordability features.
- **Gold:** business-facing datasets for portfolio, customer, product and risk analytics.

## Production Azure Mapping

| Local project | Azure production equivalent |
|---|---|
| CSV | ADLS Gen2 landing zone |
| PySpark | Azure Databricks |
| Parquet | Delta Lake |
| Local pipeline | Databricks Workflow / ADF |
| Tests | CI/CD + Databricks Jobs |
| Data governance | Unity Catalog + Microsoft Purview |
