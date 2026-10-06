# Interview Questions

### 1. Why Medallion Architecture?
Bronze preserves source fidelity, Silver creates trusted standardized data, and Gold serves business use cases.

### 2. Why use PySpark?
Loan portfolios can become very large. Spark provides distributed processing while keeping transformations expressive through the DataFrame API.

### 3. Why use window functions?
They support analytics such as customer ranking, previous-period comparison and running calculations without collapsing rows through groupBy.

### 4. How is loan risk represented?
The sample derives a simple risk band from credit score. A production bank would combine additional bureau, affordability, delinquency and policy signals.

### 5. How would you productionize this?
Move input to ADLS Gen2, use Delta Lake, orchestrate with Databricks Workflows or ADF, govern with Unity Catalog/Purview, add incremental processing, quarantine invalid records and monitor pipeline SLAs.

### 6. What would you optimize at scale?
Partition by reporting date where appropriate, avoid unnecessary shuffles, broadcast small dimensions, select only required columns, use Delta statistics and optimize file sizes.
