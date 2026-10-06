# Business Requirements

## Objective
Analyze a bank's loan application portfolio to understand approval behavior, credit risk, product performance and customer borrowing patterns.

## Required Analytics

1. Monthly application and approval trends.
2. Approval rate by loan type.
3. Requested versus approved loan amount.
4. Customer-level approved loan exposure.
5. Risk-band distribution.
6. Average credit score and interest rate.
7. Month-over-month change in approved amount.
8. Ranking of customers by approved exposure.

## Data Quality Rules

- `loan_id` must be unique and non-null.
- `customer_id` must be non-null.
- `loan_amount` and `annual_income` must be positive.
- Credit score must be within an expected 300–900 range.
- Application status must be Approved or Rejected.
- Loan type must belong to the approved product list.
