# AI prompt history

**AI Tool:** ChatGPT

### 1. Baseline Profiling and Anomaly Detection

**Prompt used:**

I have implemented baseline profiling for incoming loan application batches. I want to ensure that volume changes, null-rate increases, categorical distribution shifts and freshness deviations are detected correctly. Review the monitoring approach and identify potential gaps in the detection logic.

**Changes made:**
- Reviewed baseline and current-batch comparison logic.
- Examined anomaly detection thresholds.
- Improved understanding of explainable monitoring alerts.

### 2. Data Validation and Rejection Handling

**Prompt used:**

My pipeline validates loan application records against a customer reference dataset. How can I ensure duplicate application IDs, invalid customer references, missing mandatory fields and negative loan amounts are handled consistently without losing rejected-record traceability?

**Changes made:**
- Reviewed record-level validation scenarios.
- Verified accepted and rejected record handling.
- Examined rejection reasons and reconciliation counts.

### 3. Idempotency and Recovery

**Prompt used:**

I have implemented batch-level idempotency using file fingerprints. Review whether the approach safely handles repeated executions and partial failures. What should happen when a previously failed batch is submitted again?

**Changes made:**
- Reviewed duplicate batch detection.
- Examined retry handling.
- Verified the expected behaviour for repeated batch execution.

### 4. Automated Test Coverage

**Prompt used:**

My pytest suite uses temporary datasets to test individual pipeline scenarios. Does this approach adequately validate the actual processing functions? Identify any gaps in test coverage, particularly around schema drift, invalid references, distribution changes and retry behaviour.

**Changes made:**
- Reviewed the existing test functions.
- Understood how temporary datasets invoke functions from `src`.
- Identified the distinction between unit-style tests and integration tests using actual sample batches.

### 5. Performance Benchmarking

**Prompt used:**

I have a separate benchmark workflow for generating large synthetic datasets. How can I verify that the benchmark measures the actual pipeline implementation rather than an independent processing routine? I also want to measure throughput and execution time at different data volumes.

**Changes made:**
- Executed the 100,000-row benchmark.
- Reviewed benchmark output and performance metrics.
- Identified the need to compare benchmark processing logic with the main pipeline.

### 6. Scalability and Optimization

**Prompt used:**

I want to evaluate how the pipeline behaves when the input volume increases from 100,000 to 1,000,000 records. What performance bottlenecks should I investigate, and how can I demonstrate a measurable optimization without changing the data-quality validation rules?

**Changes made:**
- Planned larger-volume benchmarking.
- Identified performance comparison metrics.
- Planned before-and-after optimization evidence.

### 7. Data Reconciliation and Observability

**Prompt used:**

Review my pipeline's reconciliation and operational reporting approach. How can I ensure that input, accepted and rejected counts remain consistent across processing stages, and that every execution is traceable through run IDs, status, errors and stage-level metrics?

**Changes made:**
- Reviewed reconciliation checks.
- Examined pipeline execution metadata.
- Reviewed stage-level reporting and run traceability.

### 8. Assignment Compliance Review

**Prompt used:**

Review my existing implementation against the data quality monitoring assignment. Focus on correctness, reliability, observability, automated testing, scalability and reproducibility. Identify missing requirements without replacing the existing architecture or generating a new project.

**Changes made:**
- Compared implemented functionality against assignment requirements.
- Identified areas requiring additional verification.
- Prepared a checklist for final execution evidence and documentation.

---


