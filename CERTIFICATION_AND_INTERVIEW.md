# Certification & interview prep

## Databricks Certified Data Engineer Associate
Exam topics change over time, so check the current **exam guide** on the Databricks certification page before you book.
Typical coverage, mapped to this course:

| Exam area | Modules |
|---|---|
| Databricks Intelligence Platform (workspace, compute, serverless, UC basics) | 00, 06 |
| Development & ingestion (notebooks, Auto Loader, COPY INTO, read_files, Spark SQL/PySpark) | 01, 03, 04 |
| Data processing & transformations (medallion, Delta, MERGE, LDP/DLT, expectations, CDC) | 02, 03, 05 |
| Productionizing (Lakeflow Jobs, Asset Bundles, repair runs, serverless jobs) | 07 |
| Data governance & quality (UC privileges, managed vs external, Delta Sharing, lineage, audit) | 06 |

**Professional** goes deeper: streaming internals, performance tuning, CDC/SCD design, testing, CI/CD, monitoring and cost (Modules 04, 07, 08 and the Capstone).

Study plan: finish Modules 00–07 → do the official free **Databricks Academy** self-paced courses for data engineering →
take practice exams → review every wrong answer against the docs.

## 40 interview questions (answer them out loud; each one maps to a module)
**Spark (01)**
1. Transformation vs action? Narrow vs wide? What triggers a shuffle?
2. Job vs stage vs task? How many tasks does a stage have?
3. What does Catalyst do? What does AQE do at runtime?
4. `repartition` vs `coalesce`?
5. Broadcast join: when, and what's the risk?
6. Why avoid Python UDFs? Alternatives?
7. `groupBy` vs window function: when do you need each?

**Delta (02)**
8. How does Delta provide ACID on object storage? What is in `_delta_log`?
9. How does time travel work, and how does VACUUM affect it?
10. What happens if two source rows match one target row in MERGE?
11. Schema enforcement vs evolution. How do you enable evolution?
12. OPTIMIZE vs Z-ORDER vs liquid clustering?
13. What are deletion vectors?
14. What is Change Data Feed used for?
15. Shallow vs deep clone?

**Pipelines (03–05)**
16. Explain the medallion architecture and what belongs in each layer.
17. How do you make a pipeline idempotent?
18. Auto Loader vs COPY INTO vs `spark.read`?
19. What is a checkpoint? What happens if you delete it?
20. `availableNow` vs `processingTime` vs continuous?
21. What does a watermark do? What happens to data later than it?
22. Why use `foreachBatch`?
23. Implement SCD2 with MERGE: walk through it.
24. Streaming table vs materialized view in LDP?
25. `expect` vs `expect_or_drop` vs `expect_or_fail`? How do you keep the dropped rows?
26. What does AUTO CDC (APPLY CHANGES) handle for you? What does `sequence_by` do?
27. How do you handle late-arriving or out-of-order data?

**Governance (06)**
28. Explain UC's 3-level namespace and the privileges needed to SELECT a table.
29. Managed vs external tables? What happens on DROP?
30. What is a volume?
31. Column masks / row filters vs dynamic views?
32. How do you find who accessed a table, and what feeds a table (lineage)?

**Ops (07–08)**
33. Job cluster vs all-purpose vs serverless: cost and use?
34. How do you pass values between tasks?
35. What are Asset Bundles, and how does dev mode differ from prod mode?
36. Describe your CI/CD for Databricks. How do you test pipelines?
37. A job got slow. Walk me through your debugging (Spark UI / query profile).
38. What is data skew and how do you fix it?
39. Small-file problem: causes and fixes?
40. How do you reduce Databricks cost?

## Behavioural story bank (fill in from your projects)
- **A data quality problem I caught:** Module 03/05 quarantine + DQ gate.
- **A performance win:** Module 08 timing table.
- **A pipeline I made reliable:** Module 04 checkpoints + idempotent SCD2 MERGE (including the replay bug and how you guarded against it).
- **Shipping safely:** Module 07 bundle + CI/CD with dev/prod targets.
