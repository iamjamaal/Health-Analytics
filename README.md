  # Healthcare Analytics Star Schema

> **Transforming normalized OLTP data into an optimized OLAP star schema for healthcare analytics**

A complete data warehousing project demonstrating the design, implementation, and performance optimization of a star schema for healthcare encounter analytics. This project showcases a **15.4x average query performance improvement** over traditional normalized database queries.


##  Overview

This project transforms a normalized healthcare OLTP database into a dimensional star schema optimized for analytical queries. The system tracks patient encounters, diagnoses, procedures, providers, and financial metrics to support healthcare operations analytics.

### Business Context

Healthcare organizations need fast, reliable analytics to answer critical questions:
- How many patient encounters happen by specialty each month?
- What are the most common diagnosis-procedure combinations?
- What is our 30-day readmission rate by specialty?
- What is our revenue distribution across specialties and time periods?

Traditional normalized databases struggle with these analytical queries due to complex JOINs, runtime date calculations, and lack of pre-computed metrics.

### Solution

A dimensional star schema with:
- **Pre-computed date attributes** (eliminating DATE_FORMAT overhead)
- **Pre-aggregated readmission flags** (eliminating expensive self-joins)
- **Denormalized dimensions** (reducing JOIN complexity)
- **Optimized indexing** (composite and bitmap indexes)
- **Partitioning strategy** (date-based partition elimination)

**Result:** 5-40x faster query execution with simpler SQL.


### Project Structure 
 -  query_analysis.txt
 -  design_decisions.txt
 -  star_schema.sql
 -  star_schema_queries.txt
 -  etl_design.txt
 -  reflection.md


