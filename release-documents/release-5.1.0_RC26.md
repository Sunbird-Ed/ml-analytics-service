
---

# Project PySpark Batch Pipeline – Release Note

---

## 1. Overview

The Project batch pipeline is split into **two focused PySpark scripts** to enable:

* Incremental ingestion
* Precise Druid segment management
* Reduced reprocessing cost
* Clear separation of responsibilities

### Scripts

| Script                         | Responsibility                                                                   |
| ------------------------------ | -------------------------------------------------------------------------------- |
| `pyspark_project_batch_raw.py` | Raw extraction, solution-wise processing, cloud upload, **sl-project ingestion** |
| `pyspark_project_batch_agg.py` | Aggregation & analytics ingestion for **project and program-level status datasources**                         |

---

## 2. pyspark_project_batch_raw.py

### Purpose

Handles **raw project data ingestion** into the **`sl-project` Druid datasource** with **solution-level granularity**.

---

## 2.1 First-Time Execution Flow

Used when:

* Running the pipeline for the very first time
* Performing a full reset of the `sl-project` datasource

### Step-by-Step Logic

1. **Drop all segments from `sl-project` datasource**

   * Ensures a clean state before ingestion

2. **Fetch all distinct `solution_id`s**

   * Source: MongoDB `projects` collection
   * Filters applied:

     * `isAPrivateProgram = false`
     * `isDeleted = false`

3. **Fetch project data per `solution_id`**

   * Projects are processed independently for each solution

4. **Process and flatten data**

   * Flattens:

     * projects
     * tasks
     * subtasks
     * evidences & remarks
   * Produces solution-wise raw datasets

5. **Upload processed data to cloud storage**

   * Output format:

     ```
     sl_project_<solution_id>.json
     ```

6. **Ingest solution-wise data from cloud into Druid**

7. **Segment granularity configuration**

   * Segment granularity: **SECOND**
   * Reference timestamp column:

     ```
     solution_created_at
     ```

#### Why SECOND Granularity?

* `solution_created_at` is **unique per solution**
* Guarantees:

  * One segment per solution
  * No segment overlap
  * Precise segment deletion & re-ingestion

---

## 2.2 Incremental Execution Flow (Not First Time)

Used for:

* Daily runs
* Incremental updates
* Partial reprocessing

### Step-by-Step Logic

1. **Query `programActivityLogs` collection**

   * Fetch documents from the **last 3 days**
   * Extract updated `solution_id`s

2. **For each updated `solution_id`**

   * Fetch `solution_created_at` timestamp
   * Drop **only the Druid segment created in that exact second**

3. **Continue standard processing**

   * Fetch project data
   * Process & flatten
   * Upload solution-wise JSON to cloud
   * Ingest updated data into Druid

> Only impacted solution segments are touched — the rest of the datasource remains unchanged.

---

## 2.3 Benefits of Raw Pipeline Design

* ✅ Avoids dropping the entire datasource
* ✅ Enables true incremental ingestion
* ✅ Precise segment-level reprocessing
* ✅ Lower compute and ingestion cost

---

## 3. pyspark_project_batch_agg.py

### Purpose

Handles **aggregation and analytics ingestion**, maintaining the same logic as the legacy `pyspark_project_batch.py`, but **limited to aggregated datasets only**.

---

### Scope of This Script

This script **only manages the following Druid datasources**:

* `ml-project-status`
* `ml-project-programLevel-status`

---

### Key Characteristics

* Aggregation logic remains **unchanged** from legacy script
* Processes:

  * Project status metrics
  * Program-level project status metrics
* Responsible for:

  * Aggregation
  * Datasource creation
  * Druid ingestion for Aggregated datasets

---

### ⚠️ Segment Granularity (IMPORTANT)

| Script                         | Segment Granularity |
| ------------------------------ | ------------------- |
| `pyspark_project_batch_raw.py` | **SECOND**          |
| `pyspark_project_batch_agg.py` | **DAY**             |

**Reason for DAY granularity in aggregation layer:**

* Aggregated datasets are:

  * Date-based
  * Roll-up oriented
  * Not solution-specific
* Day-level segments are optimal for:

  * Query performance
  * Reduced segment count

---

## 4. Execution Diagrams

### 4.1 First-Time Run

```
MongoDB
  │
  ├─ Fetch all solution_ids
  │
  ▼
pyspark_project_batch_raw.py
  │
  ├─ Drop entire sl-project datasource
  ├─ Process each solution
  ├─ Upload solution-wise JSON
  │
  ▼
Druid (sl-project)
  └─ SECOND-level segments (solution_created_at)
```

---

### 4.2 Incremental Run

```
MongoDB
  │
  ├─ Query programActivityLogs (last 3 days)
  │
  ▼
Updated solution_ids
  │
  ├─ Drop specific SECOND-level segments
  │
  ▼
pyspark_project_batch_raw.py
  │
  ├─ Reprocess only updated solutions
  │
  ▼
Druid (sl-project)
```

---

### 4.3 Aggregation Pipeline

```
Processed Project Data
  │
  ▼
pyspark_project_batch_agg.py
  │
  ├─ Project-level aggregation
  ├─ Program-level aggregation
  │
  ▼
Druid
  ├─ ml-project-status (DAY granularity)
  └─ ml-project-programLevel-status (DAY granularity)
```

---

## 5. Command-Line Examples

### 5.1 Running `pyspark_project_batch_raw.py`


#### 1. First-Time Full Ingestion

```bash
python3 pyspark_project_batch_raw.py --is-first-time True
```

#### 2. Incremental Daily Run

```bash
python3 pyspark_project_batch_raw.py
```

### 5.2 Running `pyspark_project_batch_agg.py`

#### 1. Full Ingestion Daily Run

```bash
python3 pyspark_project_batch_agg.py --program_id <program_id>
```

---
