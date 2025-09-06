# Drilldown Data Pipeline

A complete end-to-end data pipeline using **Airflow** for orchestration, **dbt** for data transformation, and **BigQuery** for data warehousing.

## 🏗️ Architecture

```
Raw Data (CSV) → BigQuery (Raw) → dbt (Core) → dbt (Marts) → Data Quality Gate
```

- **Raw Layer**: CSV files loaded into `drilldown_raw` dataset
- **Core Layer**: Staging, dimensions, and fact tables in `drilldown_core` dataset
- **Marts Layer**: Business logic models in `drilldown_mart` dataset
- **Quality Gate**: Automated data quality validation

## 📋 Prerequisites

- **Docker** and **Docker Compose** installed
- **Google Cloud Project** with BigQuery enabled
- **Service Account** with BigQuery permissions
- **Git** (to clone this repository)

## 🚀 Quick Start

### 1. Clone the Repository

```bash
git clone <your-repo-url>
cd Drilldown
```

### 2. Set Up Google Cloud Credentials

#### Option A: Use Existing Service Account

1. Download your service account JSON key file
2. Place it in `airflow/keys/gcp-sa.json`
3. Update the project ID in the file if needed

#### Option B: Create New Service Account

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Navigate to **IAM & Admin** → **Service Accounts**
3. Click **Create Service Account**
4. Grant these roles:
   - **BigQuery Data Editor**
   - **BigQuery Job User**
   - **BigQuery Data Viewer**
5. Create and download the JSON key
6. Place it in `airflow/keys/gcp-sa.json`

### 3. Update Configuration

Edit `dbt/profiles.yml` and update the project ID:

```yaml
drilldown_profile:
  target: core
  outputs:
    raw:
      project: YOUR_PROJECT_ID # ← Change this
      # ... rest of config
```

### 4. Run the Pipeline

```bash
# Start the entire pipeline
docker compose up -d

# Check if Airflow is running
docker compose ps

# View logs (optional)
docker compose logs -f airflow
```

### 5. Access Airflow UI

1. Open your browser and go to: `http://localhost:8080`
2. Login with:
   - **Username**: `admin`
   - **Password**: `admin`
3. Find the `dbt_end_to_end` DAG
4. Click the **play button** to trigger the pipeline

## 📊 Pipeline Steps

The pipeline runs these steps in sequence:

1. **`create_datasets`** - Creates BigQuery datasets
2. **`dbt_deps`** - Installs dbt dependencies
3. **`dbt_seed`** - Loads CSV data to `drilldown_raw`
4. **`dbt_run_core`** - Builds core models in `drilldown_core`
5. **`dbt_run_marts`** - Builds mart models in `drilldown_mart`
6. **`dbt_test_core`** - Tests core models
7. **`dbt_test_marts`** - Tests mart models
8. **`dq_gate`** - Data quality validation

## 🗂️ Data Structure

### Raw Data (CSV Files)

- `companies.csv` - Company information
- `contacts.csv` - Contact details
- `deals.csv` - Sales deals and stages
- `invoices.csv` - Invoice records
- `invoice_lines.csv` - Invoice line items
- `products.csv` - Product catalog

### Core Models

- **Staging**: `stg_deals`, `stg_invoices` - Cleaned raw data
- **Dimensions**: `dim_companies`, `dim_products` - Business entities
- **Fact**: `fact_sales` - Sales transactions

### Mart Models

- `customer_ltv` - Customer lifetime value
- `product_perf` - Product performance metrics
- `sales_rep_perf` - Sales representative performance
- `product_bundles` - Product bundle analysis
- `finance_dso` - Days sales outstanding
- `ai__customer_snapshot` - Customer analytics

## 🔧 Configuration Files

### Docker Compose (`docker-compose.yml`)

- Defines Airflow service with proper volumes and environment variables
- Maps local directories to container paths
- Sets up port forwarding (8080)

### dbt Configuration

- **`dbt_project.yml`** - Project settings and model configurations
- **`profiles.yml`** - BigQuery connection settings
- **`sources.yml`** - Raw data source definitions
- **`schema.yml`** - Model tests and documentation

### Airflow DAG (`airflow/dags/dbt_pipeline.py`)

- Orchestrates the entire pipeline
- Handles dependencies between tasks
- Includes data quality gate

## 🐛 Troubleshooting

### Common Issues

#### 1. "Connection Reset" Error

```bash
# Check if Airflow is fully started
docker compose logs airflow

# Wait for "Listening at: http://0.0.0.0:8080" message
# Then try accessing http://localhost:8080
```

#### 2. GCP Authentication Error

```bash
# Verify service account file exists
ls -la airflow/keys/gcp-sa.json

# Check file permissions
chmod 600 airflow/keys/gcp-sa.json
```

#### 3. BigQuery Dataset Not Found

```bash
# Check if datasets were created
# Look for "create_datasets" task logs in Airflow UI
```

#### 4. dbt Test Failures

```bash
# Check individual task logs in Airflow UI
# Common issues:
# - Missing dependencies
# - Data type mismatches
# - Schema configuration errors
```

### Useful Commands

```bash
# View container status
docker compose ps

# View logs
docker compose logs -f airflow

# Restart containers
docker compose restart

# Stop everything
docker compose down

# Rebuild and start
docker compose up --build -d

# Access container shell
docker compose exec airflow bash
```

## 📈 Monitoring

### Airflow UI

- **DAGs**: View pipeline status and history
- **Task Instances**: Check individual task logs
- **Graph View**: Visualize task dependencies
- **Logs**: Detailed execution logs

### BigQuery Console

- **Datasets**: `drilldown_raw`, `drilldown_core`, `drilldown_mart`
- **Tables**: View created models and data
- **Query Editor**: Run custom queries

## 🔄 Data Quality

The pipeline includes automated data quality checks:

- **Freshness**: Data should be less than 2 years old
- **Integrity**: No negative line amounts
- **Completeness**: Required fields are not null
- **Consistency**: Referential integrity between tables

## 🚀 Production Considerations

For production deployment:

1. **Environment Variables**: Use proper environment variable management
2. **Secrets Management**: Store credentials securely
3. **Monitoring**: Set up alerts for pipeline failures
4. **Scaling**: Consider using Kubernetes for larger workloads
5. **Security**: Implement proper IAM roles and network security

## 📚 Additional Resources

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [dbt Documentation](https://docs.getdbt.com/)
- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [Docker Compose Documentation](https://docs.docker.com/compose/)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test the pipeline
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

**Need Help?** Check the troubleshooting section or open an issue in the repository.
