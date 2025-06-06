# yt_Data_ETL_pipeline

DATA SET LINK: https://www.kaggle.com/datasets/datasnaek/youtube-new
 
YouTube Trending Data Analysis ETL Pipeline

Developed a scalable end-to-end ETL pipeline to process and analyze YouTube trending video data leveraging modern cloud and big data technologies. The solution involved:

Extract, Transform, Load (ETL): Utilized PySpark for efficient extraction, transformation, and loading of raw YouTube trending data.

Storage & Staging: Implemented Amazon S3 as a data lake for raw data ingestion and staging of transformed datasets in partitioned Parquet format.

Execution Environment: Deployed and executed ETL workflows on Amazon EC2 instances ensuring flexible, scalable compute resources.

Orchestration: Automated the entire data pipeline using Apache Airflow, enabling reliable and scheduled workflows.

Schema Management: Employed AWS Glue Data Catalog and Amazon Athena to manage metadata and perform schema validation on datasets.

Data Warehousing: Loaded transformed data into Amazon Redshift for fast, scalable querying and analytics on the Parquet-based data warehouse.

This project enabled comprehensive, region-wise and category-wise trend analysis of YouTube videos by combining cloud-native storage, big data processing, and automated orchestration tools, ensuring efficient data ingestion, transformation, and querying at scale.

