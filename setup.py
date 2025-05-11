from setuptools import find_packages, setup

setup(
    name="cam_on_dagster_dbt",
    version="0.1.0",
    packages=find_packages(include=["*", "assets.*"]),
    package_data={
        "": ["dbt-project/**/*"],  # Include packaged dbt assets
    },
    install_requires=[
        "dagster==1.10.14",
        "dagster-webserver==1.10.14",
        "dagster-dbt==0.26.14",
        "dagster-dlt==0.26.14",
        "dbt-core==1.9.4",
        "dbt-duckdb==1.9.3",
        "dlt==1.10.0",
        "duckdb==1.2.2",
        "pandas==2.2.3",
        "numpy==2.2.5",
        "pyarrow>=10.0.0",
        "sqlalchemy>=2.0.40",
        "python-dotenv==1.1.0",
        "psycopg2-binary==2.9.10",
        "gspread==6.2.0",
        "google-api-core==2.24.2",
        "google-api-python-client==2.169.0",
        "google-auth==2.40.0",
    ],
    extras_require={
        "dev": ["pytest", "ipython"],
    },
)
