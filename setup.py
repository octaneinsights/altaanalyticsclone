from setuptools import find_packages, setup

setup(
    name="fieldroutes_pipeline",
    packages=find_packages(),
    install_requires=[
        "dagster",
        "pandas",
        "requests",
        "snowflake-connector-python",
        "pyyaml",
    ],
)