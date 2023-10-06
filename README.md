# JourneyToTripAdvisor

Clickstream loaded from external S3 into a GCP project using the *loading_to_bigquery.py* Python script.

Credentials and access files are handled sepparately for privacy and access control.

After database is constructed, analysis is performed localy connecting to the BigQuery database, and extracting only required data that can be handled with available comúting resources.
