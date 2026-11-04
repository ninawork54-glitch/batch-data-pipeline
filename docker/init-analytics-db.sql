-- Metabase and the publish task read from this database, kept separate from the
-- Airflow metadata database so a pipeline reset never touches scheduler state.
CREATE DATABASE analytics;
