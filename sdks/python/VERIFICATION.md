export PROJECT=my-gcp-project
export STAGING=gs://my-gcp-bucket.appspot.com/tempbq
export BQ_DS=bqtest
 
time python -m apache_beam.io.gcp.bigquery_write_perf_test     --test-pipeline-options="
    --runner=TestDirectRunner --profile_cpu --profile_location=./aiprof
    --project=$PROJECT --experiments=use_beam_bq_sink
    --region=us-central1
    --staging_location=$STAGING
    --temp_location=$STAGING/temp
    --sdk_location=./dist/apache-beam-2.22.0.dev0.tar.gz  # OR WHICHEVER TARBALL
    --publish_to_big_query=false
    --output_dataset=$BQ_DS
    --output_table=bq_write_test_50m
    --input_options='{\"num_records\": 50057, \"key_size\": 10, \"value_size\": 1024}'"
