# es-enrichment-sg
Sand &amp; Gravel Enrichment - Python Lambdas.

##Wrangler
The enrichment wrangler is the start of the process. It first picks up the sng data from s3, then performs a small amount of column renaming. It invokes the method lambda with this data. The method response contains two dataframes(data and anomalies), which are split out in the wrangler. Data is sent on to the sqs queue whereas the anomalies are sent via an sns topic.

##Method
The enrichment method performs the joins and actually enriches the data. 2 lookups are used, 'responder_lookup' and 'county_lookup'. These lookups are joined onto the input data. 3 data anomaly checks are performed: marine_mismatch_check, missing_county_check, missing_region_check. Anomaly information is joined to the output dataframe before it is returned.