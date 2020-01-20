# es-enrichment-sg
Enrichment - Python Lambdas.

## Wrangler
The enrichment wrangler is the start of the process. It first picks up the sng data from s3. It invokes the method lambda with this data. The method response contains two dataframes(data and anomalies), which are split out in the wrangler. Data is sent on to the sqs queue whereas the anomalies are sent via an sns topic.

## Method
The method is generic. As well as the data, it receives information about lookups to use and survey specific parameters.
example:
```
"RuntimeVariables": {
    "data":{ ...},
    "lookups":{
      "0": {
        "file_name": "responder_county_lookup_prod.json",
        "columns_to_keep": [
          "responder_id",
          "county"
        ],
        "join_column": "responder_id",
        "required": [
          "county"
        ]
      },
      "1": {
        "file_name": "county_lookup_county.json",
        "columns_to_keep": [
          "county_name",
          "region",
          "county",
          "marine"
        ],
        "join_column": "county",
        "required": [
          "region",
          "marine"
        ]
      }
    },
    "marine_mismatch_check": "true",
    "period_column": "period",
    "identifier_column": "responder_id"
}
```
#### Lookups
The 'file_name' dictates which file to get from s3.<br> 
The 'columns_to_keep' represents the columns from the lookup to join on.<br> 
The 'join_column' is the column to use to join onto the data.<br>
The 'required' columns are used later in integrity tests, checking that no nulls exist in any required columns.<br><br>
#### Parameters
Parameters are taken from environment variables in the wrangler, packaged and sent over to the method.
marine_mismatch_check - determines whether to run the marine mismatch check or not.

### Integrity Checks
There are two integrity checks in the method.<br>
#### Missing column detector
Using a list of required columns that are constructed from the lookups section of the input. The missing column detector filters the original dataset to see any instances where required columns are null for a reference. It outputs a list of references with missing data for columns.
#### Marine Mismatch Detector
Detects references that are producing marine but from a county that doesnt produce marine by checking the 'land_or_marine' column against a specified column(marine) to confirm that if M, the marine column is y.<br><br>
Marine mismatch detector is only suitable for sand and gravel. So far that is the only survey that differentiates between land and marine, so is the only survey that would benefit from this check. 
