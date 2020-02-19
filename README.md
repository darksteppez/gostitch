# GOSTITCH
gostitch is a package written to send batch data payloads to [Stitch](https://www.stitchdata.com/) using the Stitch Import API. The package provides the structs needed for marshaling data to Stitch-compatible JSON payloads and sending. The package utilizes the net/http standard library for the API connection. The initial payload should be a JSON array of records to be inserted to Stitch.

The package is setup to use the /v2/import/patch POST endpoint provided by Stitch. It does not have single-record POST capabilities. As per Stitch's documentation this is the preferred method of sending data to take advantage of data type enforcement.

# USAGE

To use the package you will need to generate each component of the payload including:

`messages`: this will be a slice of at least one []SingleRecord that can be ranged over when creating the batch payload
`schema`: this will be a slice of `map[string]string` that make up your data structure schema, based on [JSON schema](https://json-schema.org/)
`keynames`: a slice of strings containing the fields that should be used as keys by Stitch
`tablename`: the name of the table that the data will be loaded to in Stitch

For a full example of the usage see `example/example.go`.