# Change Data Feed for Shared Views

## Summary

This change extends the Delta Sharing CDF API to shared views and other versionless CDF sources.
Clients and servers negotiate support with `versionlessCDF=true`. A versionless response has no
recipient-visible commit sequence, so clients use timestamp bounds and return `_change_type` and
`_commit_timestamp` without `_commit_version`.

The response is snapshot-shaped in both Parquet and Delta formats: Protocol, Metadata, and regular
AddFile actions. The response Metadata describes the physical returned files, which already contain
the CDF columns. AddFile wrapper versions and timestamps describe the provider's materialized table;
they are not source CDF metadata.

Versioned table CDF, snapshot reads, URL refresh, and streaming retain their existing behavior.

## Protocol contract

A batch client advertises:

```text
delta-sharing-capabilities: versionlessCDF=true
```

For a versionless CDF response, the server:

- echoes `versionlessCDF=true`;
- omits `Delta-Table-Version`;
- accepts `startingTimestamp` and `endingTimestamp`, not version bounds;
- returns Protocol and Metadata followed only by AddFile actions;
- includes physical `_change_type` and `_commit_timestamp` fields in the response schema and files;
- omits physical `_commit_version`.

The AddFile wrapper may have `version` and `timestamp` fields. Clients retain these fields for the
normal AddFile model and URL refresh, but must not expose them as CDF commit columns.

The response capability is authoritative. Clients do not infer a versionless response only from a
missing table-version header. A versioned table response continues to require its existing header
and versioned CDF actions.

## Client scan paths

### Python

For a Parquet response, Python reads the AddFiles with its Arrow path using the response schema. It
disables the legacy CDF synthesis that adds change type, version, and timestamp from file-action
metadata.

For a Delta response, Python writes the returned Protocol, Metadata, and Add actions as synthetic
version zero and uses Delta Kernel's regular `ScanBuilder`. It does not use
`TableChangesScanBuilder`, since the response is a complete materialized snapshot rather than a
sequence of source commits.

Versioned Delta CDF continues to use `TableChangesScanBuilder` and versioned Parquet CDF continues
to synthesize its CDF columns from the CDF actions.

### Spark

Spark does not use Delta Kernel for Delta-format responses. For versionless CDF it builds a
`RemoteDeltaBatchFileIndex` from `DeltaTableFiles.files` and scans it through `HadoopFsRelation`
using the response schema. The same regular scan path works for Parquet- and Delta-format
versionless responses.

The relation registers the returned IDs and URLs with the existing URL cache. A later URL refresh
still invokes `getCDFFiles`; only the already-fetched initial response may be prefetched.

DBR can pass its first versionless Delta response and existing client into package-scoped OSS
factories. The existing `RemoteDeltaCDFRelation` constructor remains unchanged, including its case
class product arity, and continues to fetch normally when no response is supplied.

## Compatibility

Old servers ignore an unknown request capability and continue serving versioned table CDF. New
servers expose versionless CDF only when the client advertises support. Structured Streaming does
not advertise the capability because it requires stable version offsets.

No Delta Kernel change is required: versionless Delta CDF uses the existing snapshot
`ScanBuilder`, while versioned CDF retains the existing table-changes builder.

## Verification

Unit coverage checks native AddFile parsing with unrelated wrapper metadata, physical CDF output
columns, regular Kernel snapshot scans in Python, regular Spark file-index scans, and both normal
and prefetched Spark relation construction. End-to-end fixtures exercise Python and Spark with
Parquet- and Delta-format versionless responses while retaining a versioned-table control.
