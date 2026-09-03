# Change Data Feed for Shared Views

## Summary

This change extends the Delta Sharing change data feed (CDF) API to shared views. A client and
server negotiate support through the `cdfOnViews=true` capability. A successful view-CDF response
echoes that capability and omits all commit-version information. Both Python and Spark read the
returned files directly, using timestamps rather than versions to define the requested range.

The design preserves the existing table-CDF behavior. Table responses continue to carry a table
version, per-file versions, and the `_commit_version` result column. Snapshot reads are also
unchanged; the new response marker applies only to CDF requests for views.

## Motivation

Table CDF is organized around a stable sequence of table commits, so current clients expect a
version in the response header, on every file action, and in every output row. A view exposes the
result of a provider-side query instead of that underlying commit sequence. Assigning table-like
versions on the client would therefore create identifiers with no protocol meaning and would still
leave the client unable to reproduce the provider's view evaluation. View CDF needs an explicit
response type, timestamp-based bounds, and provider-resolved output files.

## Goals and non-goals

The goals are to:

- Support batch CDF reads from shared views in the Python and Spark connectors.
- Support both Parquet- and Delta-format response action envelopes.
- Give clients an explicit, reliable way to distinguish view CDF from table CDF.
- Require timestamp bounds for views and remove commit versions from their wire and result schemas.
- Keep the existing table-CDF paths backward compatible.

This change does not add streaming CDF for views, identify views during ordinary snapshot reads, or
introduce a general client-side model of provider materialization. It also does not ask clients to
evaluate a view definition or reproduce version-dependent Delta semantics.

## Protocol

Clients that support this feature advertise the following capability on CDF requests:

```text
delta-sharing-capabilities: cdfOnViews=true
```

Capability names and values are parsed case-insensitively. A server MUST reject a view-CDF request
from a client that does not advertise the capability. For a successful view-CDF request, the server
MUST echo `cdfOnViews=true` in the response. The server MUST set this response capability only for
CDF on a view; table CDF and all snapshot responses omit it.

The echoed capability is the authoritative object-type signal. Clients do not infer that an object
is a view merely because the `Delta-Table-Version` header is absent. The response contracts are:

| Property | Table CDF | View CDF |
| --- | --- | --- |
| Accepted bounds | Versions or timestamps | Timestamps only |
| `cdfOnViews=true` response capability | Absent | Required |
| `Delta-Table-Version` | Required | Absent |
| File-action `version` | Required | Absent |
| Result metadata columns | `_change_type`, `_commit_version`, `_commit_timestamp` | `_change_type`, `_commit_timestamp` |

View requests use the existing required `startingTimestamp` bound and may include
`endingTimestamp`; the server rejects `startingVersion` or `endingVersion`. The range remains
inclusive, matching the existing CDF API.

Every returned view-CDF file action is self-contained and directly readable with the logical schema
in the response metadata. Before returning files, the provider resolves any semantics that would
otherwise depend on table history, including deletion vectors and column mapping. This is required
because a view has no recipient-visible commit sequence for the client to replay.

Both response formats use the same semantic contract. A Parquet response contains the existing
protocol, metadata, and CDF file actions. A Delta-format response retains its protocol, metadata,
and `deltaSingleAction` wire wrappers, but those wrappers describe final files rather than a log to
reconstruct. In both formats, file actions have timestamps, omit versions on the wire, and map to
nullable version fields in the client model.

## Client design

### Shared response model

The JVM and Python REST clients parse the response capability into an `isCDFOnView` flag and make
file-action versions nullable. They validate the response before exposing it to a reader:

- A view response cannot also contain `Delta-Table-Version` or per-file versions.
- A table response must contain both the table version and per-file versions.
- A response with neither a table version nor `cdfOnViews=true` is invalid.
- Every CDF file action must still contain a timestamp.
- All pages of a paginated response must agree on format, metadata, version-header presence, and
  the view-CDF capability.

No missing version is synthesized. Consequently, the read layer omits `_commit_version` instead of
creating it with null values.

### Python

The Python REST client normalizes Parquet- and Delta-format view responses into the existing CDF
file-action model and marks the response as view CDF. The pandas reader then reads the resolved
files directly and constructs a result schema without `_commit_version`.

The existing Delta-format table-CDF implementation is unchanged: table actions still have versions
and may be assembled into a temporary local Delta log for processing. The direct-file branch is
selected only when the server explicitly returns `cdfOnViews=true`. Python APIs that return Spark
DataFrames use the Spark connector and therefore follow the Spark path below.

### Spark

Spark continues to create a `RemoteDeltaLog` as the entry point for a batch relation, but view CDF
does not inspect or replay a remote snapshot log. `RemoteDeltaCDFRelation` obtains the logical and
partition schemas from the CDF response, builds file indexes from its add, CDC, and remove actions,
and derives index sizes from those actions. This avoids a separate snapshot metadata request.

The file indexes add `_change_type` and `_commit_timestamp` as appropriate while leaving out
`_commit_version`. Existing table CDF retains the current snapshot- and version-based behavior.
Structured Streaming rejects a response marked as view CDF because a resumable stream requires
stable version offsets, which views do not expose.

## Compatibility and rollout

Capability negotiation makes the change safe to roll out independently. Servers can continue
serving table CDF to old and new clients. They expose view CDF only to clients that advertise
support, while clients preserve their old behavior unless the response explicitly identifies view
CDF. Providers must satisfy the complete response contract before echoing the capability.

The implementation is split into five stacked changes: shared JVM protocol/model infrastructure,
Python support, Spark support, end-to-end coverage, and public documentation.

## Testing

Unit tests cover capability construction and parsing, invalid header/action combinations,
timestamp-only validation, nullable action versions, and output schemas. Spark tests additionally
exercise add/CDC/remove file indexes, partition filtering, action-derived sizes, and verify that the
view-CDF path makes no snapshot metadata request.

End-to-end tests use the repository test server to cover Python and Spark against both Parquet- and
Delta-format view-CDF responses. Direct server assertions verify that the capability is emitted only
for view CDF, while table CDF and ordinary view metadata responses retain their existing headers.
