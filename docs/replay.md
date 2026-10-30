# Recollecting interfaces

For operations, it can be useful to recollect some interfaces for a delimited period of
time, so some consolidations and compute can be executed again.

## Replay options

Collectors support these common mandatory options for replay:

```
  --replay-interface-name REPLAY_INTERFACE_NAME [REPLAY_INTERFACE_NAME ...]
                        Enable Collect Replay on these interfaces (default: )
  --replay-start-date REPLAY_START_DATE
                        Start datetime of the replay in ZULU format (default: None)
  --replay-end-date REPLAY_END_DATE
                        End datetime of the replay in ZULU format (default: None
```

> In 2.5.0 version, replay is only available for OData collector. Other implementations will be added to next releases.

## Usage

Typical command line usage, considering common options are already set by environment variables:

```
python -m maas_collector.rawdata.cli.odata -v --replay-interface-name PRIP_S3A_ACRI PRIP_S5P_DLR --replay-start-date 2022-08-15T00:00:00Z --replay-end-date 2022-08-15T00:05:00Z
```

This will collect interfaces PRIP_S3A_ACRI and PRIP_S5P_DLR between 2022-08-15T00:00:00Z and 2022-08-15T00:05:00Z.

A replay collects exactly `--replay-start-date`..`--replay-end-date`:
`odata_start_offset` and `end_date_time_offset` apply to live collection only.

Note using interface replay automatically:

- sets the loop period to 0 (one shot)
- forces database updates and consequently messages on the bus

## Jira interfaces

A jira interface is replayed with one attempt (no retry). Its `jql_query` must contain
`updated > {date_criteria}`: a replay substitutes the window, so that the query becomes
`updated > "<start>" AND updated <= "<end>"` (a resumed replay starts at the last date
of its journal).

- JQL dates have a minute precision: the bounds are truncated to the minute, so
  `--replay-start-date 2026-10-01T10:00:59Z` becomes `"2026-10-01 10:00"`. Give whole
  minutes to collect exactly the window.
- Jira reads a JQL date in the timezone of the account the interface connects with,
  not in UTC: the window is shifted by the offset of that account. Give the API
  account the UTC timezone in its Jira profile to collect the window asked.

## Exit status

The replay exits with status 0 when every interface was replayed, and with status 1
when at least one interface exhausted its retries (`Failed replay for ...` is logged)
or when the replay is refused before collecting anything: an interface disabled,
unknown, or a jira one without `end_point` (the error is logged).

It exits with status 74 when files could not be backed up: a file whose backup did
not end with an `OK` report for every destination (the "Cannot backup" path
included) is counted, the replay collects everything it can, then exits and prints,
as its last line, `backup failed: <count> files to <backup name>` (the names of the
backups, sorted, when there are several; `(inline)` for a backup block that has no
name). Status 1 wins when both happened, and the line is printed anyway. A live
collector is not concerned: it logs the failed backups and goes on.

## Error messages

### Unknown interface

When making a typo on an interface name, the list of available interface names is displayed in such message:

```
ValueError: Interface PRIP_S3A_AC does not exists. Available interfaces: ['AUXIP_Exprivia', 'DD_DHUS', 'DD_DHUS_S5P', 'LTA_Acri', 'LTA_CloudFerro', 'LTA_Exprivia_S1', 'LTA_Exprivia_S2', 'LTA_Exprivia_S3', 'LTA_S5P_DLR', 'LTA_Werum', 'PRIP_S1A_Serco', 'PRIP_S2A_ATOS', 'PRIP_S2B_CAPGEMINI', 'PRIP_S3A_ACRI', 'PRIP_S3B_SERCO', 'PRIP_S5P_DLR', 'Satellite-Unavailability']
```

### Bad dates

Misspelling dates will display messages like:

```
ValueError: month must be in 1..12
```

### Inconsistent dates

If end date is lower than start date, this message will appear:

```

ValueError: Replay arguments: end date is lower than start date
```
