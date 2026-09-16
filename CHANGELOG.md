# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [v5.4.6] - 2026-09-16

### Added

- Support nested dotted paths in ApplyFieldMerge

## [v5.4.5] - 2026-07-10

### Added

- Introduced franz-go based Kafka client and consumer implementation

## [v5.4.4] - 2026-07-07

### Added

- Add dependency management and conditional evaluation for rule actions

## [v5.4.3] - 2026-05-28

### Added

- Added appendOnly param to bulkIngestRequest struct

## [v5.4.2] - 2026-04-28

### Added

- Add `cron_threshold` function for evaluating active cron schedules

## [v5.4.1] - 2026-03-24

### Fixed

- Refactored folderId to be a pointer in ExternalConfig retrieval logic

## [v5.4.0] - 2026-03-23

### Added

- Add folder_id to ExternalConfig retrieval queries in PostgresRepository

## [v5.3.9] - 2026-03-20

### Added

- Add folder management functionality to external config repository

## [v5.3.8] - 2026-03-09

### Added

- Add support for sorting in Elasticsearch search requests

## [v5.3.7] - 2026-02-16

### Added

- Add field enrichment functionality to router Zap logger

## [v5.3.6] - 2025-12-04

### Added

- Add `MinimumShouldMatch` to `BooleanFragment` and handle its serialization logic.

### Changed

- Enhanced merge config & expression language

## [v5.3.5] - 2025-11-27

### Changed

- Migrate to ElasticSearch V8 and added authentication

## [v5.3.4] - 2025-11-25

### Added

- Added filter & exclude functions to gval

## [v5.3.3] - 2025-10-27

### Added

- Added time-based index name generator for monthly/daily ranges
- Added rule validation and expression syntax checks

## [v5.3.2] - 2025-09-03

### Added

- Add gval function

## [v5.3.1] - 2025-07-04

### Changed

- Export Cache cleanup method

## [v5.3.0] - 2025-06-24

### Fixed

- Fix variable name in search request to use the correct input value

## [v5.2.8] - 2025-05-23

### Changed

- Set timezone for date histograms when applicable

## [v5.2.7] - 2025-05-23

### Added

- Added scope on variableconfigs + (tmp) refreshing id and choose id on creation

## [v5.2.6] - 2025-05-19

### Added

- Add `CheckPrevSetAction` flag for enhanced rule evaluation

## [v5.2.5] - 2025-04-29

### Changed

- Moved external config and variables config from engine to sdk

## [v5.2.4] / [v5.2.3] - 2025-04-28

### Changed

- Refactor parameter handling to support flexible value types

## [v5.2.2] - 2025-04-16

### Changed

- Limited model cron interval to more than once a day

## [v5.2.1] - 2025-04-15

### Added

- Support for datemillis() in range queries

## [v5.2.0] - 2025-03-27

### Added

- Added model name lowercase validation + updated deps

## [v5.1.9] - 2025-02-27

### Fixed

- Fixed ignored evaluation of long values in mapper jsoniter

## [v5.1.8] - 2025-01-29

### Security

- Bumped dependencies to fix security problems

## [v5.1.7] - 2025-01-03

### Added

- Added delete intent token

## [v5.1.6] - 2024-12-30

### Changed

- Integrated global variables in the gval language

## [v5.1.5] - 2024-11-26

### Added

- Added new sink manager & consumer panic recovery

### Changed

- Replace "host" key by "myrtea_host" in zap.log calls

## [v5.1.4] - 2024-11-20

### Added

- New avro transformer using hamba/avro

## [v5.1.1] - 2024-10-22

### Added

- Support monthly index on timebased rollmode
- Added abs function in gval

## [v5.1.0] - 2024-10-11

### Changed

- Mapper: uuid_from_long default value set in case of null value

## [v5.0.9] - 2024-09-30

### Changed

- Improved aggregate date_histogram since interval was deprecated

### Fixed

- Fixed panic error in kafka consumers

## [v5.0.8] - 2024-09-05

### Changed

- Merge config improvements

## [v5.0.7] - 2024-07-18

### Fixed

- Fix error log not visible at config initialization
- Fixed consumer channel message not closed

## [v5.0.6] - 2024-07-11

### Added

- Add zero divisor handling in safeDivide function

## [v5.0.5] - 2024-07-01

### Added

- New gval to convert number as string without exponent

## [v5.0.4] - 2024-06-14

### Changed

- Bump github.com/hashicorp/go-retryablehttp from 0.7.6 to 0.7.7

### Fixed

- Fixed redis config

## [v5.0.3] - 2024-06-12

### Removed

- Removed pointer for redis client singleton instance

## [v5.0.2] - 2024-06-12

### Added

- Added redis connection handler

## [v5.0.1] - 2024-06-03

### Changed

- Added timezone support to elastic query

## [v5.0.0] - 2024-05-31

### Changed

- Changed module version to v5.0.0

## [v4.6.2] - 2024-04-30

### Changed

- Switched default branch to main
- Improve get_formatted_duration gval func to support more input types
- Bumped go-elasticsearch library version to v8.13.1

### Removed

- Removed elasticv6, redis, bumped all dependencies

## [v4.6.0] - 2024-04-23

### Added

- Added func get_formatted_duration to gval
- Added Output variable to FieldMath merge evaluation

## [v4.5.11] - 2024-04-23

### Added

- Added safeDivide func

## [v4.5.10] - 2024-04-17

### Changed

- Update sarama lib to fix message consuming issue

## [v4.5.9] / [v4.5.8] - 2024-03-26

### Added

- Add default value for sink env variables

### Fixed

- Fixed log level already set to Info in Prod

## [v4.5.7] - 2024-03-13

### Changed

- Moved nestedLookMap functions to utils file and made it public

## [v4.5.6] - 2024-03-13

### Changed

- Optimised mapper structure & deprecated elasticv6

## [v4.5.5] - 2024-03-11

### Fixed

- Fixed go.mod deps

## [v4.5.4] - 2024-03-01

### Changed

- Bump jwx & jwtauth versions

### Fixed

- Fixed reloader/restarter routes

## [v4.5.3] - 2024-02-28

### Fixed

- Fixed go version as string in ci pipeline

## [v4.5.2] - 2024-02-27

### Changed

- Dependency bumps and test fixes

### Fixed

- Fixed tests & added pipeline

## [v4.5.1] - 2024-02-27

### Added

- Added connector config to helpers

## [v4.5.0] - 2024-02-27

### Added

- Added reloader & restarter endpoint for connector

## [v4.4.11] - 2024-02-15

### Changed

- Improved error handling in reloader

## [v4.4.10] - 2024-02-15

### Added

- Added id to reloader

## [v4.4.9] - 2024-02-15

### Added

- New reload endpoint for connectors

## [v4.4.8] - 2024-01-09

### Added

- Added wildcard and optional wildcard leaf conditions

## [v4.4.7] - 2023-11-27

### Added

- Added a function to round a number to a specific number of decimals

### Changed

- Set stricter type for document source

## [v4.4.6] - 2023-11-24

### Changed

- Improved perfs by replacing json/encoding with json-iter

## [v4.4.5] - 2023-10-31

### Changed

- Implemented basic structure for Regex leaf condition in the elastic query builder
- Bump gval version to improve performances

## [v4.4.4] - 2023-10-25

### Changed

- Improved ttlcache
- Created jsoniter mapper filter

## [v4.4.3] - 2023-10-18

### Removed

- Remove format on template

## [v4.4.2] / [v4.4.1] - 2023-10-11

### Changed

- Use FieldLeaf Format variable to set datetime format in ES templates

## [v4.4.0] - 2023-09-19

### Added

- Added format variable to model field

## [v4.3.10] - 2023-09-15

### Added

- Add new test case

### Fixed

- Fix engine.For case
- Fix go-elasticsearch version wrongly upgraded

### Removed

- Remove logs

## [v4.3.9] - 2023-09-04

### Added

- Added function get_value_current_day to gval

## [v4.3.8] - 2023-08-07

### Fixed

- Fixed a minor logger regression

## [v4.3.7] - 2023-08-07

### Fixed

- Fixed viper initialisation to support usage in parallel tests

## [v4.3.6] - 2023-08-01

### Added

- Added url decode/encode functions to gval
- Added format_date function to gval

### Changed

- Enhanced bulk index response struct

## [v4.3.4] / [v4.3.3] - 2023-06-06

### Added

- Added dependency management support between rule fields
- Add modele Rule two new field

### Changed

- Change name field and signification fieldDisabeDependsOn
- Refactored CI toolbox and upgraded to Go 1.20

## [v4.3.2] - 2023-04-21

### Added

- Support new rules configuration features

### Changed

- Reverted an earlier change

### Fixed

- Fix floating-point issue on large long used to build uuids

### Removed

- Remove float64 parsing when calculating uuid from bits

## [v4.3.1] - 2023-04-13

### Added

- Added Elasticsearch v8 client support for select fact

## [v4.3.0] - 2023-04-04

### Changed

- Go.sum cleaning

## [v4.2.14] - 2023-03-24

### Added

- Support elasticsearch v8 client
- Added description to fact

## [v4.2.13] - 2023-03-07

### Fixed

- Fixed minor issues in expressions
- Fixed shadowed variable in condition

## [v4.2.12] - 2023-03-02

### Changed

- Minor upgrades on slice and math gval functions

## [v4.2.11] - 2023-03-02

### Fixed

- Fix gval contains function

## [v4.2.10] - 2023-03-02

### Removed

- Remove obsolete logs...

## [v4.2.9] - 2023-03-02

### Added

- Support jsoniter mapper wildcard lookup + new slices gval expression

## [v4.2.8] - 2023-02-20

### Added

- Add control on DefaultMultiConsumer processors

## [v4.2.6] / [v4.2.5] - 2023-02-08

### Added

- Added a replace function to handle char arrays in gval

### Fixed

- Fix previous version

## [v4.2.4] - 2023-02-08

### Added

- Support new faster json mapper with jsoniter

## [v4.2.2] - 2023-02-06

### Changed

- Change time encoder to ISO8601
- Removed the FiltredJsonMessage message type

### Fixed

- Fixed environment variable name for sink ingester

## [v4.2.1] - 2023-01-23

### Added

- Support kafka header filters

## [v4.2.0] - 2023-01-19

### Changed

- Sarama refactoring

## [v4.1.11] - 2023-01-11

### Added

- Support simple router default configuration for most connector components

## [v4.1.10] - 2023-01-10

### Changed

- Migrate shared router component from engine-api

## [v4.1.9] - 2023-01-09

### Added

- Add logger production config

## [v4.1.8] - 2022-12-12

### Fixed

- Fixed invalid reference in connector transformer

## [v4.1.7] - 2022-12-09

### Fixed

- Fixed minor issues in mapper json avro

## [v4.1.6] - 2022-12-08

### Added

- Support more generic message in BatchSink

## [v4.1.5] - 2022-12-08

### Added

- Support more generic message in BatchSink

## [v4.1.4] - 2022-12-08

### Added

- Add forcefieldupdate mergeconfig

## [v4.1.3] - 2022-12-07

### Added

- Added new connector components

## [v4.1.2] - 2022-11-10

### Added

- Support input type interface{} in gval length() function

## [v4.1.1] - 2022-05-12

### Security

- Cleaned up go.mod and fixed a JWT vulnerability

## [v4.1.0] - 2022-05-11

### Fixed

- Fixed query builder handling of the For condition operator with a slice of elements

## [v4.0.0] - 2020-09-11

### Added

- Initial public release of Myrtea v4
- Added gval function flatten_fact to flatten facts with dimensions
- Added flatten map function
- Added DeleteNestedMap function
- Added gval function extract_from_date
- Added new fact fragment to support optional for operator
- Added gval function to truncate date

### Changed

- Changed the FlattenMap parameters to match all other functions
- Reverted an incompatible change on server package

### Fixed

- Fixed expression operator for map division by alphanumeric
- Fixed flattenFact gval function
- Fixed return type in NewSecuredServer and NewUnsecuredServer
- Fixed possible empty parameters in ES template leading to an invalid template
- Fixed case resolution, skip action if its resolution fails
