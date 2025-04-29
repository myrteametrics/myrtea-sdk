package tests

const (
	// ExternalGenericConfigDropTableV1 SQL statement to drop table external_generic_config_v1
	ExternalGenericConfigDropTableV1 string = `DROP TABLE IF EXISTS external_generic_config_v1`
	// ExternalGenericConfigV1 SQL statement to create table elasticsearch_indices_v1
	ExternalGenericConfigV1 string = `create table if not exists external_generic_config_v1 (
		id   serial primary key,
	    name varchar(100) not null,
		data jsonb        not null
	);`
)
