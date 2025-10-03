-- tsb.customer_data_bank definition

-- Drop table

-- DROP TABLE tsb.customer_data_bank;

CREATE TABLE tsb.customer_data_bank (
	customer_id serial4 NOT NULL,
	full_name varchar(100) NULL,
	date_of_birth date NULL,
	gender varchar(10) NULL,
	address text NULL,
	phone_number varchar(20) NULL,
	email_address varchar(100) NULL,
	national_id_or_passport_number varchar(50) NULL,
	occupation varchar(50) NULL,
	customer_type varchar(50) NULL,
	account_numbers varchar(100) NULL,
	age int4 NULL,
	created_by varchar(100) NULL,
	created_on timestamptz DEFAULT now() NULL,
	modified_by varchar(100) NULL,
	modified_on timestamptz NULL,
	status varchar(50) NULL,
	CONSTRAINT customer_data_bank_pkey PRIMARY KEY (customer_id)
);
