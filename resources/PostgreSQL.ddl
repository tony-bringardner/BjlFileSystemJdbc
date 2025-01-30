create schema file_source;

DROP TABLE file_source.file ;

CREATE TABLE file_source.file (
	FILEID BIGINT  UNIQUE generated always as IDENTITY PRIMARY KEY,
	PARENTID BIGINT DEFAULT 0 ,
	NAME VARCHAR(250) NOT NULL ,
	OWNER VARCHAR(10) NOT NULL ,
	GROUP_NAME VARCHAR(10) DEFAULT 'staff',
	CREATE_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	LAST_ACCESS_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	last_modified_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

	OWNER_READABLE BOOLEAN DEFAULT true,
	OWNER_WRITEABLE BOOLEAN DEFAULT true,
	OWNER_EXECUTABLE BOOLEAN DEFAULT true,

	GROUP_READABLE BOOLEAN DEFAULT true,
	GROUP_WRITEABLE BOOLEAN DEFAULT false,
	GROUP_EXECUTABLE BOOLEAN DEFAULT false,

	OTHER_READABLE BOOLEAN DEFAULT true,
	OTHER_WRITEABLE BOOLEAN DEFAULT false,
	OTHER_EXECUTABLE BOOLEAN DEFAULT false,
	FILE_TYPE VARCHAR(10) DEFAULT 'Undefined',
	LENGTH BIGINT DEFAULT 0 ,
	CHUNK_COUNT INT DEFAULT 0 ,
	UNIQUE  (PARENTID,NAME)
	) 
	;

DROP TABLE file_source.file_data;

CREATE TABLE file_source.file_data (
	CHUNK_NUMBER BIGINT NOT NULL,
	FILEID BIGINT NOT NULL  ,
	LENGTH INT NOT NULL  ,
	DATA BYTEA,
	primary key (chunk_number,fileid)
	)
	;
	