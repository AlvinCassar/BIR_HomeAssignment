-- BIR --- HOME ASSIGNMENT ---

-- FILES USED / EDITED ---
-- countries.csv	- not edited
-- locations.csv	- not edited
-- afsis.csv		- removed commas from fields + removed 1 duplicate row
-- catches.csv		- removed commas from fields + fixed broken rows
-- species.csv		- removed commas from fields

CREATE DATABASE birFishDB
USE birFishDB;
GO

-- CREATE CSV TABLES

CREATE SCHEMA csv;
GO

CREATE TABLE csv.asfis
(
	ISSCAAP NVARCHAR(MAX),
	TAXOCODE NVARCHAR(MAX),
	[3A_CODE] NVARCHAR(MAX),
	Scientific_name NVARCHAR(MAX),
	English_name NVARCHAR(MAX),
	French_name NVARCHAR(MAX),
	Spanish_name NVARCHAR(MAX),
	Arabic_name NVARCHAR(MAX),
	Chinese_name NVARCHAR(MAX),
	Russian_name NVARCHAR(MAX),
	Author NVARCHAR(MAX),
	Family NVARCHAR(MAX),
	[Order] NVARCHAR(MAX),
	Stats_data NVARCHAR(MAX)
);
GO

CREATE TABLE csv.species
(
	FAO_CODE NVARCHAR(MAX),
	LatinSpeciesName NVARCHAR(MAX),
	English NVARCHAR(MAX),
	Spanish NVARCHAR(MAX),
	French NVARCHAR(MAX)
);
GO

CREATE TABLE csv.locations
(
	tblCodeID NVARCHAR(MAX),
	Code NVARCHAR(MAX),
	[Description] NVARCHAR(MAX),
	tblCodeTypeID NVARCHAR(MAX),
	CodeType NVARCHAR(MAX),
	Created NVARCHAR(MAX),
	Modified NVARCHAR(MAX),
	Deprected NVARCHAR(MAX)
);
GO

CREATE TABLE csv.catches
(
	Species NVARCHAR(MAX),
	Area NVARCHAR(MAX),
	Units NVARCHAR(MAX),
	Country NVARCHAR(MAX),
	[2019] NVARCHAR(MAX),
	[2018] NVARCHAR(MAX),
	[2017] NVARCHAR(MAX),
	[2016] NVARCHAR(MAX),
	[2015] NVARCHAR(MAX),
	[2014] NVARCHAR(MAX),
	[2013] NVARCHAR(MAX),
	[2012] NVARCHAR(MAX),
	[2011] NVARCHAR(MAX),
	[2010] NVARCHAR(MAX),
	[2009] NVARCHAR(MAX),
	[2008] NVARCHAR(MAX),
	[2007] NVARCHAR(MAX),
	[2006] NVARCHAR(MAX)
);
GO

CREATE TABLE csv.countries
(
	Code NVARCHAR(MAX),
	Country NVARCHAR(MAX),
)
GO

-- BULK INSERT INTO CSV TABLES ----

--countries table
BULK INSERT CSV.countries
FROM 'D:\Downloads\Bir_home_Ass\Country.csv'
WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR='\n' );

--catches table
BULK INSERT CSV.catches
FROM 'D:\Downloads\Bir_home_Ass\Catches.csv'
WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR='\n' );

--asfis table
BULK INSERT CSV.asfis
FROM 'D:\Downloads\Bir_home_Ass\ASFIS.csv'
WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR='\n' );

--locations table
BULK INSERT CSV.locations
FROM 'D:\Downloads\Bir_home_Ass\Locations.csv'
WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR='\n' );

--species table
BULK INSERT CSV.species
FROM 'D:\Downloads\Bir_home_Ass\Species.csv'
WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR='\n' );
GO

-- SECTION B --
CREATE SCHEMA OLTP
GO

-- CREATE OLTP TABLES -----
CREATE TABLE OLTP.[Location]
(
    locationID UNIQUEIDENTIFIER
        CONSTRAINT lo_lid_pk PRIMARY KEY DEFAULT NEWID(),
    locationCode NVARCHAR(15) NOT NULL
        CONSTRAINT lo_lc_un UNIQUE,
    [location] NVARCHAR(200) NOT NULL
);
GO

CREATE TABLE OLTP.Species
(
    speciesID UNIQUEIDENTIFIER
        CONSTRAINT sp_spid_pk PRIMARY KEY DEFAULT NEWID(),
    speciesCode NVARCHAR(10) NOT NULL 
        CONSTRAINT sp_spc_un UNIQUE,
    species NVARCHAR(100) NOT NULL 
        CONSTRAINT sp_sp_un UNIQUE,
    speciesFamily NVARCHAR(25),
    speciesOrder NVARCHAR(35)
);
GO

CREATE TABLE OLTP.Country
(
    countryID UNIQUEIDENTIFIER
        CONSTRAINT co_cid_pk PRIMARY KEY DEFAULT NEWID(),
    countryCode NVARCHAR(10) NOT NULL
        CONSTRAINT co_cc_un UNIQUE,
    country NVARCHAR(100) NOT NULL
        CONSTRAINT co_co_un UNIQUE
);
GO

CREATE TABLE OLTP.[Catch]
(
    catchID UNIQUEIDENTIFIER
        CONSTRAINT ca_cid_pk PRIMARY KEY DEFAULT NEWID(),
    [year] NUMERIC(4) NOT NULL,
    [catch] NUMERIC(12,4) NOT NULL,
    speciesID UNIQUEIDENTIFIER NOT NULL 
        CONSTRAINT ca_spid_fk FOREIGN KEY REFERENCES OLTP.Species(speciesID),
    countryID UNIQUEIDENTIFIER NOT NULL
        CONSTRAINT ca_cid_fk FOREIGN KEY REFERENCES OLTP.Country(countryID),
    locationID UNIQUEIDENTIFIER NOT NULL
        CONSTRAINT ca_lid_fk FOREIGN KEY REFERENCES OLTP.Location(locationID)
);
GO

-- section c
-- INSERT INTO OLTP TABLES FROM CSV ---

insert into oltp.species
	(speciesCode, species, speciesFamily, speciesOrder)
select distinct s.FAO_CODE, ISNULL(s.ENGLISH, s.LatinSpeciesName), a.family, a.[order]
from csv.species s join csv.asfis a
	on s.fao_code = a.[3a_code]
except
select speciesCode, species, speciesFamily, speciesOrder
from oltp.species;
go

insert into oltp.country
	(countryCode, country)
select distinct Code, Country
from csv.countries
except
select countryCode, country
from oltp.country;
go

insert into oltp.[Location]
	(locationCode, [location])
select distinct code, [description]
from csv.locations
except
select locationCode, [location]
from oltp.[Location];
go

insert into oltp.[Catch]
	([year], [catch], speciesId, countryId, locationId)
select CAST(c.[year] as numeric(4,0)),
	CAST(c.catches as numeric(12,4)),
	s.speciesID, co.countryID, l.locationID
from csv.catches
unpivot (catches for [year] in 
	([2019], [2018], [2017], [2016], [2015], [2014], [2013], [2012], [2011], [2010], [2009], [2008], [2007], [2006])
) as c
	join oltp.species s
	on c.species = s.speciesCode
	join oltp.country co
	on c.country = co.countryCode
	join oltp.[Location] l
	on c.area = l.locationCode
where isnumeric(c.catches) = 1 AND isnumeric(c.[year]) = 1;
go

-- section d - creating olap schema and tables
CREATE SCHEMA OLAP; 
GO

CREATE TABLE OLAP.[Location]
(
    locationKey UNIQUEIDENTIFIER
        CONSTRAINT lo_loc_pk PRIMARY KEY DEFAULT NEWID(),
    locationCode NVARCHAR(15) NOT NULL
        CONSTRAINT lo_loc_un UNIQUE,
    [location] NVARCHAR(200) NOT NULL,
	locationId UNIQUEIDENTIFIER
		CONSTRAINT lo_oltp_id_un UNIQUE
);
GO

CREATE TABLE OLAP.Country
(
	countryKey UNIQUEIDENTIFIER
		CONSTRAINT co_con_pk PRIMARY KEY DEFAULT NEWID(),
	country NVARCHAR(100) NOT NULL
		CONSTRAINT co_nat_key_un UNIQUE,
	countryId UNIQUEIDENTIFIER
		CONSTRAINT co_oltp_id_un UNIQUE
);
GO

CREATE TABLE OLAP.Species
(
	speciesKey UNIQUEIDENTIFIER
		CONSTRAINT sp_spe_pk PRIMARY KEY DEFAULT NEWID(),
	[order] NVARCHAR(35),
	family NVARCHAR(25),
	species NVARCHAR(100) NOT NULL
		CONSTRAINT sp_nat_key_un UNIQUE,
	speciesId UNIQUEIDENTIFIER
		CONSTRAINT sp_oltp_id_un UNIQUE
);
GO

CREATE TABLE OLAP.[Date]
(
	dateKey UNIQUEIDENTIFIER
		CONSTRAINT da_dat_pk PRIMARY KEY DEFAULT NEWID(),
	decade NVARCHAR(5) NOT NULL,
	[year] NUMERIC(4) NOT NULL
		CONSTRAINT da_nat_key_un UNIQUE
);
GO

CREATE TABLE OLAP.[Catch]
(
	catchKey UNIQUEIDENTIFIER
		CONSTRAINT ca_cat_pk PRIMARY KEY DEFAULT NEWID(),
	[catch] NUMERIC(12,4) NOT NULL,
	dateKey UNIQUEIDENTIFIER NOT NULL
		CONSTRAINT ca_dakey_fk FOREIGN KEY REFERENCES OLAP.[Date](dateKey),
	countryKey UNIQUEIDENTIFIER NOT NULL
		CONSTRAINT ca_cokey_fk FOREIGN KEY REFERENCES OLAP.Country(countryKey),
	speciesKey UNIQUEIDENTIFIER NOT NULL
		CONSTRAINT ca_spkey_fk FOREIGN KEY REFERENCES OLAP.Species(speciesKey),
	locationKey UNIQUEIDENTIFIER NOT NULL
		CONSTRAINT ca_lokey_fk FOREIGN KEY REFERENCES OLAP.[Location](locationKey),
	catchId UNIQUEIDENTIFIER NOT NULL
		CONSTRAINT ca_oltp_id_un UNIQUE
);
GO

--FILL OLAP SCHEMA FROM OLTP TABLES 

insert into olap.[Location]
	(locationCode, [location], locationId)
select locationCode, [location], locationId
from oltp.[location];
go

insert into olap.Country
	(country, countryId)
select  Trim(country), countryId
from oltp.country;
go

insert into olap.Species
	([order], family, species, speciesId)
select speciesOrder, speciesFamily, species, speciesId
from oltp.species;
go

insert into olap.[Date]
	(decade, [year])
select distinct iif([year] < 2010, '2000s', '2010s') , [year]
from oltp.[catch] 
except 
select decade, [year]
from olap.[date];
go

insert into olap.[Catch]
	([catch], dateKey, countryKey, speciesKey, locationKey, catchId)
select distinct [catch], dateKey, countryKey, speciesKey, locationKey, catchId
from oltp.[catch] c join olap.[Date] d
	on c.[year] = d.[year]
	join olap.Country co
	on c.countryId = co.countryId
	join olap.Species s
	on c.speciesId = s.speciesId
	join olap.[Location] l
	on c.locationId = l.locationId
except
select [catch], dateKey, countryKey, speciesKey, locationKey, catchId
from olap.[catch];
go

--section E letter a
SELECT decade, sum([catch]) as 'Total Catch'
FROM olap.[catch] c join olap.[date] d
	on c.dateKey = d.dateKey
group by grouping sets (
	(decade),
	()
)

--section E  letter b
SELECT * FROM
(
    SELECT c.country, d.[year], ca.[catch]
    FROM olap.country c join olap.[catch] ca 
    on ca.countryKey = c.countryKey
    join olap.[date] d 
    on ca.dateKey = d.dateKey
    WHERE c.country LIKE 'Norway%' OR c.country LIKE 'Sweden%'
) cty 
PIVOT(
    sum([Catch])
    FOR [Year] IN (
        [2015], [2016], [2017], [2018], [2019]
    )
) as piv_cty

-- clean up
DROP TABLE IF EXISTS OLAP.[Catch]
DROP TABLE IF EXISTS OLAP.[Country]
DROP TABLE IF EXISTS OLAP.[Date]
DROP TABLE IF EXISTS OLAP.[Location]
DROP TABLE IF EXISTS OLAP.[Species]
DROP SCHEMA IF EXISTS OLAP;

DROP TABLE IF EXISTS OLTP.[Catch]
DROP TABLE IF EXISTS OLTP.[Country]
DROP TABLE IF EXISTS OLTP.[Location]
DROP TABLE IF EXISTS OLTP.[Species]
DROP SCHEMA IF EXISTS OLTP;

DROP TABLE IF EXISTS CSV.[catches]
DROP TABLE IF EXISTS CSV.[locations]
DROP TABLE IF EXISTS CSV.[asfis]
DROP TABLE IF EXISTS CSV.[species]
DROP TABLE IF EXISTS CSV.[countries]
DROP SCHEMA IF EXISTS CSV;