--Creating and use a new Database WorldFoodProduction
CREATE DATABASE WorldFoodProduction
GO

USE WorldFoodProduction

--Import CO2 Data From Csv File (Problems Encountered : The import fails when Population is the last column 
                              -- Solution: Import All tables then delete extra ones)
                
Alter TABLE CO2_Data
Drop Column [Column 0]

SELECT * FROM CO2_Data

--Import Maize Production Table,Maize Yeilds,Rice Production, Rice Yields , Surface tempreature anomoly , Wheat Production,Wheat Yields

SELECT * FROM Maize_prod
SELECT * FROM Maize_yields
SELECT * FROM Rice_prod
SELECT * FROM Rice_yields
SELECT * FROM Surface_Temp_Anom
SELECT * FROM Wheat_prod
SELECT * FROM Wheat_yields

-- All tables are uploaded
                             

---	%%%Create a view of all the distinct countries that exist in all tables.%%%

CREATE VIEW vw_Countries

AS 

SELECT DISTINCT D.country FROM CO2_Data D

INTERSECT

SELECT DISTINCT Country FROM Maize_Prod 

INTERSECT 

SELECT DISTINCT Country FROM Maize_Yields

INTERSECT 

SELECT DISTINCT Country FROM Rice_Prod

INTERSECT 

SELECT DISTINCT Country FROM Rice_Yields 

INTERSECT 

SELECT DISTINCT Country FROM Surface_Temp_Anom

INTERSECT 

SELECT DISTINCT Country FROM Wheat_Prod

INTERSECT 

SELECT DISTINCT Country FROM Wheat_Yields


-- END of View Query



SELECT * FROM vw_Countries

DELETE FROM CO2_Data WHERE country NOT IN (SELECT Country FROM vw_Countries) --ERROR : The query processor ran out of internal resources and could not produce a query plan. This is a rare event and only expected for extremely complex queries or queries that reference a very large number of tables or partitions. Please simplify the query. If you believe you have received this message in error, contact Customer Support Services for more information.


GO 

--Problem Solve 

USE MASTER 
GO 
ALTER DATABASE WorldFoodProduction SET Compatibility_LEVEL = 120 
GO 
USE WorldFoodProduction
GO

DELETE FROM CO2_Data WHERE country NOT IN (SELECT Country FROM vw_Countries) -- Success 

DELETE FROM Maize_Prod WHERE Country NOT IN (SELECT Country FROM vw_Countries)

DELETE FROM Maize_Yields WHERE country NOT IN (SELECT Country FROM vw_Countries)

DELETE FROM Rice_Prod WHERE Country NOT IN (SELECT Country FROM vw_Countries)

DELETE FROM Rice_Yields WHERE country NOT IN (SELECT Country FROM vw_Countries)

DELETE FROM Surface_Temp_Anom WHERE country NOT IN (SELECT Country FROM vw_Countries)

DELETE FROM Wheat_Prod WHERE country NOT IN (SELECT Country FROM vw_Countries)

DELETE FROM Wheat_Yields Where Country NOT IN (SELECT Country FROM vw_Countries)


--Removing Duplicacy and Incosistency

-----------------------------------------------------------CO2_Data----------------------------------------------------------------------------------------
SELECT DISTINCT * FROM CO2_Data

SELECT Iso_Code,Country,Year,COUNT(Co2) [Number Of Occurance] FROM CO2_Data
GROUP BY Iso_Code,Country,[Year]
HAVING COUNT(*) > 1  --Duplicacy Test

SELECT Iso_Code,Country,Year,AVG(Co2) FROM CO2_Data
GROUP BY Iso_Code,Country,[Year]
HAVING [year] NOT LIKE '[1-2][0-9][0-9][0-9]' OR [iso_code] NOT LIKE '___'   --Consistency Test

UPDATE CO2_Data
SET iso_code =  REPLACE(Iso_Code,'OWID_','')
WHERE iso_code LIKE 'OWID%' --Remove OWID From Code Column

--Converting Data To suitable Datatype

ALTER TABLE CO2_Data ALTER COLUMN YEAR SMALLINT
ALTER TABLE CO2_Data ALTER COLUMN CO2 INT    -- ERROR : Conversion failed when converting the varchar value '0.02' to data type int.
ALTER TABLE Co2_Data ALTER COLUMN Population INT

--Delete Incosistent Data

DELETE FROM CO2_Data WHERE iso_code LIKE '' OR country LIKE '' OR  YEAR LIKE '' OR CO2 LIKE '' OR Country LIKE '"%' OR [Population] LIKE ''


ALTER TABLE CO2_Data ALTER COLUMN CO2 DECIMAL(8,2)






-------------------------------------------------------------Cleaning Maize_Prod ---------------------------------------------------------------

SELECT Code,Country,[Year],COUNT(Production_Tones) [Number Of Occurance] FROM Maize_prod
GROUP BY Code,Country,[Year]
HAVING COUNT(*) > 1  --Duplicacy Test 

SELECT Code,Country,Year,AVG(Production_Tones) FROM Maize_prod
GROUP BY Code,Country,[Year]
HAVING [year] NOT LIKE '[1-2][0-9][0-9][0-9]' OR [code] NOT LIKE '___' OR  Code LIKE '' OR 
country LIKE '' OR  [YEAR] LIKE '' OR Country LIKE '"%' --Consistency Test

UPDATE Maize_prod
SET code =  REPLACE(Code,'OWID_','')
WHERE code LIKE 'OWID%' --Remove OWID From Code Column


ALTER TABLE Maize_prod alter Column Production_Tones INT
ALTER TABLE Maize_prod alter Column Year SMALLINT

DELETE FROM Maize_prod WHERE Code LIKE '' OR country LIKE '' OR  YEAR LIKE '' OR Production_Tones LIKE '' OR Country LIKE '"%' OR  [year] NOT LIKE '[1-2][0-9][0-9][0-9]' OR [code] NOT LIKE '___'

-----------------------------------------------------------Cleaning Maize_yields---------------------------------------------------------------------

SELECT * FROM Maize_Yields

SELECT Code,Country,[Year],COUNT(Yield_Hectogram_Per_Hectare) [Number Of Occurance] FROM Maize_Yields
GROUP BY Code,Country,[Year]
HAVING COUNT(*) > 1  --Duplicacy Test 

WITH Maize_Yield_Cte AS 
( 
        SELECT *,ROW_Number() OVER (Partition BY Code,Year Order BY Code) AS RowNumber 
		FROM Maize_Yields
)
DELETE FROM Maize_Yield_Cte WHERE RowNumber > 1 --Deleting Duplicate Rows



SELECT Code,Country,Year,AVG(Yield_Hectogram_Per_Hectare) FROM Maize_Yields
GROUP BY Code,Country,[Year]
HAVING [year] NOT LIKE '[1-2][0-9][0-9][0-9]' OR [code] NOT LIKE '___' OR  Code LIKE '' OR 
country LIKE '' OR  [YEAR] LIKE '' OR Country LIKE '"%' --Consistency Test

UPDATE Maize_Yields
SET code =  REPLACE(Code,'OWID_','')
WHERE code LIKE 'OWID%' --Remove OWID From Code Column


DELETE FROM Maize_yields WHERE Code LIKE '' OR country LIKE '' OR  YEAR LIKE '' OR Yield_Hectogram_Per_Hectare LIKE '' OR Country LIKE '"%'

SELECT * FROM Maize_Yields


SELECT * FROM Maize_Yields
WHERE Yield_Hectogram_Per_Hectare LIKE ''

ALTER TABLE Maize_yields ALTER COLUMN YEAR SMALLINT
ALTER TABLE Maize_yields ALTER COLUMN Yield_hectogram_per_hectare INT


---------------------------------------------------------Cleaning Rice_prod------------------------------------------------------------------------------------

SELECT * FROM Rice_prod

SELECT Code,Country,[Year],COUNT(Production_Tones) [Number Of Occurance] FROM Rice_prod
GROUP BY Code,Country,[Year]
HAVING COUNT(*) > 1  --Duplicacy Test 

SELECT Code,Country,Year,AVG(Production_Tones) FROM Rice_prod
GROUP BY Code,Country,[Year]
HAVING [year] NOT LIKE '[1-2][0-9][0-9][0-9]' OR [code] NOT LIKE '___' OR  Code LIKE '' OR 
country LIKE '' OR  [YEAR] LIKE '' OR Country LIKE '"%' --Consistency Test

UPDATE Rice_prod
SET code =  REPLACE(Code,'OWID_','')
WHERE code LIKE 'OWID%' --Remove OWID From Code Column


ALTER TABLE Rice_prod ALTER COLUMN YEAR SMALLINT 
ALTER TABLE Rice_prod ALTER COLUMN Production_Tones BIGINT

DELETE FROM Rice_Prod WHERE Code LIKE '' OR country LIKE '' OR  YEAR LIKE '' OR Production_Tones LIKE '' OR Country LIKE '"%'

------------------------------------------------------Cleaning Rice_Yields-----------------------------------------------------------------------------------------

SELECT * FROM Rice_Yields

SELECT Code,Country,[Year],COUNT(Yield_Hectogram_Per_Hectare) [Number Of Occurance] FROM Rice_Yields
GROUP BY Code,Country,[Year]
HAVING COUNT(*) > 1  --Duplicacy Test 

SELECT Code,Country,Year,AVG(Yield_Hectogram_Per_Hectare) FROM Rice_Yields
GROUP BY Code,Country,[Year]
HAVING [year] NOT LIKE '[1-2][0-9][0-9][0-9]' OR [code] NOT LIKE '___' OR  Code LIKE '' OR 
country LIKE '' OR  [YEAR] LIKE '' OR Country LIKE '"%' --Consistency Test

UPDATE Rice_Yields
SET code =  REPLACE(Code,'OWID_','')
WHERE code LIKE 'OWID%' --Remove OWID From Code Column



ALTER TABLE Rice_Yields ALTER COLUMN Year SMALLINT
ALTER TABLE Rice_yields ALTER COLUMN Yield_Hectogram_Per_Hectare INT

SELECT * FROM Rice_Yields
WHERE Yield_Hectogram_Per_Hectare LIKE '1992,13973'

DELETE FROM Rice_Yields WHERE Code LIKE '' OR country LIKE '' OR  YEAR LIKE '' OR Yield_Hectogram_Per_Hectare LIKE '' OR Country LIKE '"%'

-------------------------------------------------------Cleaning Surface Tempreature anomoly--------------------------------------------------------------------------

SELECT * FROM Surface_Temp_Anom 

SELECT Code,Country,[Year],COUNT(Surface_temp_anomaly) [Number Of Occurance] FROM Surface_Temp_Anom
GROUP BY Code,Country,[Year]
HAVING COUNT(*) > 1  --Duplicacy Test 

SELECT Code,Country,Year,AVG(Surface_temp_anomaly) FROM Surface_Temp_Anom
GROUP BY Code,Country,[Year]
HAVING [year] NOT LIKE '[1-2][0-9][0-9][0-9]' OR [code] NOT LIKE '___' OR  Code LIKE '' OR 
country LIKE '' OR  [YEAR] LIKE '' OR Country LIKE '"%' --Consistency Test

UPDATE Surface_Temp_Anom
SET code =  REPLACE(Code,'OWID_','')
WHERE code LIKE 'OWID%' --Remove OWID From Code Column


ALTER TABLE Surface_Temp_anom ALTER COLUMN YEAR SMALLINT
ALTER TABLE Surface_Temp_anom ALTER COLUMN Surface_temp_anomaly DECIMAL(8,2)

DELETE FROM Surface_Temp_anom WHERE Code LIKE '' OR country LIKE '' OR  YEAR LIKE '' OR Surface_temp_anomaly LIKE '' OR Country LIKE '"%'

------------------------------------------------------- WHEAT_PROD -------------------------------------------------------------------------------------------------

SELECT * FROM [dbo].[Wheat_Prod]

SELECT Code,Country,[Year],COUNT(Production_tones) [Number Of Occurance] FROM WHEAT_PROD
GROUP BY Code,Country,[Year]
HAVING COUNT(*) > 1  --Duplicacy Test 

SELECT Code,Country,Year,AVG(Production_tones) FROM WHEAT_PROD
GROUP BY Code,Country,[Year]
HAVING [year] NOT LIKE '[1-2][0-9][0-9][0-9]' OR [code] NOT LIKE '___' OR  Code LIKE '' OR 
country LIKE '' OR  [YEAR] LIKE '' OR Country LIKE '"%' --Consistency Test

UPDATE WHEAT_PROD
SET code =  REPLACE(Code,'OWID_','')
WHERE code LIKE 'OWID%' --Remove OWID From Code Column


DELETE FROM WHEAT_PROD WHERE Code LIKE '' OR country LIKE '' OR  YEAR LIKE '' OR Production_tones LIKE '' OR Country LIKE '"%'

ALTER TABLE Wheat_prod ALTER COLUMN YEAR SMALLINT
ALTER TABLE Wheat_prod ALTER COLUMN Production_tones BIGINT 



-------------------------------------------------------WHEAT_Yields -------------------------------------------------------------------------------------------------------

SELECT * FROM Wheat_Yields 

SELECT Code,Country,[Year],COUNT(Yield_hectogram_Per_Hectare) [Number Of Occurance] FROM WHEAT_Yields
GROUP BY Code,Country,[Year]
HAVING COUNT(*) > 1  --Duplicacy Test 

SELECT Code,Country,Year,AVG(Yield_hectogram_Per_Hectare) FROM WHEAT_Yields
GROUP BY Code,Country,[Year]
HAVING [year] NOT LIKE '[1-2][0-9][0-9][0-9]' OR [code] NOT LIKE '___' OR  Code LIKE '' OR 
country LIKE '' OR  [YEAR] LIKE '' OR Country LIKE '"%' --Consistency Test

UPDATE WHEAT_Yields
SET code =  REPLACE(Code,'OWID_','')
WHERE code LIKE 'OWID%' --Remove OWID From Code Column


DELETE FROM WHEAT_Yields WHERE Code LIKE '' OR country LIKE '' OR  YEAR LIKE '' OR Yield_hectogram_Per_Hectare LIKE '' OR Country LIKE '"%'

ALTER TABLE Wheat_Yields ALTER COLUMN YEAR SMALLINT
ALTER TABLE Wheat_Yields ALTER COLUMN Yield_hectogram_Per_Hectare INT


--Data Range Cleaning 


-------------------------------------------------------------------CO2_Data----------------------------------------------------------------------------
SELECT DISTINCT country FROM CO2_Data

WITH Co2_Cte
AS
(
SELECT MIN(Year) [MYear] ,ROW_NUMBER() OVER (PARTITION BY MIN(Year) ORDER BY MIN(Year) DESC) AS [Same Range Occurance]  FROM Co2_data
GROUP BY Country
)

SELECT * FROM Co2_Cte
WHERE [Same Range Occurance] > 1

SELECT DISTINCT  MIN(Year) FROM CO2_Data
GROUP BY country
ORDER BY MIN(Year) DESC --MIN =  1993

SELECT DISTINCT MAX(Year) FROM CO2_Data
GROUP BY country
ORDER BY MAX(Year) ASC -- MAX = 2017


DELETE FROM CO2_Data WHERE Year < 1993 OR Year > 2017 

DELETE FROM CO2_Data WHERE Country IN (SELECT DISTINCT Country FROM CO2_Data
                                         GROUP BY country
                                         HAVING MIN(Year) != 1993)





------------------------------------------------------------------Maize_Prod------------------------------------------------------------------------------
WITH MINRange_Cte
AS
(
SELECT Country, MIN(Year) [MYear] ,ROW_NUMBER() OVER (PARTITION BY MIN(Year) ORDER BY MIN(Year) DESC) AS [Same Range Occurance]  FROM Maize_Prod
GROUP BY Country
)

SELECT * FROM MINRange_Cte
WHERE MYear > 1993

GO

WITH MAXRange_Cte
AS
(
SELECT Country, MAX(Year) [MYear] ,ROW_NUMBER() OVER (PARTITION BY MAX(Year) ORDER BY MAX(Year) DESC) AS [Same Range Occurance]  FROM Maize_Prod
GROUP BY Country
)

SELECT * FROM MAXRange_Cte
WHERE MYear < 2017
GO



SELECT DISTINCT MIN(Year) FROM Maize_Prod
GROUP BY country
ORDER BY MIN(Year) DESC --MIN =  1993

SELECT DISTINCT MAX(Year) FROM Maize_Prod
GROUP BY country
ORDER BY MAX(Year) ASC -- MAX = 2017

DELETE FROM Maize_Prod WHERE Country IN (SELECT DISTINCT Country FROM Maize_Prod
                                         GROUP BY country
                                         HAVING MIN(Year) > 1993)

DELETE FROM Maize_Prod WHERE Country IN (SELECT DISTINCT Country FROM Maize_Prod
                                         GROUP BY country
                                         HAVING MAX(year) < 2017)

DELETE FROM Maize_Prod WHERE Year < 1993 OR Year > 2017 




-----------------------------------------------------------------Maize_Yields---------------------------------------------------------------------------------	 

WITH MINRange_Cte
AS
(
SELECT Country, MIN(Year) [MYear] ,ROW_NUMBER() OVER (PARTITION BY MIN(Year) ORDER BY MIN(Year) DESC) AS [Same Range Occurance]  FROM Maize_Yields
GROUP BY Country
)

SELECT * FROM MINRange_Cte
WHERE MYear > 1993

GO

WITH MAXRange_Cte
AS
(
SELECT Country, MAX(Year) [MYear] ,ROW_NUMBER() OVER (PARTITION BY MAX(Year) ORDER BY MAX(Year) DESC) AS [Same Range Occurance]  FROM Maize_Yields
GROUP BY Country
)

SELECT * FROM MAXRange_Cte
WHERE MYear < 2017
GO



SELECT DISTINCT MIN(Year) FROM Maize_Yields
GROUP BY country
ORDER BY MIN(Year) DESC --MIN =  1993

SELECT DISTINCT MAX(Year) FROM Maize_Yields
GROUP BY country
ORDER BY MAX(Year) ASC -- MAX = 2017

DELETE FROM Maize_Yields WHERE Country IN (SELECT DISTINCT Country FROM Maize_Yields
                                         GROUP BY country
                                         HAVING MIN(Year) > 1993)

DELETE FROM Maize_Yields WHERE Country IN (SELECT DISTINCT Country FROM Maize_Yields
                                         GROUP BY country
                                         HAVING MAX(year) < 2017)

DELETE FROM Maize_Yields WHERE Year < 1993 OR Year > 2017 


-----------------------------------------------------------------Rice_Prod---------------------------------------------------------------------------------	
WITH MINRange_Cte
AS
(
SELECT Country, MIN(Year) [MYear] ,ROW_NUMBER() OVER (PARTITION BY MIN(Year) ORDER BY MIN(Year) DESC) AS [Same Range Occurance]  FROM Rice_Prod
GROUP BY Country
)

SELECT * FROM MINRange_Cte
WHERE MYear > 1993

GO

WITH MAXRange_Cte
AS
(
SELECT Country, MAX(Year) [MYear] ,ROW_NUMBER() OVER (PARTITION BY MAX(Year) ORDER BY MAX(Year) DESC) AS [Same Range Occurance]  FROM Rice_Prod
GROUP BY Country
)

SELECT * FROM MAXRange_Cte
WHERE MYear < 2017
GO

SELECT DISTINCT MIN(Year) FROM Rice_Prod
GROUP BY country
ORDER BY MIN(Year) DESC --MIN =  1993

SELECT DISTINCT MAX(Year) FROM Rice_Prod
GROUP BY country
ORDER BY MAX(Year) ASC -- MAX = 2017


DELETE FROM Rice_Prod WHERE Country IN (SELECT DISTINCT Country FROM Rice_Prod
                                         GROUP BY country
                                         HAVING MIN(Year) > 1993)

DELETE FROM Rice_Prod WHERE Country IN (SELECT DISTINCT Country FROM Rice_Prod
                                         GROUP BY country
                                         HAVING MAX(year) < 2017)

DELETE FROM Rice_Prod WHERE Year < 1993 OR Year > 2017 

-----------------------------------------------------------------Rice_Yields---------------------------------------------------------------------------------	 
WITH MINRange_Cte
AS
(
SELECT Country, MIN(Year) [MYear] ,ROW_NUMBER() OVER (PARTITION BY MIN(Year) ORDER BY MIN(Year) DESC) AS [Same Range Occurance]  FROM Rice_Yields
GROUP BY Country
)

SELECT * FROM MINRange_Cte
WHERE MYear > 1993

GO

WITH MAXRange_Cte
AS
(
SELECT Country, MAX(Year) [MYear] ,ROW_NUMBER() OVER (PARTITION BY MAX(Year) ORDER BY MAX(Year) DESC) AS [Same Range Occurance]  FROM Rice_Yields
GROUP BY Country
)

SELECT * FROM MAXRange_Cte
WHERE MYear < 2017
GO


SELECT DISTINCT MIN(Year) FROM Rice_Yields
GROUP BY country
ORDER BY MIN(Year) DESC --MIN =  1993

SELECT DISTINCT MAX(Year) FROM Rice_Yields
GROUP BY country
ORDER BY MAX(Year) ASC -- MAX = 2017


DELETE FROM Rice_Yields WHERE Country IN (SELECT DISTINCT Country FROM Rice_Yields
                                         GROUP BY country
                                         HAVING MIN(Year) > 1993)

DELETE FROM Rice_Yields WHERE Country IN (SELECT DISTINCT Country FROM Rice_Yields
                                         GROUP BY country
                                         HAVING MAX(year) < 2017)

DELETE FROM Rice_Yields WHERE Year < 1993 OR Year > 2017 

-----------------------------------------------------------------Surface_Temp_Anom---------------------------------------------------------------------------------	 
WITH MINRange_Cte
AS
(
SELECT Country, MIN(Year) [MYear] ,ROW_NUMBER() OVER (PARTITION BY MIN(Year) ORDER BY MIN(Year) DESC) AS [Same Range Occurance]  FROM Surface_Temp_Anom
GROUP BY Country
)

SELECT * FROM MINRange_Cte
WHERE MYear > 1993

GO

WITH MAXRange_Cte
AS
(
SELECT Country, MAX(Year) [MYear] ,ROW_NUMBER() OVER (PARTITION BY MAX(Year) ORDER BY MAX(Year) DESC) AS [Same Range Occurance]  FROM Surface_Temp_Anom
GROUP BY Country
)

SELECT * FROM MAXRange_Cte
GO


SELECT DISTINCT MIN(Year) FROM Surface_Temp_Anom
GROUP BY country
ORDER BY MIN(Year) DESC --MIN =  1993

SELECT DISTINCT MAX(Year) FROM Surface_Temp_Anom
GROUP BY country
ORDER BY MAX(Year) ASC -- MAX = 2017


DELETE FROM Surface_Temp_Anom WHERE Country IN (SELECT DISTINCT Country FROM Surface_Temp_Anom
                                         GROUP BY country
                                         HAVING MIN(Year) > 1993)

DELETE FROM Surface_Temp_Anom WHERE Country IN (SELECT DISTINCT Country FROM Surface_Temp_Anom
                                         GROUP BY country
                                         HAVING MAX(year) < 2017)

DELETE FROM Surface_Temp_Anom WHERE Year < 1993 OR Year > 2017 


-----------------------------------------------------------------Wheat_prod---------------------------------------------------------------------------------	 
WITH MINRange_Cte
AS
(
SELECT Country, MIN(Year) [MYear] ,ROW_NUMBER() OVER (PARTITION BY MIN(Year) ORDER BY MIN(Year) DESC) AS [Same Range Occurance]  FROM Wheat_prod
GROUP BY Country
)

SELECT * FROM MINRange_Cte
WHERE MYear > 1993

GO

WITH MAXRange_Cte
AS
(
SELECT Country, MAX(Year) [MYear] ,ROW_NUMBER() OVER (PARTITION BY MAX(Year) ORDER BY MAX(Year) DESC) AS [Same Range Occurance]  FROM Wheat_prod
GROUP BY Country
)

SELECT * FROM MAXRange_Cte
WHERE MYear < 2017
GO


SELECT DISTINCT MIN(Year) FROM Wheat_prod
GROUP BY country
ORDER BY MIN(Year) DESC --MIN =  1993

SELECT DISTINCT MAX(Year) FROM Wheat_prod
GROUP BY country
ORDER BY MAX(Year) ASC -- MAX = 2017


DELETE FROM Wheat_prod WHERE Country IN (SELECT DISTINCT Country FROM Wheat_prod
                                         GROUP BY country
                                         HAVING MIN(Year) > 1993)

DELETE FROM Wheat_prod WHERE Country IN (SELECT DISTINCT Country FROM Wheat_prod
                                         GROUP BY country
                                         HAVING MAX(year) < 2017)

DELETE FROM Wheat_prod WHERE Year < 1993 OR Year > 2017

-----------------------------------------------------------------Wheat_Yields---------------------------------------------------------------------------------	 
WITH MINRange_Cte
AS
(
SELECT Country, MIN(Year) [MYear] ,ROW_NUMBER() OVER (PARTITION BY MIN(Year) ORDER BY MIN(Year) DESC) AS [Same Range Occurance]  FROM Wheat_Yields
GROUP BY Country
)

SELECT * FROM MINRange_Cte
WHERE MYear > 1993

GO

WITH MAXRange_Cte
AS
(
SELECT Country, MAX(Year) [MYear] ,ROW_NUMBER() OVER (PARTITION BY MAX(Year) ORDER BY MAX(Year) DESC) AS [Same Range Occurance]  FROM Wheat_Yields
GROUP BY Country
)

SELECT * FROM MAXRange_Cte
WHERE MYear < 2017
GO



SELECT DISTINCT MIN(Year) FROM Wheat_Yields
GROUP BY country
ORDER BY MIN(Year) DESC --MIN =  1993

SELECT DISTINCT MAX(Year) FROM Wheat_Yields
GROUP BY country
ORDER BY MAX(Year) ASC -- MAX = 2017

DELETE FROM Wheat_Yields WHERE Country IN (SELECT DISTINCT Country FROM Wheat_Yields
                                         GROUP BY country
                                         HAVING MIN(Year) > 1993)

DELETE FROM Wheat_Yields WHERE Country IN (SELECT DISTINCT Country FROM Wheat_Yields
                                         GROUP BY country
                                         HAVING MAX(year) < 2017)

DELETE FROM Wheat_Yields WHERE Year < 1993 OR Year > 2017
 

 --********DEFINING PRIMARY KEYS 

 --CO2_Data

 ALTER TABLE Co2_Data
 ALTER COLUMN 
              iso_Code CHAR(3) NOT NULL;

GO

 ALTER TABLE Co2_Data
 ALTER COLUMN 
              [Year] SMALLINT NOT NULL;		  
GO

ALTER TABLE Co2_Data
ALTER COLUMN 
             [Population] BIGINT NOT NULL;
GO

ALTER TABLE Co2_Data 
ADD PRIMARY KEY (iso_Code,[Year])
GO

--Maize_Prod

 ALTER TABLE Maize_Prod
 ALTER COLUMN 
              CODE CHAR(3) NOT NULL;
GO

 ALTER TABLE Maize_Prod
 ALTER COLUMN 
              [Year] SMALLINT NOT NULL;		  
GO

ALTER TABLE Maize_Prod 
ADD PRIMARY KEY (Code,[Year])
GO

--Maize_Yields

 ALTER TABLE Maize_Yields
 ALTER COLUMN 
              CODE CHAR(3) NOT NULL;
GO
 ALTER TABLE Maize_Yields
 ALTER COLUMN 
              [Year] SMALLINT NOT NULL;		  
GO

ALTER TABLE Maize_Yields 
ADD PRIMARY KEY (Code,[Year])
GO

--Rice_Prod

 ALTER TABLE Rice_Prod
 ALTER COLUMN 
              CODE CHAR(3) NOT NULL;
GO

 ALTER TABLE Rice_Prod
 ALTER COLUMN 
              [Year] SMALLINT NOT NULL;		  
GO

ALTER TABLE Rice_Prod 
ADD PRIMARY KEY (Code,[Year])
GO

--Rice_Yields

 ALTER TABLE Rice_Yields
 ALTER COLUMN 
              CODE CHAR(3) NOT NULL;
GO

 ALTER TABLE Rice_Yields
 ALTER COLUMN 
              [Year] SMALLINT NOT NULL;		  
GO

ALTER TABLE Rice_Yields 
ADD PRIMARY KEY (Code,[Year])
GO

--Surface_Temp_Anom

 ALTER TABLE Surface_Temp_Anom
 ALTER COLUMN 
              CODE CHAR(3) NOT NULL;
GO

 ALTER TABLE Surface_Temp_Anom
 ALTER COLUMN 
              [Year] SMALLINT NOT NULL;		  
GO

ALTER TABLE Surface_Temp_Anom 
ADD PRIMARY KEY (Code,[Year])
GO

--Wheat_Prod

 ALTER TABLE Wheat_Prod
 ALTER COLUMN 
              CODE CHAR(3) NOT NULL;
GO

 ALTER TABLE Wheat_Prod
 ALTER COLUMN 
              [Year] SMALLINT NOT NULL;		  
GO

ALTER TABLE Wheat_Prod 
ADD PRIMARY KEY (Code,[Year])
GO

--Wheat_Yields


 ALTER TABLE Wheat_Yields
 ALTER COLUMN 
              CODE CHAR(3) NOT NULL;
GO

 ALTER TABLE Wheat_Yields
 ALTER COLUMN 
              [Year] SMALLINT NOT NULL;		  
GO

ALTER TABLE Wheat_Yields 
ADD PRIMARY KEY (Code,[Year])
GO

--MERGE 

CREATE VIEW vw_CntryToDlt
AS
(
SELECT DISTINCT *
FROM CO2_Data CD
FULL OUTER JOIN Maize_Prod MP ON CD.iso_code = MP.Code AND CD.[year] = MP.[Year]
FULL OUTER JOIN Maize_Yields MY ON CD.iso_code = MY.Code AND CD.[year] = MY.[Year]
FULL OUTER JOIN Rice_Prod RP ON CD.iso_code = RP.Code AND CD.[year] = RP.[Year]
FULL OUTER JOIN Rice_Yields RY ON CD.iso_code = Ry.Code AND CD.[year] = RY.[Year]
FULL OUTER JOIN Surface_Temp_Anom STA ON CD.iso_code = STA.Code AND CD.[year] = STA.[Year]
FULL OUTER JOIN Wheat_Prod WP ON CD.iso_code = WP.Code AND CD.[year] = WP.[Year]
FULL OUTER JOIN Wheat_Yields WY ON CD.iso_code = WY.Code AND CD.[year] = WY.[Year]
WHERE CD.country IS NULL
)

DELETE FROM CO2_Data WHERE country IS NULL
GO		
DELETE FROM Maize_Prod WHERE country IS NULL
GO
DELETE FROM Maize_Yields WHERE country IS NULL
GO
DELETE FROM Rice_Prod WHERE country IS NULL
GO
DELETE FROM Rice_Yields WHERE country IS NULL
GO
DELETE FROM Surface_Temp_Anom WHERE country IS NULL
GO
DELETE FROM Wheat_Prod WHERE country IS NULL
GO
DELETE FROM Wheat_Yields WHERE country IS NULL
GO


CREATE VIEW vw_WorldFoodProduction
AS
(

SELECT CD.country,CD.ISo_Code,CD.[Year],CD.co2,MP.Production_Tones [Maize Production],My.Yield_Hectogram_Per_Hectare[Maize Yield],RP.Production_Tones [Rice Production],RY.Yield_Hectogram_Per_Hectare [Rice Yield],
                STA.Surface_Temp_Anomaly [Surface Tempreature Anomaly],WP.Production_Tones [Wheat Production],WY.Yield_Hectogram_Per_Hectare [Wheat Yield],CD.Population 
FROM CO2_Data CD
JOIN Maize_Prod MP ON CD.iso_code = MP.Code AND CD.[year] = MP.[Year]
JOIN Maize_Yields MY ON MP.code = MY.Code AND MP.[year] = MY.[Year]
JOIN Rice_Prod RP ON MY.code = RP.Code AND MY.[year] = RP.[Year]
JOIN Rice_Yields RY ON RP.code = Ry.Code AND RP.[year] = RY.[Year] 
JOIN Surface_Temp_Anom STA ON RY.code = STA.Code AND RY.[year] = STA.[Year] 
JOIN Wheat_Prod WP ON STA.code = WP.Code AND STA.[year] = WP.[Year]
JOIN Wheat_Yields WY ON WP.code = WY.Code AND WP.[year] = WY.[Year]
)

SELECT * FROM vw_WorldFoodProduction