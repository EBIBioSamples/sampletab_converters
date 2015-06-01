
--ArrayExpress
UPDATE SAMPLE_ASSAY 
  SET USER_ACCESSION = SUBSTR(SUBMISSION_ACCESSION,3), SUBMISSION_ACCESSION = 'ArrayExpress' 
  WHERE SUBMISSION_ACCESSION LIKE 'GAE-%' 
    AND IS_DELETED = 0;

--PRIDE samples in multiple submissions put to minimum
--UPDATE SAMPLE_ASSAY
--  SET SUBMISSION_ACCESSION = 'pride'
--  WHERE ACCESSION in (
--    SELECT MIN(accession) as minimum
--      FROM (
--        SELECT a.ACCESSION as accession, a.USER_ACCESSION as user_accession FROM SAMPLE_ASSAY a
--            JOIN SAMPLE_ASSAY b ON a.USER_ACCESSION = b.USER_ACCESSION
--          WHERE a.SUBMISSION_ACCESSION LIKE 'GPR-%' 
--            AND b.SUBMISSION_ACCESSION LIKE 'GPR-%' 
--            AND a.IS_DELETED = 0
--            AND b.IS_DELETED = 0
--            AND a.ACCESSION != b.ACCESSION    
--      )
--      GROUP BY user_accession
--  );
--THIS WONT WORK PROPERLY, NEED TO HANDLE MIN AND MAX AT ONCE
    
    
    
UPDATE SAMPLE_REFERENCE 
    SET USER_ACCESSION = CONCAT(CONCAT(SUBMISSION_ACCESSION, ' : '), USER_ACCESSION),
        SUBMISSION_ACCESSION = 'cosmic' 
    WHERE SUBMISSION_ACCESSION LIKE 'GCM-%' AND IS_DELETED = 0;
    
UPDATE SAMPLE_ASSAY
    SET SUBMISSION_ACCESSION = 'ENA'
    WHERE SUBMISSION_ACCESSION LIKE 'ena' AND IS_DELETED = 0;


--cleanup some old / erroneous submissions
--DGVa no longer exists/imported
UPDATE SAMPLE_ASSAY SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION LIKE 'GVA-%';
UPDATE SAMPLE_GROUPS SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION LIKE 'GVA-%';
--IMSR is reference, not assay
UPDATE SAMPLE_ASSAY SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION LIKE 'GMS-%';
--CGaP error (should be GSB-3)
UPDATE SAMPLE_ASSAY SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION = 'GCG-hipsci';
--submission errors
UPDATE SAMPLE_ASSAY SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION = 'GSB-128';
UPDATE SAMPLE_ASSAY SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION = 'GSB-129';
UPDATE SAMPLE_ASSAY SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION = 'GSB-130';
UPDATE SAMPLE_ASSAY SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION = 'GSB-131';
UPDATE SAMPLE_ASSAY SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION = 'GSB-132';
UPDATE SAMPLE_ASSAY SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION = 'GSB-133';
UPDATE SAMPLE_ASSAY SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION = 'GSB-134';
UPDATE SAMPLE_ASSAY SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION = 'GSB-135';
UPDATE SAMPLE_ASSAY SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION = 'GSB-41';
UPDATE SAMPLE_ASSAY SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION = 'GSB-42';
UPDATE SAMPLE_ASSAY SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION = 'GSB-45';
UPDATE SAMPLE_ASSAY SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION = 'GSB-67';

--direct submission cleanupUPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'biosamples' WHERE SUBMISSION_ACCESSION = 'GSB-1' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'eva' WHERE SUBMISSION_ACCESSION = 'GSB-100' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-101' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-104' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-105' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-106' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-107' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-108' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'eva' WHERE SUBMISSION_ACCESSION = 'GSB-109' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-110' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-111' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-112' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'eva' WHERE SUBMISSION_ACCESSION = 'GSB-117' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'eva' WHERE SUBMISSION_ACCESSION = 'GSB-118' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-119' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-120' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-121' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-122' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-123' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-124' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-125' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-126' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'biosamples' WHERE SUBMISSION_ACCESSION = 'GSB-127' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-13' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-131' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-132' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-133' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-134' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-135' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-136' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-137' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-138' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-139' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-140' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-141' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'biosamples' WHERE SUBMISSION_ACCESSION = 'GSB-142' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'biosamples' WHERE SUBMISSION_ACCESSION = 'GSB-143' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'biosamples' WHERE SUBMISSION_ACCESSION = 'GSB-144' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'eva' WHERE SUBMISSION_ACCESSION = 'GSB-147' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'eva' WHERE SUBMISSION_ACCESSION = 'GSB-148' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-149' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-15' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-150' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-158' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-16' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'eva' WHERE SUBMISSION_ACCESSION = 'GSB-174' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'eva' WHERE SUBMISSION_ACCESSION = 'GSB-175' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'eva' WHERE SUBMISSION_ACCESSION = 'GSB-179' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-189' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-19' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-190' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-197' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-198' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-199' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'biosamples' WHERE SUBMISSION_ACCESSION = 'GSB-2' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-200' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-201' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-202' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-203' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-204' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-205' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-206' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-207' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-208' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-209' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-21' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-210' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-211' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-212' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-213' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-214' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-215' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-216' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'biosamples' WHERE SUBMISSION_ACCESSION = 'GSB-217' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-22' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-23' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-24' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-25' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-26' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-27' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-29' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'hipsci' WHERE SUBMISSION_ACCESSION = 'GSB-3' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-30' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-31' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'biosamples' WHERE SUBMISSION_ACCESSION = 'GSB-32' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-35' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-36' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-37' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-38' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'biosamples' WHERE SUBMISSION_ACCESSION = 'GSB-4' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'biosamples' WHERE SUBMISSION_ACCESSION = 'GSB-40' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'biosamples' WHERE SUBMISSION_ACCESSION = 'GSB-41' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'biosamples' WHERE SUBMISSION_ACCESSION = 'GSB-42' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'biosamples' WHERE SUBMISSION_ACCESSION = 'GSB-43' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-44' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-45' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-46' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-47' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-48' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-49' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-50' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-51' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'biosamples' WHERE SUBMISSION_ACCESSION = 'GSB-52' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-53' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'biosamples' WHERE SUBMISSION_ACCESSION = 'GSB-54' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'eva' WHERE SUBMISSION_ACCESSION = 'GSB-56' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-57' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'biosamples' WHERE SUBMISSION_ACCESSION = 'GSB-58' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-59' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-60' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-61' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-62' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-63' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-66' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-67' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'biosamples' WHERE SUBMISSION_ACCESSION = 'GSB-68' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-8' AND IS_DELETED = 0;
UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-TEST-19' AND IS_DELETED = 0;

UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'biosamples' WHERE SUBMISSION_ACCESSION = 'GSB-1';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'eva' WHERE SUBMISSION_ACCESSION = 'GSB-100';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-101';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-104';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-105';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-106';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-107';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-108';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'eva' WHERE SUBMISSION_ACCESSION = 'GSB-109';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-110';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-111';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-112';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'eva' WHERE SUBMISSION_ACCESSION = 'GSB-117';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'eva' WHERE SUBMISSION_ACCESSION = 'GSB-118';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-119';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-120';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-121';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-122';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-123';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-124';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-125';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-126';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'biosamples' WHERE SUBMISSION_ACCESSION = 'GSB-127';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-13';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-131';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-132';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-133';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-134';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-135';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-136';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-137';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-138';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-139';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-140';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-141';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'biosamples' WHERE SUBMISSION_ACCESSION = 'GSB-142';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'biosamples' WHERE SUBMISSION_ACCESSION = 'GSB-143';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'biosamples' WHERE SUBMISSION_ACCESSION = 'GSB-144';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'eva' WHERE SUBMISSION_ACCESSION = 'GSB-147';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'eva' WHERE SUBMISSION_ACCESSION = 'GSB-148';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-149';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-15';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-150';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-158';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-16';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'eva' WHERE SUBMISSION_ACCESSION = 'GSB-174';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'eva' WHERE SUBMISSION_ACCESSION = 'GSB-175';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'eva' WHERE SUBMISSION_ACCESSION = 'GSB-179';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-189';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-19';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-190';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-197';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-198';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-199';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'biosamples' WHERE SUBMISSION_ACCESSION = 'GSB-2';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-200';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-201';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-202';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-203';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-204';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-205';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-206';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-207';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-208';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-209';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-21';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-210';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-211';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-212';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-213';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-214';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-215';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'ebisc' WHERE SUBMISSION_ACCESSION = 'GSB-216';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'biosamples' WHERE SUBMISSION_ACCESSION = 'GSB-217';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-22';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-23';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-24';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-25';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-26';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-27';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-29';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'hipsci' WHERE SUBMISSION_ACCESSION = 'GSB-3';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-30';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-31';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'biosamples' WHERE SUBMISSION_ACCESSION = 'GSB-32';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-35';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-36';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-37';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-38';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'biosamples' WHERE SUBMISSION_ACCESSION = 'GSB-4';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'biosamples' WHERE SUBMISSION_ACCESSION = 'GSB-40';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'biosamples' WHERE SUBMISSION_ACCESSION = 'GSB-41';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'biosamples' WHERE SUBMISSION_ACCESSION = 'GSB-42';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'biosamples' WHERE SUBMISSION_ACCESSION = 'GSB-43';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-44';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-45';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-46';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-47';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-48';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-49';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-50';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-51';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'biosamples' WHERE SUBMISSION_ACCESSION = 'GSB-52';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-53';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'biosamples' WHERE SUBMISSION_ACCESSION = 'GSB-54';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'eva' WHERE SUBMISSION_ACCESSION = 'GSB-56';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-57';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'biosamples' WHERE SUBMISSION_ACCESSION = 'GSB-58';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-59';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-60';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-61';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-62';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-63';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-66';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-67';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'biosamples' WHERE SUBMISSION_ACCESSION = 'GSB-68';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-8';
UPDATE SAMPLE_GROUP SET SUBMISSION_ACCESSION = 'bbmri' WHERE SUBMISSION_ACCESSION = 'GSB-TEST-19';
--TODO other accessions



--query to check for unfixed submissions
SELECT DISTINCT SUBMISSION_ACCESSION FROM SAMPLE_ASSAY 
  WHERE IS_DELETED = 0;

--Reference Samples
--IMSR
--note this doesn't handle duplicates correctly, but neither does the existing system
UPDATE SAMPLE_REFERENCE 
    SET USER_ACCESSION = CONCAT(CONCAT(SUBMISSION_ACCESSION, ' : '), USER_ACCESSION),
        SUBMISSION_ACCESSION = 'imsr' 
    WHERE SUBMISSION_ACCESSION LIKE 'GMS-%' AND IS_DELETED = 0;
    
--TODO FINISH
    
    
--Groups
--ArrayExpress
UPDATE SAMPLE_GROUPS 
  SET USER_ACCESSION = SUBSTR(SUBMISSION_ACCESSION,3), SUBMISSION_ACCESSION = 'ArrayExpress' 
  WHERE SUBMISSION_ACCESSION LIKE 'GAE-%' 
    AND IS_DELETED = 0;
--PRIDE
UPDATE SAMPLE_GROUPS 
  SET USER_ACCESSION = SUBMISSION_ACCESSION, SUBMISSION_ACCESSION = 'pride' 
  WHERE SUBMISSION_ACCESSION LIKE 'GPR-%' 
    AND IS_DELETED = 0;
--ENA
UPDATE SAMPLE_GROUPS 
  SET USER_ACCESSION = SUBSTR(SUBMISSION_ACCESSION,5), SUBMISSION_ACCESSION = 'ENA' 
  WHERE SUBMISSION_ACCESSION LIKE 'GEN-?RP%' 
    AND IS_DELETED = 0;
--COSMIC
UPDATE SAMPLE_GROUPS 
  SET USER_ACCESSION = SUBMISSION_ACCESSION, SUBMISSION_ACCESSION = 'cosmic' 
  WHERE SUBMISSION_ACCESSION LIKE 'GCM-%' 
    AND IS_DELETED = 0;
    
--cleanup
UPDATE SAMPLE_GROUPS SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION LIKE 'GEM-%';
UPDATE SAMPLE_GROUPS SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION LIKE 'GNC-SAM%';
UPDATE SAMPLE_GROUPS SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION = 'GMS-24';
UPDATE SAMPLE_GROUPS SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION = 'GMS-1';
UPDATE SAMPLE_GROUPS SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION = 'GMS-9';
UPDATE SAMPLE_GROUPS SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION = 'GMS-14';
UPDATE SAMPLE_GROUPS SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION = 'GMS-12';
UPDATE SAMPLE_GROUPS SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION = 'GMS-4';
UPDATE SAMPLE_GROUPS SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION = 'GMS-2';
UPDATE SAMPLE_GROUPS SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION = 'GMS-17';
UPDATE SAMPLE_GROUPS SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION = 'GMS-6';
UPDATE SAMPLE_GROUPS SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION = 'GMS-11';
UPDATE SAMPLE_GROUPS SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION = 'GMS-21';
UPDATE SAMPLE_GROUPS SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION = 'GMS-25';
UPDATE SAMPLE_GROUPS SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION = 'GMS-20';
UPDATE SAMPLE_GROUPS SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION = 'GMS-26';
UPDATE SAMPLE_GROUPS SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION = 'GMS-3';
UPDATE SAMPLE_GROUPS SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION = 'GMS-23';
UPDATE SAMPLE_GROUPS SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION = 'GMS-13';
UPDATE SAMPLE_GROUPS SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION = 'GMS-5';
UPDATE SAMPLE_GROUPS SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION = 'GMS-7';
UPDATE SAMPLE_GROUPS SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION = 'GMS-10';

--IMSR do this after cleanup
UPDATE SAMPLE_GROUPS 
  SET USER_ACCESSION = SUBMISSION_ACCESSION, SUBMISSION_ACCESSION = 'imsr' 
  WHERE SUBMISSION_ACCESSION LIKE 'GMS-%' 
    AND IS_DELETED = 0;


--TODO FINISH    