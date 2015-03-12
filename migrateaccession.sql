
UPDATE SAMPLE_ASSAY 
    SET USER_ACCESSION = CONCAT(CONCAT(SUBMISSION_ACCESSION, ' : '), USER_ACCESSION),
        SUBMISSION_ACCESSION = 'arrayexpress' 
    WHERE SUBMISSION_ACCESSION LIKE 'GAE-%' AND IS_DELETED = 0;

UPDATE SAMPLE_ASSAY
  SET SUBMISSION_ACCESSION = 'pride'
  WHERE ACCESSION in (
    SELECT MIN(accession) as minimum
      FROM (
        SELECT a.ACCESSION as accession, a.USER_ACCESSION as user_accession FROM SAMPLE_ASSAY a
            JOIN SAMPLE_ASSAY b ON a.USER_ACCESSION = b.USER_ACCESSION
          WHERE a.SUBMISSION_ACCESSION LIKE 'GPR-%' 
            AND b.SUBMISSION_ACCESSION LIKE 'GPR-%' 
            AND a.IS_DELETED = 0
            AND b.IS_DELETED = 0
            AND a.ACCESSION != b.ACCESSION    
      )
      GROUP BY user_accession
  );
UPDATE SAMPLE_ASSAY SET IS_DELETED = 1 WHERE SUBMISSION_ACCESSION LIKE 'GPR-%';
    
UPDATE SAMPLE_ASSAY
    SET SUBMISSION_ACCESSION = 'cosmic'
    WHERE SUBMISSION_ACCESSION LIKE 'GCM-%' AND IS_DELETED = 0;
    
UPDATE SAMPLE_ASSAY
    SET SUBMISSION_ACCESSION = 'ENA'
    WHERE SUBMISSION_ACCESSION LIKE 'ena' AND IS_DELETED = 0;
    
UPDATE SAMPLE_ASSAY
    SET SUBMISSION_ACCESSION = 'BioSamples'
    WHERE SUBMISSION_ACCESSION LIKE 'biosamples' AND IS_DELETED = 0;
    
UPDATE SAMPLE_ASSAY
    SET SUBMISSION_ACCESSION = 'ArrayExpress'
    WHERE SUBMISSION_ACCESSION LIKE 'arrayexpress' AND IS_DELETED = 0;



UPDATE SAMPLE_REFERENCE 
    SET USER_ACCESSION = CONCAT(CONCAT(SUBMISSION_ACCESSION, ' : '), USER_ACCESSION),
        SUBMISSION_ACCESSION = 'imsr' 
    WHERE SUBMISSION_ACCESSION LIKE 'GMS-%' AND IS_DELETED = 0;
    