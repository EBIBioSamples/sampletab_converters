package uk.ac.ebi.fgpt.sampletab.subs;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.annotation.Transactional;

public class ExperimentDAO {

    private JdbcTemplate jdbcTemplate;
    private DataSource dataSource;

    private Logger log = LoggerFactory.getLogger(getClass());
    
    public ExperimentDAO(DataSource dataSource) {
        setDataSource(dataSource);
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
        jdbcTemplate = new JdbcTemplate(dataSource);
    }
    
    @Transactional
    public void storeExperiment(Experiment e) {
       
        if (e.getId() == null) {
            //new, add it
            jdbcTemplate.update("INSERT INTO experiments (accession, name, date_submitted, date_last_processed, comment, " +
            		"is_deleted, num_submissions, num_samples, release_date, is_released) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ", 
                    new Object[]{ e.getAccession(), e.getName(), e.getSubmitted(), e.getLastProcessed(),
                    e.getComment(), e.isDeleted(), e.getNumGroups(), e.getNumSamples(), e.getReleaseDate(), e.isReleased()} );
            //add id number 
            e.setId(jdbcTemplate.queryForInt("SELECT id FROM experiments WHERE accession = ?", 
                    new Object[]{ e.getAccession()}));
        } else {
            //existing, update it
            jdbcTemplate.update("UPDATE experiments SET accession = ? , name = ? , " +
                    "date_submitted = ? , date_last_processed = ? , comment = ? , " +
                    "is_deleted = ? , num_submissions = ? , num_samples = ? , release_date = ? , is_released = ?" +
                    "WHERE id = ?", 
                    new Object[]{ e.getAccession(), e.getName(), e.getSubmitted(), e.getLastProcessed(),
                    e.getComment(), e.isDeleted(), e.getNumGroups(), e.getNumSamples(), e.getReleaseDate(), e.isReleased(),
                    e.getId()});
        }
    }
    
    public Experiment getExperiment(int id) {
        List<Experiment> experiments = jdbcTemplate.query("SELECT * FROM experiments WHERE id = ?",
                new Object[] { id },
                new ExperimentRowMapper());
        return experiments.get(0);
    }

    @Transactional
    public Experiment getExperiment(String submissionIdentifier) {
        List<Experiment> experiments = jdbcTemplate.query("SELECT * FROM experiments WHERE accession = ?",
                new Object[] { submissionIdentifier },
                new ExperimentRowMapper());
        //no experiment? create
        if (experiments.size() == 0 || experiments.get(0) == null) {
            Experiment experiment = new Experiment();
            experiment.setAccession(submissionIdentifier);
            storeExperiment(experiment);
            return experiment;
        } else {
            return experiments.get(0);
        }
    }
    
    private class ExperimentRowMapper implements RowMapper {

        @Override
        public Experiment mapRow(ResultSet rs, int line) throws SQLException {
          ExperimentResultSetExtractor extractor = new ExperimentResultSetExtractor();
          return extractor.extractData(rs);
        }
    }
    
    private class ExperimentResultSetExtractor implements ResultSetExtractor {

        @Override
        public Experiment extractData(ResultSet rs) throws SQLException {
          Experiment exp = new Experiment();
          exp.setId(rs.getInt("id"));
          exp.setAccession(rs.getString("accession"));
          exp.setName(rs.getString("name"));
          exp.setSubmitted(rs.getDate("date_submitted"));
          exp.setLastProcessed(rs.getDate("date_last_processed"));
          exp.setComment(rs.getString("comment"));
          exp.setDeleted(rs.getInt("is_deleted"));
          exp.setNumGroups(rs.getInt("num_submissions"));
          exp.setNumSamples(rs.getInt("num_samples"));
          exp.setReleaseDate(rs.getDate("release_date"));
          exp.setReleased(rs.getBoolean("is_released"));
          return exp;
        }
    }
}
