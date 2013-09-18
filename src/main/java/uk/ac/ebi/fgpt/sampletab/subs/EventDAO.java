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

public class EventDAO {

    private JdbcTemplate jdbcTemplate;
    private DataSource dataSource;

    private Logger log = LoggerFactory.getLogger(getClass());

    
    public EventDAO(DataSource dataSource) {
        setDataSource(dataSource);
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
        jdbcTemplate = new JdbcTemplate(dataSource);
    }
    
    @Transactional
    public void storeEvent(Event e) {
        if (e.getId() == null) {
            //new, add it
            jdbcTemplate.update("INSERT INTO events (experiment_id, " +
        		"event_type, was_successful, start_time, end_time, " +
        		"machine, operator, log_file, comment, is_deleted) " +
        		"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ", new Object[]{e.getExperiment(), 
                    e.getEventType(), e.getWasSuccessful(), e.getStartTime(), 
                    e.getEndTime(), e.getMachine(), e.getOperator(), e.getLogFile(), 
                    e.getComment(), e.isDeleted() } );
            //add id number 
            e.setId(jdbcTemplate.queryForInt("SELECT id FROM events WHERE experiment_id = ? " +
            		"AND event_type = ? AND start_time = ?", 
            		new Object[]{e.getExperiment(), e.getEventType(), e.getStartTime(),}));
        } else {
            //existing, update it
            jdbcTemplate.update("UPDATE events SET experiment_id = ? , " +
                    "event_type = ? , was_successful = ? , start_time = ? ," +
                    "end_time = ? , machine = ? , operator = ?, " +
                    "log_file = ? , comment = ? , is_deleted = ? " +
                    "WHERE id = ? ", new Object[]{e.getExperiment(), 
                    e.getEventType(), e.getWasSuccessful(), e.getStartTime(), 
                    e.getEndTime(), e.getMachine(), e.getOperator(), e.getLogFile(), 
                    e.getComment(), e.isDeleted(), e.getId() });
        }
    }
    
    public Event getEvent(int id) {
        List<Event> experiments = jdbcTemplate.query("SELECT * FROM events WHERE id = ?",
                new Object[] { id },
                new ExperimentRowMapper());
        return experiments.get(0);
    }
    
    public Event getEvent(String submissionIdentifier) {
        List<Event> events = jdbcTemplate.query("SELECT * FROM events WHERE accession = ?",
                new Object[] { submissionIdentifier },
                new ExperimentRowMapper());
        return events.get(0);
    }
    
    private class ExperimentRowMapper implements RowMapper {

        @Override
        public Event mapRow(ResultSet rs, int line) throws SQLException {
            EventResultSetExtractor extractor = new EventResultSetExtractor();
          return extractor.extractData(rs);
        }
    }
    
    private class EventResultSetExtractor implements ResultSetExtractor {

        @Override
        public Event extractData(ResultSet rs) throws SQLException {
            Event event = new Event();
            event.setId(rs.getInt("id"));
            event.setExperiment(rs.getInt("experiment"));
            event.setEventType(rs.getString("event_type"));
            event.setWasSuccessful(rs.getInt("was_successful"));
            event.setStartTime(rs.getDate("start_time"));
            event.setEndTime(rs.getDate("end_time"));
            event.setMachine(rs.getString("machine"));
            event.setOperator(rs.getString("operator"));
            event.setLogFile(rs.getString("log_file"));
            event.setComment(rs.getString("comment"));
            event.setDeleted(rs.getInt("is_deleted"));  
            return event;
        }
    }
}
