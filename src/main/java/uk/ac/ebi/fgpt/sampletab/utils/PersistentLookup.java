package uk.ac.ebi.fgpt.sampletab.utils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistentLookup {
	private static final String driver = "org.apache.derby.jdbc.EmbeddedDriver";
	private static final String connectStr = "jdbc:derby:uk.ac.ebi.fgpt.sampletab.utils.PersistentLookup;create=true";
	private volatile Connection dbConnection = null;
	private volatile PreparedStatement insert = null;
	private volatile PreparedStatement has = null;
	private volatile PreparedStatement get = null;
	private volatile PreparedStatement hasTarget = null;
	private volatile PreparedStatement getTarget = null;

	// logging
	private Logger log = LoggerFactory.getLogger(getClass());

	public PersistentLookup() {
		try {
			Class.forName(driver).newInstance();
			dbConnection = DriverManager.getConnection(connectStr);
			
			DatabaseMetaData dbmd = dbConnection.getMetaData();
			ResultSet rs = dbmd.getTables(null, "APP", "lookups", null);
			if(!rs.next())
			{
				log.info("(re)creating table");
				Statement statement = dbConnection.createStatement();
				statement.execute("DROP TABLE lookups");
				
				String sql = "CREATE TABLE lookups ("
						+ "source VARCHAR(30) NOT NULL, sourceid VARCHAR(30) NOT NULL, "
						+ "target VARCHAR(30) NOT NULL, targetid VARCHAR(30))";
				statement = dbConnection.createStatement();
				statement.execute(sql);
			}
			
			insert = dbConnection.prepareStatement("INSERT INTO lookups VALUES (?, ?, ?, ?)");
			has = dbConnection.prepareStatement("SELECT count(*) FROM lookups WHERE source = ? AND sourceid = ? AND targetid = ?");
			get = dbConnection.prepareStatement("SELECT targetid FROM lookups WHERE source = ? AND sourceid = ? AND targetid != ?");
			hasTarget = dbConnection.prepareStatement("SELECT count(*) FROM lookups WHERE source = ? AND sourceid = ? AND target = ? AND targetid = ?");
			getTarget = dbConnection.prepareStatement("SELECT targetid FROM lookups WHERE source = ? AND sourceid = ? AND target = ? AND targetid != ?");

		} catch (InstantiationException e) {
			log.error("Unable to create embedded Derby database");
			e.printStackTrace();
			return;
		} catch (IllegalAccessException e) {
			log.error("Unable to create embedded Derby database");
			e.printStackTrace();
			return;
		} catch (ClassNotFoundException e) {
			log.error("Unable to create embedded Derby database");
			e.printStackTrace();
			return;
		} catch (SQLException e) {
			log.error("Unable to execute SQL on embedded Derby database");
			printSQLException(e);
			return;
		}
	}
	
	public boolean hasValues(String source, String sourceID) throws SQLException{
		has.clearParameters();
		has.setString(1, source);
		has.setString(2, sourceID);
		has.setString(3, null);
		ResultSet results = has.executeQuery();
		results.next();
		return new Boolean(results.getString(1)).booleanValue();
	}
	
	public Collection<String> getValues(String source, String sourceID) throws SQLException{
		//Assumes that source is in the database
		get.clearParameters();
		get.setString(1, source);
		get.setString(2, sourceID);
		get.setString(3, null);
		ResultSet results = get.executeQuery();
		Collection<String> values = new ArrayList<String>();
        while ( results.next() ) {
        	values.add(results.getString(1));
        }
        return values;
	}
	
	public boolean hasValuesOfTarget(String source, String sourceID, String target) throws SQLException{
		hasTarget.clearParameters();
		hasTarget.setString(1, source);
		hasTarget.setString(2, sourceID);
		hasTarget.setString(3, target);
		hasTarget.setString(4, null);
		ResultSet results = hasTarget.executeQuery();
		results.next();
		return new Boolean(results.getString(1)).booleanValue();
	}
	
	public Collection<String> getValuesOfTarget(String source, String sourceID, String target) throws SQLException{
		//Assumes that source is in the database
		getTarget.clearParameters();
		getTarget.setString(1, source);
		getTarget.setString(2, sourceID);
		getTarget.setString(3, target);
		getTarget.setString(4, null);
		ResultSet results = getTarget.executeQuery();
		Collection<String> values = new ArrayList<String>();
        while ( results.next() ) {
        	values.add(results.getString(1));
        }
        return values;
	}
	
	public void setValues(String source, String sourceID, String target, Collection<String> targetIDs) throws SQLException{
		//TODO optimize this SQL 
		for (String targetID : targetIDs){
			insert.clearParameters();
			insert.setString(1, source);
			insert.setString(2, sourceID);
			insert.setString(3, target);
			insert.setString(4, targetID);
			insert.executeUpdate();
		}
		//add a null to indicate that something was added, even if it was nothing
		insert.clearParameters();
		insert.setString(1, source);
		insert.setString(2, sourceID);
		insert.setString(3, target);
		insert.setString(4, null);
		insert.executeUpdate();
	}

	public void shutdown() {
		try {
			//close each prepared statement we started earlier
			insert.close();
			has.close();
			get.close();
			
			// the shutdown=true attribute shuts down Derby
			DriverManager.getConnection("jdbc:derby:;shutdown=true");

			// To shut down a specific database only, but keep the
			// engine running (for example for connecting to other
			// databases), specify a database in the connection URL:
			// DriverManager.getConnection("jdbc:derby:" + dbName +
			// ";shutdown=true");
		} catch (SQLException e) {
			if (((e.getErrorCode() == 50000) && ("XJ015"
					.equals(e.getSQLState())))) {
				// we got the expected exception
				log.info("Derby shut down normally");
				// Note that for single database shutdown, the expected
				// SQL state is "08006", and the error code is 45000.
			} else {
				// if the error code or SQLState is different, we have
				// an unexpected exception (shutdown failed)
				log.error("Derby did not shut down normally");
				printSQLException(e);
			}
		}
	}

	public void finalize() throws Throwable {
		try {
			shutdown();
		} finally {
			super.finalize();
		}
	}

	/**
	 * Prints details of an SQLException chain to <code>System.err</code>.
	 * Details included are SQL State, Error code, Exception message.
	 * 
	 * @param e
	 *            the SQLException from which to print details.
	 */
	public void printSQLException(SQLException e) {
		// Unwraps the entire exception chain to unveil the real cause of the
		// Exception.
		while (e != null) {
			log.error("\n----- SQLException -----");
			log.error("  SQL State:  " + e.getSQLState());
			log.error("  Error Code: " + e.getErrorCode());
			log.error("  Message:    " + e.getMessage());
			// for stack traces, refer to derby.log or uncomment this:
			// e.printStackTrace(System.err);
			e = e.getNextException();
		}
	}
}
