/**
 * EasyBeans
 * Copyright (C) 2006-2007 Bull S.A.S.
 * Contact: easybeans@ow2.org
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
 * USA
 *
 * --------------------------------------------------------------------------
 * $Id: ConnectionManager.java 5369 2010-02-24 14:58:19Z benoitf $
 * --------------------------------------------------------------------------
 */

package com.peergreen.jdbcpool;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeSet;
import java.util.logging.Logger;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;
import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.DataSource;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import org.ow2.util.log.Log;
import org.ow2.util.log.LogFactory;

/**
 * DataSource implementation. Manage a pool of connections.
 * @author Philippe Durieux
 * @author Florent Benoit
 */
public class ConnectionManager implements DataSource, XADataSource, Referenceable, ConnectionEventListener {

    /**
     * Logger.
     */
    private final Log logger = LogFactory.getLog(ConnectionManager.class);

    /**
     * Milliseconds.
     */
    private static final long MILLI = 1000L;

    /**
     * One minute in milliseconds.
     */
    private static final long ONE_MIN_MILLI = 60L * MILLI;

    /**
     * Default timeout.
     */
    private static final int DEFAULT_TIMEOUT = 60;

    /**
     * Default timeout for waiters (10s).
     */
    private static final long WAITER_TIMEOUT = 10 * MILLI;

    /**
     * Max waiters (by default).
     */
    private static final int DEFAULT_MAX_WAITERS = 1000;

    /**
     * Default prepare statement.
     */
    private static final int DEFAULT_PSTMT = 12;

    /**
     * Default sampling period.
     */
    private static final int DEFAULT_SAMPLING = 60;


    /**
     * List of all datasources.
     */
    private static Map<String, ConnectionManager> cmList = new HashMap<String, ConnectionManager>();

    /**
     * Transaction manager.
     */
    private TransactionManager tm = null;

    /**
     * List of JManagedConnection not currently used. This avoids closing and
     * reopening physical connections. We try to keep a minimum of minConPool
     * elements here.
     */
    private final TreeSet<JManagedConnection> freeList = new TreeSet<JManagedConnection>();

    /**
     * Total list of JManagedConnection physically opened.
     */
    private final LinkedList<JManagedConnection> mcList = new LinkedList<JManagedConnection>();

    /**
     * This HashMap gives the JManagedConnection from its transaction Requests
     * with same tx get always the same connection.
     */
    private final Map<Transaction, JManagedConnection> tx2mc = new HashMap<Transaction, JManagedConnection>();

    /**
     * Login timeout (DataSource impl).
     */
    private int loginTimeout = DEFAULT_TIMEOUT;

    /**
     * PrintWriter used logging (DataSource impl).
     */
    private PrintWriter log = null;

    /**
     * Constructor for Factory.
     */
    public ConnectionManager() {

    }

    /**
     * Gets the ConnectionManager matching the DataSource name.
     * @param dsname datasource name.
     * @return a connection manager impl.
     */
    public static ConnectionManager getConnectionManager(final String dsname) {
        ConnectionManager cm = cmList.get(dsname);
        return cm;
    }

    /**
     * Datasource name.
     */
    private String dSName = null;

    /**
     * @return Jndi name of the datasource
     */
    public String getDSName() {
        return dSName;
    }

    /**
     * @param s Jndi name for the datasource
     */
    public void setDSName(final String s) {
        dSName = s;
        // Add it to the list
        cmList.put(s, this);
    }

    /**
     * @serial datasource name
     */
    private String dataSourceName;

    /**
     * Gets the name of the datasource.
     * @return the name of the datasource
     */
    public String getDatasourceName() {
        return dataSourceName;
    }

    /**
     * Sets the name of the datasource.
     * @param dataSourceName the name of the datasource
     */
    public void setDatasourceName(final String dataSourceName) {
        this.dataSourceName = dataSourceName;
    }

    /**
     * url for database.
     */
    private String url = null;

    /**
     * @return the url used to get the connection.
     */
    public String getUrl() {
        return url;
    }

    /**
     * Sets the url to get connections.
     * @param url the url for JDBC connections.
     */
    public void setUrl(final String url) {
        this.url = url;
    }

    /**
     * JDBC driver Class.
     */
    private String className = null;

    /**
     * @return the JDBC driver class name.
     */
    public String getClassName() {
        return className;
    }

    /**
     * Sets the driver class for JDBC.
     * @param className the name of the JDBC driver
     * @throws ClassNotFoundException if driver is not found
     */
    public void setClassName(final String className) throws ClassNotFoundException {
        this.className = className;

        // Loads standard JDBC driver and keeps it loaded (via driverClass)
        logger.debug("Load JDBC driver {0}", className);
        try {
            Class.forName(className);
        } catch (java.lang.ClassNotFoundException e) {
            logger.error("Cannot load JDBC driver", e);
            throw e;
        }
    }

    /**
     * default user.
     */
    private String userName = null;

    /**
     * @return the user used for getting connections.
     */
    public String getUserName() {
        return userName;
    }

    /**
     * Sets the user for getting connections.
     * @param userName the name of the user.
     */
    public void setUserName(final String userName) {
        this.userName = userName;
    }

    /**
     * default passwd.
     */
    private String password = null;

    /**
     * @return the password used for connections.
     */
    public String getPassword() {
        return password;
    }

    /**
     * Sets the password used to get connections.
     * @param password the password value.
     */
    public void setPassword(final String password) {
        this.password = password;
    }

    /**
     * Isolation level for JDBC.
     */
    private int isolationLevel = -1;

    /**
     * Isolation level (but String format).
     */
    private String isolationStr = null;

    /**
     * Sets the transaction isolation level of the connections.
     * @param level the level of isolation.
     */
    public void setTransactionIsolation(final String level) {
        if (level.equals("serializable")) {
            isolationLevel = Connection.TRANSACTION_SERIALIZABLE;
        } else if (level.equals("none")) {
            isolationLevel = Connection.TRANSACTION_NONE;
        } else if (level.equals("read_committed")) {
            isolationLevel = Connection.TRANSACTION_READ_COMMITTED;
        } else if (level.equals("read_uncommitted")) {
            isolationLevel = Connection.TRANSACTION_READ_UNCOMMITTED;
        } else if (level.equals("repeatable_read")) {
            isolationLevel = Connection.TRANSACTION_REPEATABLE_READ;
        } else {
            isolationStr = "default";
            return;
        }
        isolationStr = level;
    }

    /**
     * Gets the transaction isolation level.
     * @return transaction isolation level.
     */
    public String getTransactionIsolation() {
        return isolationStr;
    }

    /**
     * count max waiters during current period.
     */
    private int waiterCount = 0;

    /**
     * count max waiting time during current period.
     */
    private long waitingTime = 0;

    /**
     * count max busy connection during current period.
     */
    private int busyMax = 0;

    /**
     * count min busy connection during current period.
     */
    private int busyMin = 0;

    /**
     * High Value for no limit for the connection pool.
     */
    private static final int NO_LIMIT = 99999;

    /**
     * Nb of milliseconds in a day.
     */
    private static final long ONE_DAY = 1440L * 60L * 1000L;

    /**
     * max number of remove at once in the freelist We avoid removing too much
     * mcs at once for perf reasons.
     */
    private static final int MAX_REMOVE_FREELIST = 10;

    /**
     * minimum size of the connection pool.
     */
    private int poolMin = 0;

    /**
     * @return min pool size.
     */
    public int getPoolMin() {
        return poolMin;
    }

    /**
     * @param min minimum connection pool size to be set.
     */
    public synchronized void setPoolMin(final int min) {
        if (poolMin != min) {
            poolMin = min;
            adjust();
        }
    }

    /**
     * maximum size of the connection pool. default value is "NO LIMIT".
     */
    private int poolMax = NO_LIMIT;

    /**
     * @return actual max pool size
     */
    public int getPoolMax() {
        return poolMax;
    }

    /**
     * @param max max pool size. -1 means "no limit".
     */
    public synchronized void setPoolMax(final int max) {
        if (poolMax != max) {
            if (max < 0 || max > NO_LIMIT) {
                if (currentWaiters > 0) {
                    notify();
                }
                poolMax = NO_LIMIT;
            } else {
                if (currentWaiters > 0 && poolMax < max) {
                    notify();
                }
                poolMax = max;
                adjust();
            }
        }
    }

    /**
     * Max age of a Connection in milliseconds. When the time is elapsed, the
     * connection will be closed. This avoids keeping connections open too long
     * for nothing.
     */
    private long maxAge = ONE_DAY;

    /**
     * Same value in mns.
     */
    private int maxAgeMn;

    /**
     * @return max age for connections (in mm).
     */
    public int getMaxAge() {
        return maxAgeMn;
    }

    /**
     * @return max age for connections (in millisec).
     */
    public long getMaxAgeMilli() {
        return maxAge;
    }

    /**
     * @param mn max age of connection in minutes.
     */
    public void setMaxAge(final int mn) {
        maxAgeMn = mn;
        // set times in milliseconds
        maxAge = mn * ONE_MIN_MILLI;
    }

    /**
     * max open time for a connection, in millisec.
     */
    private long maxOpenTime = ONE_DAY;

    /**
     * Same value in mn.
     */
    private int maxOpenTimeMn;

    /**
     * @return max age for connections (in mns).
     */
    public int getMaxOpenTime() {
        return maxOpenTimeMn;
    }

    /**
     * @return max age for connections (in millisecs).
     */
    public long getMaxOpenTimeMilli() {
        return maxOpenTime;
    }

    /**
     * @param mn max time of open connection in minutes.
     */
    public void setMaxOpenTime(final int mn) {
        maxOpenTimeMn = mn;
        // set times in milliseconds
        maxOpenTime = mn * ONE_MIN_MILLI;
    }

    /**
     * max nb of milliseconds to wait for a connection when pool is empty.
     */
    private long waiterTimeout = WAITER_TIMEOUT;

    /**
     * @return waiter timeout in seconds.
     */
    public int getMaxWaitTime() {
        return (int) (waiterTimeout / MILLI);
    }

    /**
     * @param sec max time to wait for a connection, in seconds.
     */
    public void setMaxWaitTime(final int sec) {
        waiterTimeout = sec * MILLI;
    }

    /**
     * max nb of waiters allowed to wait for a Connection.
     */
    private int maxWaiters = DEFAULT_MAX_WAITERS;

    /**
     * @return max nb of waiters
     */
    public int getMaxWaiters() {
        return maxWaiters;
    }

    /**
     * @param nb max nb of waiters
     */
    public void setMaxWaiters(final int nb) {
        maxWaiters = nb;
    }

    /**
     * sampling period in sec.
     */
    private int samplingPeriod = DEFAULT_SAMPLING; // default sampling period

    /**
     * @return sampling period in sec.
     */
    public int getSamplingPeriod() {
        return samplingPeriod;
    }

    /**
     * @param sec sampling period in sec.
     */
    public void setSamplingPeriod(final int sec) {
        if (sec > 0) {
            samplingPeriod = sec;
        }
    }

    /**
     * Level of checking on connections when got from the pool. this avoids
     * reusing bad connections because too old, for example when database was
     * restarted... 0 = no checking 1 = check that still physically opened. 2 =
     * try a null statement.
     */
    private int checkLevel = 0; // default = 0

    /**
     * @return connection checking level
     */
    public int getCheckLevel() {
        return checkLevel;
    }

    /**
     * @param level jdbc connection checking level (0, 1, or 2)
     */
    public void setCheckLevel(final int level) {
        checkLevel = level;
    }

    /**
     * PreparedStatement pool size per managed connection.
     */
    private int pstmtMax = DEFAULT_PSTMT;

    /**
     * @return PreparedStatement cache size.
     */
    public int getPstmtMax() {
        return pstmtMax;
    }

    /**
     * @param nb PreparedStatement cache size.
     */
    public void setPstmtMax(final int nb) {
        pstmtMax = nb;
        // Set the value in each connection.
        for (Iterator i = mcList.iterator(); i.hasNext();) {
            JManagedConnection mc = (JManagedConnection) i.next();
            mc.setPstmtMax(pstmtMax);
        }
    }

    /**
     * test statement used when checkLevel = 2.
     */
    private String testStatement;

    /**
     * @return test statement used when checkLevel = 2.
     */
    public String getTestStatement() {
        return testStatement;
    }

    /**
     * @param s test statement
     */
    public void setTestStatement(final String s) {
        testStatement = s;
    }

    /**
     * Configure the Connection pool. Called by the Container at init.
     * Configuration can be set in datasource.properties files.
     * @param connchecklevel JDBC connection checking level
     * @param connmaxage JDBC connection maximum age
     * @param maxopentime JDBC connection maximum open time
     * @param connteststmt SQL query for test statement
     * @param pstmtmax prepare statement pool size per managed connection
     * @param minconpool Min size for the connection pool
     * @param maxconpool Max size for the connection pool
     * @param maxwaittime Max time to wait for a connection (in seconds)
     * @param maxwaiters Max nb of waiters for a connection
     * @param samplingperiod sampling period in sec.
     */
    @SuppressWarnings("boxing")
    public void poolConfigure(final String connchecklevel, final String connmaxage, final String maxopentime,
            final String connteststmt, final String pstmtmax, final String minconpool, final String maxconpool,
            final String maxwaittime, final String maxwaiters, final String samplingperiod) {

        // Configure pool
        setCheckLevel((new Integer(connchecklevel)).intValue());
        // set con max age BEFORE min/max pool size.
        setMaxAge((new Integer(connmaxage)).intValue());
        setMaxOpenTime((new Integer(maxopentime)).intValue());
        setTestStatement(connteststmt);
        setPstmtMax((new Integer(pstmtmax)).intValue());
        setPoolMin((new Integer(minconpool)).intValue());
        setPoolMax((new Integer(maxconpool)).intValue());
        setMaxWaitTime((new Integer(maxwaittime)).intValue());
        setMaxWaiters((new Integer(maxwaiters)).intValue());
        setSamplingPeriod((new Integer(samplingperiod)).intValue());
        if (logger.isDebugEnabled()) {
            logger.debug("ConnectionManager configured with:");
            logger.debug("   jdbcConnCheckLevel  = {0}", connchecklevel);
            logger.debug("   jdbcConnMaxAge      = {0}", connmaxage);
            logger.debug("   jdbcMaxOpenTime     = {0}", maxopentime);
            logger.debug("   jdbcTestStmt        = {0}", connteststmt);
            logger.debug("   jdbcPstmtMax        = {0}", pstmtmax);
            logger.debug("   minConPool          = {0}", getPoolMin());
            logger.debug("   maxConPool          = {0}", getPoolMax());
            logger.debug("   maxWaitTime         = {0}", getMaxWaitTime());
            logger.debug("   maxWaiters          = {0}", getMaxWaiters());
            logger.debug("   samplingPeriod      = {0}", getSamplingPeriod());
        }
    }

    /**
     * maximum nb of busy connections in last sampling period.
     */
    private int busyMaxRecent = 0;

    /**
     * @return maximum nb of busy connections in last sampling period.
     */
    public int getBusyMaxRecent() {
        return busyMaxRecent;
    }

    /**
     * minimum nb of busy connections in last sampling period.
     */
    private int busyMinRecent = 0;

    /**
     * @return minimum nb of busy connections in last sampling period.
     */
    public int getBusyMinRecent() {
        return busyMinRecent;
    }

    /**
     * nb of threads waiting for a Connection.
     */
    private int currentWaiters = 0;

    /**
     * @return current number of connection waiters.
     */
    public int getCurrentWaiters() {
        return currentWaiters;
    }

    /**
     * total number of opened physical connections since the datasource
     * creation.
     */
    private int openedCount = 0;

    /**
     * @return int number of physical jdbc connection opened.
     */
    public int getOpenedCount() {
        return openedCount;
    }

    /**
     * total nb of physical connection failures.
     */
    private int connectionFailures = 0;

    /**
     * @return int number of xa connection failures on open.
     */
    public int getConnectionFailures() {
        return connectionFailures;
    }

    /**
     * total nb of connection leaks. A connection leak occurs when the caller
     * never issues a close method on the connection.
     */
    private int connectionLeaks = 0;

    /**
     * @return int number of connection leaks.
     */
    public int getConnectionLeaks() {
        return connectionLeaks;
    }

    /**
     * total number of opened connections since the datasource creation.
     */
    private int servedOpen = 0;

    /**
     * @return int number of xa connection served.
     */
    public int getServedOpen() {
        return servedOpen;
    }

    /**
     * total nb of open connection failures because waiter overflow.
     */
    private int rejectedFull = 0;

    /**
     * @return int number of open calls that were rejected due to waiter
     *         overflow.
     */
    public int getRejectedFull() {
        return rejectedFull;
    }

    /**
     * total nb of open connection failures because timeout.
     */
    private int rejectedTimeout = 0;

    /**
     * @return int number of open calls that were rejected by timeout.
     */
    public int getRejectedTimeout() {
        return rejectedTimeout;
    }

    /**
     * total nb of open connection failures for any other reason.
     */
    private int rejectedOther = 0;

    /**
     * @return int number of open calls that were rejected.
     */
    public int getRejectedOther() {
        return rejectedOther;
    }

    /**
     * @return int number of open calls that were rejected.
     */
    public int getRejectedOpen() {
        return rejectedFull + rejectedTimeout + rejectedOther;
    }

    /**
     * maximum nb of waiters since datasource creation.
     */
    private int waitersHigh = 0;

    /**
     * @return maximum nb of waiters since the datasource creation.
     */
    public int getWaitersHigh() {
        return waitersHigh;
    }

    /**
     * maximum nb of waiters in last sampling period.
     */
    private int waitersHighRecent = 0;

    /**
     * @return maximum nb of waiters in last sampling period.
     */
    public int getWaitersHighRecent() {
        return waitersHighRecent;
    }

    /**
     * total nb of waiters since datasource creation.
     */
    private int totalWaiterCount = 0;

    /**
     * @return total nb of waiters since the datasource creation.
     */
    public int getWaiterCount() {
        return totalWaiterCount;
    }

    /**
     * total waiting time in milliseconds.
     */
    private long totalWaitingTime = 0;

    /**
     * @return total waiting time since the datasource creation.
     */
    public long getWaitingTime() {
        return totalWaitingTime;
    }

    /**
     * max waiting time in milliseconds.
     */
    private long waitingHigh = 0;

    /**
     * @return max waiting time since the datasource creation.
     */
    public long getWaitingHigh() {
        return waitingHigh;
    }

    /**
     * max waiting time in milliseconds in last sampling period.
     */
    private long waitingHighRecent = 0;

    /**
     * @return max waiting time in last sampling period.
     */
    public long getWaitingHighRecent() {
        return waitingHighRecent;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getLoginTimeout() throws SQLException {
        return loginTimeout;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setLoginTimeout(final int seconds) throws SQLException {
        loginTimeout = seconds;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return log;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setLogWriter(final PrintWriter out) throws SQLException {
        log = out;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Connection getConnection() throws SQLException {
        return getConnection(userName, password);
    }

    /**
     * Attempts to establish a connection with the data source that this
     * DataSource object represents. - comes from the javax.sql.DataSource
     * interface
     * @param username - the database user on whose behalf the connection is
     *        being made
     * @param password - the user's password
     * @return a connection to the data source
     * @throws SQLException - if a database access error occurs
     */
    @Override
    public Connection getConnection(final String username, final String password) throws SQLException {
        JManagedConnection mc = null;

        // Get the current Transaction
        Transaction tx = null;
        try {
            tx = tm.getTransaction();
        } catch (NullPointerException n) {
            // current is null: we are not in EasyBeans Server.
            logger.error("ConnectionManager: should not be used outside a EasyBeans Server");
        } catch (SystemException e) {
            logger.error("ConnectionManager: getTransaction failed", e);
        }
        logger.debug("Tx = {0}", tx);

        // Get a JManagedConnection in the pool for this user
        mc = openConnection(username, tx);
        Connection ret = mc.getConnection();

        // Enlist XAResource if we are actually in a transaction
        if (tx != null) {
            if (mc.getOpenCount() == 1) { // Only if first/only thread
                try {
                    logger.debug("enlist XAResource on {0}", tx);
                    tx.enlistResource(mc.getXAResource());
                    ret.setAutoCommit(false);
                } catch (RollbackException e) {
                    // Although tx has been marked to be rolled back,
                    // XAResource has been correctly enlisted.
                    logger.warn("XAResource enlisted, but tx is marked rollback", e);
                } catch (IllegalStateException e) {
                    // In case tx is committed, no need to register resource!
                    ret.setAutoCommit(true);
                } catch (Exception e) {
                    logger.error("Cannot enlist XAResource", e);
                    logger.error("Connection will not be enlisted in a transaction");
                    // should return connection in the pool XXX
                    throw new SQLException("Cannot enlist XAResource");
                }
            }
        } else {
            ret.setAutoCommit(true); // in case we do not start a Tx
        }

        // return a Connection object
        return ret;
    }

    /**
     * Attempts to establish a physical database connection that can be
     * used in a distributed transaction.
     *
     * @return  an <code>XAConnection</code> object, which represents a
     *          physical connection to a data source, that can be used in
     *          a distributed transaction
     * @exception SQLException if a database access error occurs
     */
    @Override
    public XAConnection getXAConnection() throws SQLException {
        return getXAConnection(userName, password);
    }

    /**
     * Attempts to establish a physical database connection, using the given
     * user name and password. The connection that is returned is one that can
     * be used in a distributed transaction - comes from the
     * javax.sql.XADataSource interface
     * @param user - the database user on whose behalf the connection is being
     *        made
     * @param passwd - the user's password
     * @return an XAConnection object, which represents a physical connection to
     *         a data source, that can be used in a distributed transaction
     * @throws SQLException - if a database access error occurs
     */
    @Override
    @SuppressWarnings("boxing")
    public XAConnection getXAConnection(final String user, final String passwd) throws SQLException {
        // Create the actual connection in the std driver
        Connection conn = null;
        try {
            if (user.length() == 0) {
                conn = DriverManager.getConnection(url);
                logger.debug("    * New Connection on {0}", url);
            } else {
                // Accept password of zero length.
                conn = DriverManager.getConnection(url, user, passwd);
                logger.debug("    * New Connection on {0} for user {1}", url, user);
            }
        } catch (SQLException e) {
            logger.error("Could not get Connection on {0}", url, e);
            throw new SQLException("Could not get Connection on url : " + url + " for user : " + user + " inner exception"
                    + e.getMessage());
        }

        // Attempt to set the transaction isolation level
        // Depending on the underlaying database, this may not succeed.
        if (isolationLevel != -1) {
            try {
                logger.debug("set transaction isolation to {0}", isolationLevel);
                conn.setTransactionIsolation(isolationLevel);
            } catch (SQLException e) {
                String ilstr = "?";
                switch (isolationLevel) {
                    case Connection.TRANSACTION_SERIALIZABLE:
                        ilstr = "SERIALIZABLE";
                        break;
                    case Connection.TRANSACTION_NONE:
                        ilstr = "NONE";
                        break;
                    case Connection.TRANSACTION_READ_COMMITTED:
                        ilstr = "READ_COMMITTED";
                        break;
                    case Connection.TRANSACTION_READ_UNCOMMITTED:
                        ilstr = "READ_UNCOMMITTED";
                        break;
                    case Connection.TRANSACTION_REPEATABLE_READ:
                        ilstr = "REPEATABLE_READ";
                        break;
                    default:
                        throw new SQLException("Invalid isolation level '" + ilstr + "'.");
                }
                logger.error("Cannot set transaction isolation to {0} for this DataSource url {1}", ilstr, url, e);
                isolationLevel = -1;
            }
        }

        // Create the JManagedConnection object
        JManagedConnection mc = new JManagedConnection(conn, this);

        // return the XAConnection
        return mc;
    }

    // -----------------------------------------------------------------
    // Referenceable Implementation
    // -----------------------------------------------------------------

    /**
     * Retrieves the Reference of this object. Used at binding time by JNDI to
     * build a reference on this object.
     * @return The non-null Reference of this object.
     * @exception NamingException If a naming exception was encountered while
     *            retrieving the reference.
     */
    @Override
    public Reference getReference() throws NamingException {

        Reference ref = new Reference(this.getClass().getName(), DataSourceFactory.class.getName(), null);
        // These values are used by ObjectFactory (see DataSourceFactory.java)
        ref.add(new StringRefAddr("datasource.name", getDSName()));
        ref.add(new StringRefAddr("datasource.url", getUrl()));
        ref.add(new StringRefAddr("datasource.classname", getClassName()));
        ref.add(new StringRefAddr("datasource.username", getUserName()));
        ref.add(new StringRefAddr("datasource.password", getPassword()));
        ref.add(new StringRefAddr("datasource.isolationlevel", getTransactionIsolation()));
        Integer checklevel = new Integer(getCheckLevel());
        ref.add(new StringRefAddr("connchecklevel", checklevel.toString()));
        Integer maxage = new Integer(getMaxAge());
        ref.add(new StringRefAddr("connmaxage", maxage.toString()));
        Integer maxopentime = new Integer(getMaxOpenTime());
        ref.add(new StringRefAddr("maxopentime", maxopentime.toString()));
        ref.add(new StringRefAddr("connteststmt", getTestStatement()));
        Integer pstmtmax = new Integer(getPstmtMax());
        ref.add(new StringRefAddr("pstmtmax", pstmtmax.toString()));
        Integer minpool = new Integer(getPoolMin());
        ref.add(new StringRefAddr("minconpool", minpool.toString()));
        Integer maxpool = new Integer(getPoolMax());
        ref.add(new StringRefAddr("maxconpool", maxpool.toString()));
        Integer maxwaittime = new Integer(getMaxWaitTime());
        ref.add(new StringRefAddr("maxwaittime", maxwaittime.toString()));
        Integer maxwaiters = new Integer(getMaxWaiters());
        ref.add(new StringRefAddr("maxwaiters", maxwaiters.toString()));
        Integer samplingperiod = new Integer(getSamplingPeriod());
        ref.add(new StringRefAddr("samplingperiod", samplingperiod.toString()));
        return ref;
    }

    /**
     * Notifies this <code>ConnectionEventListener</code> that
     * the application has called the method <code>close</code> on its
     * representation of a pooled connection.
     *
     * @param event an event object describing the source of
     * the event
     */
    @Override
    public void connectionClosed(final ConnectionEvent event) {
        JManagedConnection mc = (JManagedConnection) event.getSource();
        closeConnection(mc, XAResource.TMSUCCESS);
    }

    /**
     * Notifies this <code>ConnectionEventListener</code> that
     * a fatal error has occurred and the pooled connection can
     * no longer be used.  The driver makes this notification just
     * before it throws the application the <code>SQLException</code>
     * contained in the given <code>ConnectionEvent</code> object.
     *
     * @param event an event object describing the source of
     * the event and containing the <code>SQLException</code> that the
     * driver is about to throw
     */
    @Override
    @SuppressWarnings("boxing")
    public void connectionErrorOccurred(final ConnectionEvent event) {

        JManagedConnection mc = (JManagedConnection) event.getSource();
        logger.debug("mc= {0}", mc.getIdentifier());

        // remove it from the list of open connections for this thread
        // only if it was opened outside a tx.
        closeConnection(mc, XAResource.TMFAIL);
    }

    /**
     * @return int number of xa connection
     */
    public int getCurrentOpened() {
        return mcList.size();
    }

    /**
     * @return int number of busy xa connection.
     */
    public int getCurrentBusy() {
        return mcList.size() - freeList.size();
    }

    /**
     * compute current min/max busyConnections.
     */
    public void recomputeBusy() {
        int busy = getCurrentBusy();
        if (busyMax < busy) {
            busyMax = busy;
        }
        if (busyMin > busy) {
            busyMin = busy;
        }
    }

    /**
     * @return int number of xa connection reserved for tx.
     */
    public int getCurrentInTx() {
        return tx2mc.size();
    }

    /**
     * make samples with some monitoring values.
     */
    public synchronized void sampling() {
        waitingHighRecent = waitingTime;
        if (waitingHigh < waitingTime) {
            waitingHigh = waitingTime;
        }
        waitingTime = 0;

        waitersHighRecent = waiterCount;
        if (waitersHigh < waiterCount) {
            waitersHigh = waiterCount;
        }
        waiterCount = 0;

        busyMaxRecent = busyMax;
        busyMax = getCurrentBusy();
        busyMinRecent = busyMin;
        busyMin = getCurrentBusy();
    }

    /**
     * Adjust the pool size, according to poolMax and poolMin values. Also
     * remove old connections in the freeList.
     */
    @SuppressWarnings("boxing")
    public synchronized void adjust() {
        logger.debug(dSName);

        // Remove max aged elements in freelist
        // - Not more than MAX_REMOVE_FREELIST
        // - Don't reduce pool size less than poolMin
        int count = mcList.size() - poolMin;
        // In case count is null, a new connection will be
        // recreated just after
        if (count >= 0) {
            if (count > MAX_REMOVE_FREELIST) {
                count = MAX_REMOVE_FREELIST;
            }
            for (Iterator i = freeList.iterator(); i.hasNext();) {
                JManagedConnection mc = (JManagedConnection) i.next();
                if (mc.isAged()) {
                    logger.debug("remove a timed out connection");
                    i.remove();
                    destroyItem(mc);
                    count--;
                    if (count <= 0) {
                        break;
                    }
                }
            }
        }
        recomputeBusy();

        // Close (physically) connections lost (opened for too long time)
        for (Iterator i = mcList.iterator(); i.hasNext();) {
            JManagedConnection mc = (JManagedConnection) i.next();
            if (mc.inactive()) {
                if (logger.isWarnEnabled()) {
                    logger.warn("close a timed out open connection {0}", mc.getIdentifier());
                }
                i.remove();
                // destroy mc
                mc.remove();
                connectionLeaks++;
                // Notify 1 thread waiting for a Connection.
                if (currentWaiters > 0) {
                    notify();
                }
            }
        }

        // Shrink the pool in case of max pool size
        // This occurs when max pool size has been reduced by admin console.
        if (poolMax != NO_LIMIT) {
            while (freeList.size() > poolMin && mcList.size() > poolMax) {
                JManagedConnection mc = freeList.first();
                freeList.remove(mc);
                destroyItem(mc);
            }
        }
        recomputeBusy();

        // Recreate more Connections while poolMin is not reached
        while (mcList.size() < poolMin) {
            JManagedConnection mc = null;
            try {
                mc = (JManagedConnection) getXAConnection();
                openedCount++;
            } catch (SQLException e) {
                throw new IllegalStateException("Could not create " + poolMin + " mcs in the pool : ", e);
            }
            // tx = null. Assumes maxage already configured.
            freeList.add(mc);
            mcList.add(mc);
            mc.addConnectionEventListener(this);
        }
    }

    /**
     * Lookup connection in the pool for this user/tx.
     * @param user user name
     * @param tx Transaction the connection is involved
     * @return a free JManagedConnection (never null)
     * @throws SQLException Cannot open a connection because the pool's max size
     *         is reached
     */
    @SuppressWarnings("boxing")
    public synchronized JManagedConnection openConnection(final String user, final Transaction tx) throws SQLException {
        JManagedConnection mc = null;
        // If a Connection exists already for this tx, just return it.
        // If no transaction, never reuse a connection already used.
        if (tx != null) {
            mc = tx2mc.get(tx);
            if (mc != null) {
                logger.debug("Reuse a Connection for same tx");
                mc.hold();
                servedOpen++;
                return mc;
            }
        }
        // Loop until a valid mc is found
        long timetowait = waiterTimeout;
        long starttime = 0;
        while (mc == null) {
            // try to find an mc in the free list
            if (freeList.isEmpty()) {
                // In case we have reached the maximum limit of the pool,
                // we must wait until a connection is released.
                if (mcList.size() >= poolMax) {
                    boolean stoplooping = true;
                    // If a timeout has been specified, wait, unless maxWaiters
                    // is reached.
                    if (timetowait > 0) {
                        if (currentWaiters < maxWaiters) {
                            currentWaiters++;
                            // Store the maximum concurrent waiters
                            if (waiterCount < currentWaiters) {
                                waiterCount = currentWaiters;
                            }
                            if (starttime == 0) {
                                starttime = System.currentTimeMillis();
                                logger.debug("Wait for a free Connection, {0}", mcList.size());
                            }
                            try {
                                wait(timetowait);
                            } catch (InterruptedException ign) {
                                logger.warn("Interrupted");
                            } finally {
                                currentWaiters--;
                            }
                            long stoptime = System.currentTimeMillis();
                            long stillwaited = stoptime - starttime;
                            timetowait = waiterTimeout - stillwaited;
                            stoplooping = (timetowait <= 0);
                            if (stoplooping) {
                                // We have been waked up by the timeout.
                                totalWaiterCount++;
                                totalWaitingTime += stillwaited;
                                if (waitingTime < stillwaited) {
                                    waitingTime = stillwaited;
                                }
                            } else {
                                if (!freeList.isEmpty() || mcList.size() < poolMax) {
                                    // We have been notified by a connection
                                    // released.
                                    logger.debug("Notified after {0}", stillwaited);
                                    totalWaiterCount++;
                                    totalWaitingTime += stillwaited;
                                    if (waitingTime < stillwaited) {
                                        waitingTime = stillwaited;
                                    }
                                }
                                continue;
                            }
                        }
                    }
                    if (stoplooping && freeList.isEmpty() && mcList.size() >= poolMax) {
                        if (starttime > 0) {
                            rejectedTimeout++;
                            logger.warn("Cannot create a Connection - timeout");
                        } else {
                            rejectedFull++;
                            logger.warn("Cannot create a Connection");
                        }
                        throw new SQLException("No more connections in " + getDatasourceName());
                    }
                    continue;
                }
                logger.debug("empty free list: Create a new Connection");
                try {
                    // create a new XA Connection
                    mc = (JManagedConnection) getXAConnection();
                    openedCount++;
                } catch (SQLException e) {
                    connectionFailures++;
                    rejectedOther++;
                    logger.warn("Cannot create new Connection for tx", e);
                    throw e;
                }
                // Register the connection manager as a ConnectionEventListener
                mc.addConnectionEventListener(this);
                mcList.add(mc);
            } else {
                mc = freeList.last();
                freeList.remove(mc);
                // Check the connection before reusing it
                if (checkLevel > 0) {
                    try {
                        JConnection conn = (JConnection) mc.getConnection();
                        if (conn.isPhysicallyClosed()) {
                            logger.warn("The JDBC connection has been closed!");
                            destroyItem(mc);
                            starttime = 0;
                            mc = null;
                            continue;
                        }
                        if (checkLevel > 1) {
                            java.sql.Statement stmt = conn.createStatement();
                            stmt.execute(testStatement);
                            stmt.close();
                        }
                    } catch (Exception e) {
                        logger.error("DataSource " + getDatasourceName() + " error: removing invalid mc", e);
                        destroyItem(mc);
                        starttime = 0;
                        mc = null;
                        continue;
                    }
                }
            }
        }
        recomputeBusy();
        mc.setTx(tx);
        if (tx == null) {
            logger.debug("Got a Connection - no TX: ");
        } else {
            logger.debug("Got a Connection for TX: ");
            // register synchronization
            try {
                tx.registerSynchronization(mc);
                tx2mc.put(tx, mc); // only if registerSynchronization was OK.
            } catch (javax.transaction.RollbackException e) {
                // / optimization is probably possible at this point
                logger.warn("DataSource " + getDatasourceName() + " error: Pool mc registered, but tx is rollback only", e);
            } catch (javax.transaction.SystemException e) {
                logger.error("DataSource " + getDatasourceName() + " error in pool: system exception from transaction manager ",
                        e);
            } catch (IllegalStateException e) {
                // In case transaction has already committed, do as if no tx.
                logger.warn("Got a Connection - committed TX: ", e);
                mc.setTx(null);
            }
        }
        mc.hold();
        servedOpen++;
        return mc;
    }

    /**
     * The transaction has committed (or rolled back). We can return its
     * connections to the pool of available connections.
     * @param tx the non null transaction
     */
    public synchronized void freeConnections(final Transaction tx) {
        logger.debug("free connection for Tx = " + tx);
        JManagedConnection mc = tx2mc.remove(tx);
        if (mc == null) {
            logger.error("pool: no connection found to free for Tx = " + tx);
            return;
        }
        mc.setTx(null);
        if (mc.isOpen()) {
            // Connection not yet closed (but committed).
            logger.debug("Connection not closed by caller");
            return;
        }
        freeItem(mc);
    }

    /**
     * Close all connections in the pool, when server is shut down.
     */
    public synchronized void closeAllConnection() {
        // Close physically all connections
        Iterator it = mcList.iterator();
        try {
            while (it.hasNext()) {
                JManagedConnection mc = (JManagedConnection) it.next();
                mc.close();
            }
        } catch (java.sql.SQLException e) {
            logger.error("Error while closing a Connection:", e);
        }
    }

    // -----------------------------------------------------------------------
    // private methods
    // -----------------------------------------------------------------------

    /**
     * Mark a specific Connection in the pool as closed. If it is no longer
     * associated to a Tx, we can free it.
     * @param mc XAConnection being closed
     * @param flag TMSUCCESS (normal close) or TMFAIL (error) or null if error.
     * @return false if has not be closed (still in use)
     */
    private boolean closeConnection(final JManagedConnection mc, final int flag) {
        // The connection will be available only if not associated
        // to a transaction. Else, it will be reusable only for the
        // same transaction.
        if (!mc.release()) {
            return false;
        }
        if (mc.getTx() != null) {
            logger.debug("keep connection for same tx");
        } else {
            freeItem(mc);
        }

        // delist Resource if in transaction
        Transaction tx = null;
        try {
            tx = tm.getTransaction();
        } catch (NullPointerException n) {
            // current is null: we are not in EasyBeans Server.
            logger.error("Pool: should not be used outside a EasyBeans Server", n);
        } catch (SystemException e) {
            logger.error("Pool: getTransaction failed:", e);
        }
        if (tx != null && mc.isClosed()) {
            try {
                tx.delistResource(mc.getXAResource(), flag);
            } catch (Exception e) {
                logger.error("Pool: Exception while delisting resource:", e);
            }
        }
        return true;
    }

    /**
     * Free item and return it in the free list.
     * @param item The item to be freed
     */
    @SuppressWarnings("boxing")
    private synchronized void freeItem(final JManagedConnection item) {
        // Add it to the free list
        // Even if maxage is reached, because we avoids going under min pool
        // size.
        // PoolKeeper will manage aged connections.
        freeList.add(item);
        if (logger.isDebugEnabled()) {
            logger.debug("item added to freeList: " + item.getIdentifier());
        }

        // Notify 1 thread waiting for a Connection.
        if (currentWaiters > 0) {
            notify();
        }
        recomputeBusy();
    }

    /**
     * Destroy an mc because connection closed or error occured.
     * @param mc The mc to be destroyed
     */
    private synchronized void destroyItem(final JManagedConnection mc) {
        mcList.remove(mc);
        mc.remove();
        // Notify 1 thread waiting for a Connection.
        if (currentWaiters > 0) {
            notify();
        }
        recomputeBusy();
    }

    /**
     * Check on a connection the test statement.
     * @param testStatement the statement to use for test
     * @return the test statement if the test succeeded, an error message
     *         otherwise
     * @throws SQLException If an error occured when trying to test (not due to
     *         the test itself, but to other preliminary or post operation).
     */
    public String checkConnection(final String testStatement) throws SQLException {
        String noError = testStatement;
        JManagedConnection mc = null;
        boolean jmcCreated = false;
        if (!freeList.isEmpty()) {
            // find a connection to test in the freeList
            Iterator it = freeList.iterator();
            while (it.hasNext()) {
                mc = (JManagedConnection) it.next();
                try {
                    JConnection conn = (JConnection) mc.getConnection();
                    if (!conn.isPhysicallyClosed()) {
                        // ok, we found a connection we can use to test
                        logger.debug("Use a free JManagedConnection to test with " + testStatement);
                        break;
                    }
                    mc = null;
                } catch (SQLException e) {
                    // Can't use this connection to test
                    mc = null;
                }
            }
        }
        if (mc == null) {
            // try to create mc Connection
            logger.debug("Create a JManagedConnection to test with " + testStatement);
            Connection conn = null;
            try {
                conn = DriverManager.getConnection(url, userName, password);
            } catch (SQLException e) {
                logger.error("Could not get Connection on " + url + ":", e);
            }
            mc = new JManagedConnection(conn, this);
            jmcCreated = true;
        }
        if (mc != null) {
            // Do the test on a the free connection or the created connection
            JConnection conn = (JConnection) mc.getConnection();
            java.sql.Statement stmt = conn.createStatement();
            try {
                stmt.execute(testStatement);
            } catch (SQLException e) {
                // The test fails
                return e.getMessage();
            }
            stmt.close();
            if (jmcCreated) {
                mc.close();
            }
        }
        return noError;
    }

    /**
     * Sets the transaction managed used by the connections.
     * @param tm the transaction manager.
     */
    protected void setTm(final TransactionManager tm) {
        this.tm = tm;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

}