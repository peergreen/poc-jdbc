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
 * $Id: JManagedConnection.java 5369 2010-02-24 14:58:19Z benoitf $
 * --------------------------------------------------------------------------
 */

package com.peergreen.jdbcpool;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.StatementEventListener;
import javax.sql.XAConnection;
import javax.transaction.Synchronization;
import javax.transaction.Transaction;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.ow2.util.log.Log;
import org.ow2.util.log.LogFactory;

/**
 * This class represents the connection managed by the pool. This connection is
 * a managed connection and is notified of the transaction events.
 * @author Philippe Durieux
 * @author Florent Benoit
 */
public class JManagedConnection implements Comparable, XAConnection, XAResource, Synchronization {

    /**
     * Logger.
     */
    private static Log logger = LogFactory.getLog(JManagedConnection.class);

    /**
     * Connection to the database.
     */
    private Connection physicalConnection = null;

    /**
     * Connection returned to the user.
     */
    private Connection implConn = null;

    /**
     * Maximum of prepared statements.
     */
    private int pstmtmax = 0;

    /**
     * Current number of opened prepared statements.
     */
    private int psOpenNb = 0;

    /**
     * Event listeners (of PooledConnection).
     */
    private final Vector<ConnectionEventListener> eventListeners = new Vector<ConnectionEventListener>();

    /**
     * count of opening this connection. >0 if open.
     */
    private int open = 0;

    /**
     * Transaction timeout value.
     */
    private int timeout = 0;

    /**
     * Transaction the connection is involved with.
     */
    private Transaction tx = null;

    /**
     * Counter of all managed connections created.
     */
    private static int objcount = 0;

    /**
     * Identifier of this connection.
     */
    private final int identifier;

    /**
     * Prepared statements that were reused.
     */
    private int reUsedPreparedStatements = 0;

    /**
     * List of PreparedStatement in the pool.
     */
    private Map<String, JStatement> psList = null;

    /**
     * Link to the connection manager.
     */
    private ConnectionManager ds = null;

    /**
     * Time of the death for this connection.
     */
    private long deathTime = 0;

    /**
     * Time for closing this connection.
     */
    private long closeTime = 0;


    /**
     * Builds a new managed connection on a JDBC connection.
     * @param physicalConnection the physical JDBC Connection.
     * @param ds the connection manager
     */
    public JManagedConnection(final Connection physicalConnection, final ConnectionManager ds) {
        this.physicalConnection = physicalConnection;
        this.ds = ds;

        // An XAConnection holds 2 objects: 1 Connection + 1 XAResource
        this.implConn = new JConnection(this, physicalConnection);

        open = 0;
        deathTime = System.currentTimeMillis() + ds.getMaxAgeMilli();

        identifier = objcount++;

        // Prepared statement.
        pstmtmax = ds.getPstmtMax();
        psOpenNb = 0;
        psList = Collections.synchronizedMap(new HashMap<String, JStatement>());

    }

    /**
     * @return The identifier of this JManagedConnection
     */
    public int getIdentifier() {
        return identifier;
    }

    /**
     * Dynamically change the prepared statement pool size.
     * @param max the maximum of prepared statement.
     */
    public void setPstmtMax(final int max) {
        pstmtmax = max;
        if (psList == null) {
            psList = Collections.synchronizedMap(new HashMap<String, JStatement>(pstmtmax));
        }
    }

    /**
     * Commit the global transaction specified by xid.
     * @param xid transaction xid
     * @param onePhase true if one phase commit
     * @throws XAException XA protocol error
     */
    @Override
    public void commit(final Xid xid, final boolean onePhase) throws XAException {
        logger.debug("XA-COMMIT for {0}", xid);

        // Commit the transaction
        try {
            physicalConnection.commit();
        } catch (SQLException e) {
            logger.error("Cannot commit transaction", e);
            notifyError(e);
            throw new XAException("Error on commit");
        }
    }

    /**
     * Ends the work performed on behalf of a transaction branch.
     * @param xid transaction xid
     * @param flags currently unused
     * @throws XAException XA protocol error
     */
    @Override
    public void end(final Xid xid, final int flags) throws XAException {
        logger.debug("XA-END for {0}", xid);
    }

    /**
     * Tell the resource manager to forget about a heuristically completed
     * transaction branch.
     * @param xid transaction xid
     * @throws XAException XA protocol error
     */
    @Override
    public void forget(final Xid xid) throws XAException {
        logger.debug("XA-FORGET for {0}", xid);
    }

    /**
     * Obtain the current transaction timeout value set for this XAResource
     * instance.
     * @return the current transaction timeout in seconds
     * @throws XAException XA protocol error
     */
    @Override
    public int getTransactionTimeout() throws XAException {
        logger.debug("getTransactionTimeout for {0}", this);
        return timeout;
    }

    /**
     * Determine if the resource manager instance represented by the target
     * object is the same as the resource manager instance represented by the
     * parameter xares.
     * @param xares An XAResource object
     * @return True if same RM instance, otherwise false.
     * @throws XAException XA protocol error
     */
    @Override
    public boolean isSameRM(final XAResource xares) throws XAException {

        // In this pseudo-driver, we must return true only if
        // both objects refer to the same XAResource, and not
        // the same Resource Manager, because actually, we must
        // send commit/rollback on each XAResource involved in
        // the transaction.
        if (xares.equals(this)) {
            logger.debug("isSameRM = true {0}", this);
            return true;
        }
        logger.debug("isSameRM = false {0}", this);
        return false;
    }

    /**
     * Ask the resource manager to prepare for a transaction commit of the
     * transaction specified in xid.
     * @param xid transaction xid
     * @throws XAException XA protocol error
     * @return always OK
     */
    @Override
    public int prepare(final Xid xid) throws XAException {
        logger.debug("XA-PREPARE for {0}", xid);
        // No 2PC on standard JDBC drivers
        return XA_OK;
    }

    /**
     * Obtain a list of prepared transaction branches from a resource manager.
     * @param flag unused parameter.
     * @return an array of transaction Xids
     * @throws XAException XA protocol error
     */
    @Override
    public Xid[] recover(final int flag) throws XAException {
        logger.debug("XA-RECOVER for {0}", this);
        // Not implemented
        return null;
    }

    /**
     * Inform the resource manager to roll back work done on behalf of a
     * transaction branch.
     * @param xid transaction xid
     * @throws XAException XA protocol error
     */
    @Override
    public void rollback(final Xid xid) throws XAException {
        logger.debug("XA-ROLLBACK for {0}", xid);

        // Make sure that we are not in AutoCommit mode
        try {
            if (physicalConnection.getAutoCommit()) {
                logger.error("Rollback called on XAResource with AutoCommit set");
                throw (new XAException(XAException.XA_HEURCOM));
            }
        } catch (SQLException e) {
            logger.error("Cannot getAutoCommit", e);
            notifyError(e);
            throw (new XAException("Error on getAutoCommit"));
        }

        // Rollback the transaction
        try {
            physicalConnection.rollback();
        } catch (SQLException e) {
            logger.error("Cannot rollback transaction", e);
            notifyError(e);
            throw (new XAException("Error on rollback"));
        }
    }

    /**
     * Set the current transaction timeout value for this XAResource instance.
     * @param seconds timeout value, in seconds.
     * @return always true
     * @throws XAException XA protocol error
     */
    @Override
    @SuppressWarnings("boxing")
    public boolean setTransactionTimeout(final int seconds) throws XAException {
        logger.debug("setTransactionTimeout to {0} for {1}", seconds, this);
        timeout = seconds;
        return true;
    }

    /**
     * Start work on behalf of a transaction branch specified in xid.
     * @param xid transaction xid
     * @param flags unused parameter
     * @throws XAException XA protocol error
     */
    @Override
    public void start(final Xid xid, final int flags) throws XAException {
        logger.debug("XA-START for {0}", xid);
    }

    /**
     * Return an XA resource to the caller.
     * @return The XAResource
     * @exception SQLException - if a database-access error occurs
     */
    @Override
    public XAResource getXAResource() throws SQLException {
        return this;
    }

    /**
     * Compares this object with another specified object.
     * @param o the object to compare
     * @return a value detecting if these objects are matching or not.
     */
    @Override
    public int compareTo(final Object o) {
        JManagedConnection other = (JManagedConnection) o;
        int diff = getReUsedPreparedStatements() - other.getReUsedPreparedStatements();
        if (diff == 0) {
            return getIdentifier() - other.getIdentifier();
        }
        return diff;
    }

    /**
     * @return value of reused prepared statement.
     */
    public int getReUsedPreparedStatements() {
        return reUsedPreparedStatements;
    }

    /**
     * Create an object handle for a database connection.
     * @exception SQLException - if a database-access error occurs
     * @return connection used by this managed connection
     */
    @Override
    public Connection getConnection() throws SQLException {
        // Just return the already created object.
        return implConn;
    }

    /**
     * Close the database connection.
     * @exception SQLException - if a database-access error occurs
     */
    @Override
    public void close() throws SQLException {

        // Close the actual Connection here.
        if (physicalConnection != null) {
            physicalConnection.close();
        } else {
            logger.error("Connection already closed. Stack of this new close()", new Exception());
        }
        physicalConnection = null;
        implConn = null;
    }

    /**
     * Add an event listener.
     * @param listener event listener
     */
    @Override
    public void addConnectionEventListener(final ConnectionEventListener listener) {
        eventListeners.addElement(listener);
    }

    /**
     * Remove an event listener.
     * @param listener event listener
     */
    @Override
    public void removeConnectionEventListener(final ConnectionEventListener listener) {
        eventListeners.removeElement(listener);
    }



    /**
     * synchronization implementation. {@inheritDoc}
     */
    @Override
    public void beforeCompletion() {
        // nothing to do
    }

    /**
     * synchronization implementation. {@inheritDoc}
     */
    @Override
    public void afterCompletion(final int status) {
        if (tx != null) {
            ds.freeConnections(tx);
        } else {
            logger.error("NO TX!");
        }
    }

    /**
     * @return true if connection max age has expired
     */
    public boolean isAged() {
        return (deathTime < System.currentTimeMillis());
    }

    /**
     * @return true if connection is still open
     */
    public boolean isOpen() {
        return (open > 0);
    }

    /**
     * @return open count
     */
    public int getOpenCount() {
        return open;
    }

    /**
     * Check if the connection has been unused for too long time. This occurs
     * usually when the caller forgot to call close().
     * @return true if open time has been reached, and not involved in a tx.
     */
    public boolean inactive() {
        return (open > 0 && tx == null && closeTime < System.currentTimeMillis());
    }

    /**
     * @return true if connection is closed
     */
    public boolean isClosed() {
        return (open <= 0);
    }

    /**
     * Notify as opened.
     */
    public void hold() {
        open++;
        closeTime = System.currentTimeMillis() + ds.getMaxOpenTimeMilli();
    }

    /**
     * notify as closed.
     * @return true if normal close.
     */
    public boolean release() {
        open--;
        if (open < 0) {
            logger.warn("connection was already closed");
            open = 0;
            return false;
        }
        if (tx == null && open > 0) {
            logger.error("connection-open counter overflow");
            open = 0;
        }
        return true;
    }

    /**
     * Set the associated transaction.
     * @param tx Transaction
     */
    public void setTx(final Transaction tx) {
        this.tx = tx;
    }

    /**
     * @return the Transaction
     */
    public Transaction getTx() {
        return tx;
    }

    /**
     * remove this item, ignoring exception on close.
     */
    public void remove() {
        // Close the physical connection
        try {
            close();
        } catch (java.sql.SQLException ign) {
            logger.error("Could not close Connection: ", ign);
        }

        // remove all references (for GC)
        tx = null;

    }

    // -----------------------------------------------------------------
    // Other methods
    // -----------------------------------------------------------------

    /**
     * Try to find a PreparedStatement in the pool for the given options.
     * @param sql the sql of the prepared statement
     * @param resultSetType the type of resultset
     * @param resultSetConcurrency the concurrency of this resultset
     * @return a preparestatement object
     * @throws SQLException if an errors occurs on the database.
     */
    public PreparedStatement prepareStatement(final String sql, final int resultSetType, final int resultSetConcurrency)
            throws SQLException {

        logger.debug("sql = {0}", sql);
        if (pstmtmax == 0) {
            return physicalConnection.prepareStatement(sql, resultSetType, resultSetConcurrency);
        }
        JStatement ps = null;
        synchronized (psList) {
            ps = psList.get(sql);
            if (ps != null) {
                if (!ps.isClosed()) {
                    logger.warn("reuse an open pstmt");
                }
                ps.reuse();
                reUsedPreparedStatements++;
            } else {
                // Not found in cache. Create a new one.
                PreparedStatement aps = physicalConnection.prepareStatement(sql, resultSetType, resultSetConcurrency);
                ps = new JStatement(aps, this, sql);
                psList.put(sql, ps);
            }
            psOpenNb++;
        }
        return ps;
    }

    /**
     * Try to find a PreparedStatement in the pool.
     * @param sql the given sql query.
     * @throws SQLException if an error in the database occurs.
     * @return a given prepared statement.
     */
    public PreparedStatement prepareStatement(final String sql) throws SQLException {
        return prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    /**
     * A PreparedStatement has been logically closed.
     * @param ps a prepared statement.
     */
    public void notifyPsClose(final JStatement ps) {
        logger.debug(ps.getSql());
        synchronized (psList) {
            psOpenNb--;
            if (psList.size() >= pstmtmax) {
                // Choose a closed element to remove.
                JStatement lru = null;
                Iterator i = psList.values().iterator();
                while (i.hasNext()) {
                    lru = (JStatement) i.next();
                    if (lru.isClosed()) {
                        // actually, remove the first closed element.
                        i.remove();
                        lru.forget();
                        break;
                    }
                }
            }
        }
    }

    /**
     * Notify a Close event on Connection.
     */
    @SuppressWarnings("boxing")
    public void notifyClose() {

        // Close all PreparedStatement not already closed
        // When a Connection has been closed, no PreparedStatement should
        // remain open. This can avoids lack of cursor on some databases.
        synchronized (psList) {
            if (psOpenNb > 0) {
                JStatement jst = null;
                Iterator i = psList.values().iterator();
                while (i.hasNext()) {
                    jst = (JStatement) i.next();
                    if (jst.forceClose()) {
                        psOpenNb--;
                    }
                }
                if (psOpenNb != 0) {
                    logger.warn("Bad psOpenNb value = {0}", psOpenNb);
                    psOpenNb = 0;
                }
            }
        }

        // Notify event to listeners
        for (int i = 0; i < eventListeners.size(); i++) {
            ConnectionEventListener l = eventListeners.elementAt(i);
            l.connectionClosed(new ConnectionEvent(this));
        }
    }

    /**
     * Notify an Error event on Connection.
     * @param ex the given exception
     */
    public void notifyError(final SQLException ex) {
        // Notify event to listeners
        for (int i = 0; i < eventListeners.size(); i++) {
            ConnectionEventListener l = eventListeners.elementAt(i);
            l.connectionErrorOccurred(new ConnectionEvent(this, ex));
        }
    }

    @Override
    public void addStatementEventListener(StatementEventListener listener) {

    }

    @Override
    public void removeStatementEventListener(StatementEventListener listener) {

    }

}