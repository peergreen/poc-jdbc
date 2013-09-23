/**
 * EasyBeans
 * Copyright (C) 2006 Bull S.A.S.
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
 * $Id: JConnection.java 5369 2010-02-24 14:58:19Z benoitf $
 * --------------------------------------------------------------------------
 */

package com.peergreen.jdbcpool;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
/**
 * This class represent a connection linked to the physical and XA connections.
 * All errors are reported to the managed connection. This connection is
 * returned to the client.
 * @author Philippe Durieux
 * @author Florent Benoit
 */
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.ow2.util.log.Log;
import org.ow2.util.log.LogFactory;

/**
 * This class represent a connection linked to the physical and XA connections.
 * All errors are reported to the managed connection.
 * This connection is returned to the client.
 * @author Philippe Durieux
 * @author Florent Benoit
 */
public class JConnection implements Connection {

    /**
     * Logger used for debug.
     */
    private static Log logger = LogFactory.getLog(JConnection.class);

    /**
     * JDBC connection provided by the DriverManager.
     */
    private Connection physicalConnection = null;

    /**
     * XA connection which receive events.
     */
    private JManagedConnection xaConnection = null;

    /**
     * Buils a Connection (viewed by the user) which rely on a Managed
     * connection and a physical connection.
     * @param xaConnection the XA connection.
     * @param physicalConnection the connection to the database.
     */
    public JConnection(final JManagedConnection xaConnection, final Connection physicalConnection) {
        this.xaConnection = xaConnection;
        this.physicalConnection = physicalConnection;
    }

    /**
     * Gets the physical connection to the database.
     * @return physical connection to the database
     */
    public Connection getConnection() {
        return physicalConnection;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Statement createStatement() throws SQLException {
        try {
            return physicalConnection.createStatement();
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareStatement(final String sql) throws SQLException {
        try {
            // Ask the Managed Connection to find one in the pool, if possible.
            return xaConnection.prepareStatement(sql);
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CallableStatement prepareCall(final String sql) throws SQLException {
        try {
            return physicalConnection.prepareCall(sql);
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String nativeSQL(final String sql) throws SQLException {
        try {
            return physicalConnection.nativeSQL(sql);
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    /**
     * @return true if the connection to the database is closed or not.
     * @throws SQLException if a database access error occurs
     */
    public boolean isPhysicallyClosed() throws SQLException {
        return physicalConnection.isClosed();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isClosed() throws SQLException {

        // TODO : This should return the status of the connection viewed from
        // the user,
        // not the physical connection!
        try {
            return physicalConnection.isClosed();
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        try {
            return physicalConnection.getMetaData();
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setReadOnly(final boolean readOnly) throws SQLException {
        try {
            physicalConnection.setReadOnly(readOnly);
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isReadOnly() throws SQLException {
        try {
            return physicalConnection.isReadOnly();
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setCatalog(final String catalog) throws SQLException {
        try {
            physicalConnection.setCatalog(catalog);
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getCatalog() throws SQLException {
        try {
            return physicalConnection.getCatalog();
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    /**
     * Trigger an event to the listener.
     * @exception SQLException if a database access error occurs
     */
    @Override
    public void close() throws SQLException {
        xaConnection.notifyClose();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setTransactionIsolation(final int level) throws SQLException {
        try {
            physicalConnection.setTransactionIsolation(level);
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getTransactionIsolation() throws SQLException {
        try {
            return physicalConnection.getTransactionIsolation();
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SQLWarning getWarnings() throws SQLException {
        try {
            return physicalConnection.getWarnings();
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clearWarnings() throws SQLException {
        try {
            physicalConnection.clearWarnings();
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    /**
     * In a JDBC-XA driver, Connection.commit is only called if we are outside a
     * global transaction. {@inheritDoc}
     */
    @Override
    public void commit() throws SQLException {
        try {
            physicalConnection.commit();
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    /**
     * In a JDBC-XA driver, Connection.rollback is only called if we are outside
     * a global transaction. {@inheritDoc}
     */
    @Override
    public void rollback() throws SQLException {
        try {
            physicalConnection.rollback();
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    /**
     * In a JDBC-XA driver, autocommit is false if we are in a global Tx.
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("boxing")
    public void setAutoCommit(final boolean autoCommit) throws SQLException {
        try {
            physicalConnection.setAutoCommit(autoCommit);
        } catch (SQLException e) {
            logger.error("setAutoCommit( {0} ) failed: ", autoCommit, e);
            xaConnection.notifyError(e);
            throw e;
        }
    }

    /**
     * In a JDBC-XA driver, autocommit is false if we are in a global Tx.
     * {@inheritDoc}
     */
    @Override
    public boolean getAutoCommit() throws SQLException {
        try {
            return physicalConnection.getAutoCommit();
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Statement createStatement(final int resultSetType, final int resultSetConcurrency) throws SQLException {
        try {
            return physicalConnection.createStatement(resultSetType, resultSetConcurrency);
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        try {
            return physicalConnection.getTypeMap();
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setTypeMap(final Map<String, Class<?>> map) throws SQLException {
        try {
            physicalConnection.setTypeMap(map);
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareStatement(final String sql, final int resultSetType, final int resultSetConcurrency)
            throws SQLException {
        try {
            return xaConnection.prepareStatement(sql, resultSetType, resultSetConcurrency);
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CallableStatement prepareCall(final String sql, final int resultSetType, final int resultSetConcurrency)
            throws SQLException {
        try {
            return physicalConnection.prepareCall(sql, resultSetType, resultSetConcurrency);
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Statement createStatement(final int resultSetType, final int resultSetConcurrency, final int resultSetHoldability)
            throws SQLException {
        try {
            return physicalConnection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getHoldability() throws SQLException {
        try {
            return physicalConnection.getHoldability();
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CallableStatement prepareCall(final String sql, final int resultSetType, final int resultSetConcurrency,
            final int resultSetHoldability) throws SQLException {
        try {
            return physicalConnection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareStatement(final String sql, final int autoGeneratedKeys) throws SQLException {
        try {
            return physicalConnection.prepareStatement(sql, autoGeneratedKeys);
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareStatement(final String sql, final int resultSetType, final int resultSetConcurrency,
            final int resultSetHoldability) throws SQLException {
        try {
            return physicalConnection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareStatement(final String sql, final int[] columnIndexes) throws SQLException {
        try {
            return physicalConnection.prepareStatement(sql, columnIndexes);
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareStatement(final String sql, final String[] columnNames) throws SQLException {
        try {
            return physicalConnection.prepareStatement(sql, columnNames);
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void releaseSavepoint(final Savepoint savepoint) throws SQLException {
        try {
            physicalConnection.releaseSavepoint(savepoint);
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void rollback(final Savepoint savepoint) throws SQLException {
        try {
            physicalConnection.rollback(savepoint);
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setHoldability(final int holdability) throws SQLException {
        try {
            physicalConnection.setHoldability(holdability);
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Savepoint setSavepoint() throws SQLException {
        try {
            return physicalConnection.setSavepoint();
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public java.sql.Savepoint setSavepoint(final String name) throws SQLException {
        try {
            return physicalConnection.setSavepoint(name);
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return physicalConnection.unwrap(iface);
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        try {
            return physicalConnection.isWrapperFor(iface);
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    @Override
    public Clob createClob() throws SQLException {
        try {
            return physicalConnection.createClob();
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    @Override
    public Blob createBlob() throws SQLException {
        try {
            return physicalConnection.createBlob();
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    @Override
    public NClob createNClob() throws SQLException {
        try {
            return physicalConnection.createNClob();
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        try {
            return physicalConnection.createSQLXML();
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        try {
            return physicalConnection.isValid(timeout);
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        try {
            physicalConnection.setClientInfo(name, value);
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }

    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        try {
            physicalConnection.setClientInfo(properties);
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }

    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        try {
            return physicalConnection.getClientInfo(name);
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        try {
            return physicalConnection.getClientInfo();
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        try {
            return physicalConnection.createArrayOf(typeName, elements);
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        try {
            return physicalConnection.createStruct(typeName, attributes);
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        try {
            physicalConnection.setSchema(schema);
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }

    }

    @Override
    public String getSchema() throws SQLException {
        try {
            return physicalConnection.getSchema();
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        try {
            physicalConnection.abort(executor);
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }

    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        try {
            physicalConnection.setNetworkTimeout(executor, milliseconds);
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }

    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        try {
            return physicalConnection.getNetworkTimeout();
        } catch (SQLException e) {
            xaConnection.notifyError(e);
            throw e;
        }
    }

}