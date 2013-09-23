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
 * $Id: JStatement.java 5369 2010-02-24 14:58:19Z benoitf $
 * --------------------------------------------------------------------------
 */

package com.peergreen.jdbcpool;


import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import org.ow2.util.log.Log;
import org.ow2.util.log.LogFactory;

/**
 * Wrapper on a PreparedStatement. This wrapper is used to track close method in
 * order to avoid closing the statement, and putting it instead in a pool.
 * @author Philippe Durieux
 * @author Florent Benoit
 */
public class JStatement implements PreparedStatement {

    /**
     * Properties of this statement has been changed ? Needs to be be cleared when reused.
     */
    private boolean changed = false;

    /**
     * Is that this statement is opened ?
     */
    private boolean opened = false;

    /**
     * Being closed. (in close method).
     */
    private boolean closing = false;

    /**
     * Physical PreparedStatement object on which the wrapper is.
     */
    private final PreparedStatement ps;

    /**
     * Managed Connection the Statement belongs to.
     */
    private final JManagedConnection mc;

    /**
     * Hashcode computed in constructor.
     */
    private final int hashCode;

    /**
     * SQL used as statement.
     */
    private final String sql;

    /**
     * Logger.
     */
    private final Log logger = LogFactory.getLog(JStatement.class);

    /**
     * Builds a new statement with the given wrapped statement of given connection and given sql query.
     * @param ps the prepared statement.
     * @param mc managed connection
     * @param sql query.
     */
    public JStatement(final PreparedStatement ps, final JManagedConnection mc, final String sql) {
        this.ps = ps;
        this.mc = mc;
        this.sql = sql;
        hashCode = sql.hashCode();
        opened = true;
    }

    /**
     * @return Sql query used.
     */
    public String getSql() {
        return sql;
    }

    /**
     * @return hashcode of the object
     */
    @Override
    public int hashCode() {
        return hashCode;
    }

    /**
     * @param stmt given statement for comparing it
     * @return true if given object is equals to this current object
     */
    @Override
    public boolean equals(final Object stmt) {
        if (stmt == null) {
            return false;
        }
        // different hashcode, cannot be equals
        if (this.hashCode != stmt.hashCode()) {
            return false;
        }

        // if got same hashcode, try to see if cast is ok.
        if (!(stmt instanceof JStatement)) {
            logger.warn("Bad class {0}", stmt);
            return false;
        }
        JStatement psw = (JStatement) stmt;
        if (sql == null && psw.getSql() != null) {
            return false;
        }
        if (sql != null && !sql.equals(psw.getSql())) {
            return false;
        }
        try {
            if (psw.getResultSetType() != getResultSetType()) {
                return false;
            }
            if (psw.getResultSetConcurrency() != getResultSetConcurrency()) {
                return false;
            }
        } catch (SQLException e) {
            logger.warn("Cannot compare statements", e);
            return false;
        }
        logger.debug("Found");
        return true;
    }

    /**
     * Force a close on the Prepare Statement. Usually, it's the caller that did
     * not close it explicitly
     * @return true if it was open
     */
    public boolean forceClose() {
        if (opened) {
            logger.debug("Statements should be closed explicitly.");
            opened = false;
            return true;
        }
        return false;
    }

    /**
     * Reuses this statement so reset properties.
     * @throws SQLException if reset fails
     */
    public void reuse() throws SQLException {
        ps.clearParameters();
        ps.clearWarnings();
        opened = true;
        if (changed) {
            logger.debug("Properties statement have been changed, reset default properties");
            ps.clearBatch();
            ps.setFetchDirection(ResultSet.FETCH_FORWARD);
            ps.setMaxFieldSize(0);
            ps.setMaxRows(0);
            ps.setQueryTimeout(0);
            changed = false;
        }
    }

    /**
     * @return true if this statement has been closed, else false.
     */
    @Override
    public boolean isClosed() {
        return !opened && !closing;
    }

    /**
     * Physically close this Statement.
     * @throws SQLException
     */
    public void forget() {
        try {
            ps.close();
        } catch (SQLException e) {
            logger.error("Cannot close the PreparedStatement", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int executeUpdate() throws SQLException {
        return ps.executeUpdate();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addBatch() throws SQLException {
        changed = true;
        ps.addBatch();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clearParameters() throws SQLException {
        ps.clearParameters();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean execute() throws SQLException {
        return ps.execute();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setByte(final int parameterIndex, final byte x) throws SQLException {
        ps.setByte(parameterIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setDouble(final int parameterIndex, final double x) throws SQLException {
        ps.setDouble(parameterIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setFloat(final int parameterIndex, final float x) throws SQLException {
        ps.setFloat(parameterIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setInt(final int parameterIndex, final int x) throws SQLException {
        ps.setInt(parameterIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setNull(final int parameterIndex, final int sqlType) throws SQLException {
        ps.setNull(parameterIndex, sqlType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setLong(final int parameterIndex, final long x) throws SQLException {
        ps.setLong(parameterIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setShort(final int parameterIndex, final short x) throws SQLException {
        ps.setShort(parameterIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setBoolean(final int parameterIndex, final boolean x) throws SQLException {
        ps.setBoolean(parameterIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setBytes(final int parameterIndex, final byte[] x) throws SQLException {
        ps.setBytes(parameterIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setAsciiStream(final int parameterIndex, final InputStream x, final int length) throws SQLException {
        ps.setAsciiStream(parameterIndex, x, length);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setBinaryStream(final int parameterIndex, final InputStream x, final int length) throws SQLException {
        ps.setBinaryStream(parameterIndex, x, length);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("deprecation")
    public void setUnicodeStream(final int parameterIndex, final InputStream x, final int length) throws SQLException {
        ps.setUnicodeStream(parameterIndex, x, length);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setCharacterStream(final int parameterIndex, final Reader reader, final int length) throws SQLException {
        ps.setCharacterStream(parameterIndex, reader, length);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setObject(final int parameterIndex, final Object x) throws SQLException {
        ps.setObject(parameterIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setObject(final int parameterIndex, final Object x, final int targetSqlType) throws SQLException {
        ps.setObject(parameterIndex, x, targetSqlType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setObject(final int parameterIndex, final Object x,
            final int targetSqlType, final int scale) throws SQLException {
        ps.setObject(parameterIndex, x, targetSqlType, scale);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setNull(final int paramIndex, final int sqlType, final String typeName) throws SQLException {
        ps.setNull(paramIndex, sqlType, typeName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setString(final int parameterIndex, final String x) throws SQLException {
        ps.setString(parameterIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setBigDecimal(final int parameterIndex, final BigDecimal x) throws SQLException {
        ps.setBigDecimal(parameterIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setURL(final int parameterIndex, final URL x) throws SQLException {
        ps.setURL(parameterIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setArray(final int i, final Array x) throws SQLException {
        ps.setArray(i, x);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setBlob(final int i, final Blob x) throws SQLException {
        ps.setBlob(i, x);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setClob(final int i, final Clob x) throws SQLException {
        ps.setClob(i, x);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setDate(final int parameterIndex, final Date x) throws SQLException {
        ps.setDate(parameterIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return ps.getParameterMetaData();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setRef(final int i, final Ref x) throws SQLException {
        ps.setRef(i, x);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet executeQuery() throws SQLException {
        return ps.executeQuery();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return ps.getMetaData();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setTime(final int parameterIndex, final Time x) throws SQLException {
        ps.setTime(parameterIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setTimestamp(final int parameterIndex, final Timestamp x) throws SQLException {
        ps.setTimestamp(parameterIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setDate(final int parameterIndex, final Date x, final Calendar cal) throws SQLException {
        ps.setDate(parameterIndex, x, cal);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setTime(final int parameterIndex, final Time x, final Calendar cal) throws SQLException {
        ps.setTime(parameterIndex, x, cal);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setTimestamp(final int parameterIndex, final Timestamp x, final Calendar cal) throws SQLException {
        ps.setTimestamp(parameterIndex, x, cal);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getFetchDirection() throws SQLException {
        return ps.getFetchDirection();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getFetchSize() throws SQLException {
        return ps.getFetchSize();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMaxFieldSize() throws SQLException {
        return ps.getMaxFieldSize();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMaxRows() throws SQLException {
        return ps.getMaxRows();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getQueryTimeout() throws SQLException {
        return ps.getQueryTimeout();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getResultSetConcurrency() throws SQLException {
        return ps.getResultSetConcurrency();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getResultSetHoldability() throws SQLException {
        return ps.getResultSetHoldability();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getResultSetType() throws SQLException {
        return ps.getResultSetType();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getUpdateCount() throws SQLException {
        return ps.getUpdateCount();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void cancel() throws SQLException {
        ps.cancel();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clearBatch() throws SQLException {
        ps.clearBatch();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clearWarnings() throws SQLException {
        ps.clearWarnings();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws SQLException {
        if (!opened) {
            logger.debug("Statement already closed");
            return;
        }
        opened = false;
        closing = true;
        mc.notifyPsClose(this);
        closing = false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean getMoreResults() throws SQLException {
        return ps.getMoreResults();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int[] executeBatch() throws SQLException {
        return ps.executeBatch();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setFetchDirection(final int direction) throws SQLException {
        changed = true;
        ps.setFetchDirection(direction);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setFetchSize(final int rows) throws SQLException {
        changed = true;
        ps.setFetchSize(rows);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setMaxFieldSize(final int max) throws SQLException {
        changed = true;
        ps.setMaxFieldSize(max);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setMaxRows(final int max) throws SQLException {
        changed = true;
        ps.setMaxRows(max);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setQueryTimeout(final int seconds) throws SQLException {
        changed = true;
        ps.setQueryTimeout(seconds);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean getMoreResults(final int current) throws SQLException {
        return ps.getMoreResults(current);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setEscapeProcessing(final boolean enable) throws SQLException {
        ps.setEscapeProcessing(enable);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int executeUpdate(final String sql) throws SQLException {
        return ps.executeUpdate(sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addBatch(final String sql) throws SQLException {
        changed = true;
        ps.addBatch(sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setCursorName(final String name) throws SQLException {
        ps.setCursorName(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean execute(final String sql) throws SQLException {
        changed = true;
        return ps.execute(sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int executeUpdate(final String sql, final int autoGeneratedKeys) throws SQLException {
        changed = true;
        return ps.executeUpdate(sql, autoGeneratedKeys);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean execute(final String sql, final int autoGeneratedKeys) throws SQLException {
        changed = true;
        return ps.execute(sql, autoGeneratedKeys);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int executeUpdate(final String sql, final int[] columnIndexes) throws SQLException {
        changed = true;
        return ps.executeUpdate(sql, columnIndexes);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean execute(final String sql, final int[] columnIndexes) throws SQLException {
        changed = true;
        return ps.execute(sql, columnIndexes);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Connection getConnection() throws SQLException {
        return ps.getConnection();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return ps.getGeneratedKeys();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getResultSet() throws SQLException {
        return ps.getResultSet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SQLWarning getWarnings() throws SQLException {
        return ps.getWarnings();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int executeUpdate(final String sql, final String[] columnNames) throws SQLException {
        return ps.executeUpdate(sql, columnNames);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean execute(final String sql, final String[] columnNames) throws SQLException {
        return ps.execute(sql, columnNames);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet executeQuery(final String sql) throws SQLException {
        return ps.executeQuery(sql);
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        ps.setPoolable(poolable);

    }

    @Override
    public boolean isPoolable() throws SQLException {
        return ps.isPoolable();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        ps.closeOnCompletion();

    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return ps.isCloseOnCompletion();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return ps.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return ps.isWrapperFor(iface);
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        ps.setRowId(parameterIndex, x);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
       ps.setNString(parameterIndex, value);

    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        ps.setNCharacterStream(parameterIndex, value);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        ps.setNClob(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        ps.setClob(parameterIndex, reader, length);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        ps.setBlob(parameterIndex, inputStream, length);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        ps.setNClob(parameterIndex, reader, length);

    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        ps.setSQLXML(parameterIndex, xmlObject);

    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        ps.setAsciiStream(parameterIndex, x, length);

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        ps.setBinaryStream(parameterIndex, x, length);

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        ps.setCharacterStream(parameterIndex, reader, length);

    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        ps.setAsciiStream(parameterIndex, x);

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        ps.setBinaryStream(parameterIndex, x);

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        ps.setCharacterStream(parameterIndex, reader);

    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        ps.setNCharacterStream(parameterIndex, value);

    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        ps.setClob(parameterIndex, reader);

    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        ps.setBlob(parameterIndex, inputStream);

    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        ps.setNClob(parameterIndex, reader);

    }

}
