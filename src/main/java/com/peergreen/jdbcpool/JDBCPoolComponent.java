/**
 * EasyBeans
 * Copyright (C) 2006-2009 Bull S.A.S.
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
 * $Id: JDBCPoolComponent.java 5369 2010-02-24 14:58:19Z benoitf $
 * --------------------------------------------------------------------------
 */

package com.peergreen.jdbcpool;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.transaction.TransactionManager;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Property;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.ow2.util.log.Log;
import org.ow2.util.log.LogFactory;

/**
 * Defines a component that creates a JDBC pool in order to use it in EasyBeans.
 *
 * @author Florent Benoit
 */
@Component
@Provides
public class JDBCPoolComponent  {

    /**
     * Logger.
     */
    private static Log logger = LogFactory.getLog(JDBCPoolComponent.class);

    /**
     * Default username.
     */
    private static final String DEFAULT_USER = "";

    /**
     * Default password.
     */
    private static final String DEFAULT_PASSWORD = "";

    /**
     * Default min pool.
     */
    private static final int DEFAULT_MIN_POOL = 10;

    /**
     * Default max pool.
     */
    private static final int DEFAULT_MAX_POOL = 30;

    /**
     * Default prepared statement.
     */
    private static final int DEFAULT_PSTMT = 10;

    /**
     * Default checked level.
     */
    private static final int DEFAULT_CHECK_LEVEL = 0;

    /**
     * Default test statement.
     */
    private static final String DEFAULT_TEST_STATEMENT_HSQLDB = "select 1 from INFORMATION_SCHEMA.SYSTEM_USERS";


    /**
     * Level of checking on connections when got from the pool. this avoids
     * reusing bad connections because too old, for example when database was
     * restarted... 0 = no checking 1 = check that still physically opened. 2 =
     * try a null statement.
     */
    @Property(mandatory=false, name="checkLevel")
   protected int checkLevel = DEFAULT_CHECK_LEVEL;

    /**
     * Connection manager object.
     */
    private ConnectionManager connectionManager = null;

    /**
     * JNDI name.
     */
    @Property(name="jndiName", mandatory=true)
    private String jndiName;

    /**
     * Username.
     */
    @Property(mandatory=true, name="username")
    protected String username;

    /**
     * Password.
     */
    @Property(mandatory=true, name="password")
    protected String password;

    /**
     * URL for accessing to the database.
     */
    @Property(name="url", mandatory=true)
    protected String url;

    /**
     * Name of the driver class to use.
     */
    @Property(name="driver", mandatory=true)
    protected String driver;

    /**
     * Use transaction or not ?
     */
    private final boolean useTM = true;

    /**
     * Pool min.
     */
    private final int poolMin = DEFAULT_MIN_POOL;

    /**
     * Pool max.
     */
    private final int poolMax = DEFAULT_MAX_POOL;

    /**
     * Max of prepared statement.
     */
    private final int pstmtMax = DEFAULT_PSTMT;

    /**
     * Test statement.
     */
    @Property(mandatory=true, name="testStatement")
    protected String testStatement = DEFAULT_TEST_STATEMENT_HSQLDB;


    @Requires
    private TransactionManager transactionManager;


    /**
     * Init method.<br/> This method is called before the start method.
     *
     */
    public void init() throws Exception {
        this.connectionManager = new ConnectionManager();
        this.connectionManager.setTransactionIsolation("default");

        // Check that data are correct
        validate();

        this.connectionManager.setDatasourceName(this.jndiName);
        this.connectionManager.setDSName(this.jndiName);
        this.connectionManager.setUrl(this.url);
        try {
            this.connectionManager.setClassName(this.driver);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Cannot load jdbc driver '" + this.driver + "'.", e);
        }
        this.connectionManager.setUserName(this.username);
        this.connectionManager.setPassword(this.password);
        this.connectionManager.setTransactionIsolation("default");
        this.connectionManager.setPstmtMax(this.pstmtMax);
        this.connectionManager.setCheckLevel(this.checkLevel);
        this.connectionManager.setTestStatement(this.testStatement);

    }

    /**
     * Validate current data.
     *
     * @throws EZBComponentException if validation fails.
     */
    private void validate() throws Exception {
        // check that there is a JNDI name
        if (this.jndiName == null) {
            throw new Exception("No JNDI name set");
        }

        // check that there is an URL
        if (this.url == null) {
            throw new Exception("No URL set");
        }

        // Check that there is a driver classname
        if (this.driver == null) {
            throw new Exception("No driver set");
        }
    }

    /**
     * Start method.<br/> This method is called after the init method.
     *
     * @throws EZBComponentException if the start has failed.
     */
    @Validate
    public void start() throws Exception {
        init();

        // set settings
        if (this.useTM) {
            this.connectionManager.setTm(this.transactionManager);
        }
        this.connectionManager.setPoolMin(this.poolMin);
        this.connectionManager.setPoolMax(this.poolMax);

        // Something is there ?
        try {
            Object o = new InitialContext().lookup(this.jndiName);
            if (o != null) {
                logger.warn("Entry with JNDI name {0} already exist", this.jndiName);
            }
        } catch (NamingException e) {
            logger.debug("Nothing with JNDI name {0}", this.jndiName);
        }

        // Bind the resource.
        try {
            new InitialContext().rebind(this.jndiName, this.connectionManager);
        } catch (NamingException e) {
            throw new Exception("Cannot bind a JDBC Datasource with the jndi name '" + this.jndiName + "'.");
        }

        logger.info("DS ''{0}'', URL ''{1}'', Driver = ''{2}''.", this.jndiName, this.url, this.driver);

    }

    /**
     * Stop method.<br/> This method is called when component needs to be
     * stopped.
     *
     * @throws EZBComponentException if the stop is failing.
     */
    @Invalidate
    public void stop() throws Exception {
        // Unbind the resource.
        try {
            new InitialContext().unbind(this.jndiName);
        } catch (NamingException e) {
            throw new Exception("Cannot unbind a JDBC Datasource with the jndi name '" + this.jndiName + "'.");
        }

    }


    /**
     * @return the name of the JDBC driver.
     */
    public String getDriver() {
        return this.driver;
    }


    /**
     * @return the name that is bound in the datasource
     */
    public String getJndiName() {
        return this.jndiName;
    }


    /**
     * @return the maximum size of the JDBC pool.
     */
    public int getPoolMax() {
        return this.poolMax;
    }

    /**
     * @return the minimum size of the JDBC pool.
     */
    public int getPoolMin() {
        return this.poolMax;
    }


    /**
     * @return the minimum size of the JDBC pool.
     */
    public int getPstmtMax() {
        return this.poolMax;
    }

    /**
     * @return the URL used for the connection.
     */
    public String getUrl() {
        return this.url;
    }


    /**
     * @return the username that is used to get a connection.
     */
    public String getUsername() {
        return this.username;
    }

    /**
     * @return true if this pool will use transaction (else false).
     */
    public boolean isUseTM() {
        return this.useTM;
    }

    /**
     * @return connection checking level
     */
    public int getCheckLevel() {
        return this.checkLevel;
    }


    /**
     * @return the test statement used with a checkedlevel.
     */
    public String getTestStatement() {
        return this.testStatement;
    }



}
