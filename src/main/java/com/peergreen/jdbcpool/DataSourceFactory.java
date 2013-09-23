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
 * $Id: DataSourceFactory.java 5369 2010-02-24 14:58:19Z benoitf $
 * --------------------------------------------------------------------------
 */

package com.peergreen.jdbcpool;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

import org.ow2.util.log.Log;
import org.ow2.util.log.LogFactory;

/**
 * Class which is used for binding the ConnectionManager object. getReference()
 * method of ConnectionManager redirect to this class. The getObjectInstance()
 * method will be called for the JNDI lookup
 * @author Florent Benoit
 */
public class DataSourceFactory implements ObjectFactory {

    /**
     * Logger.
     */
    private static Log logger = LogFactory.getLog(DataSourceFactory.class);

    /**
     * Creates an object using the location or reference information specified.
     * It gets a connection manager of this server.
     * @param obj The possibly null object containing location or reference
     *        information that can be used in creating an object.
     * @param name The name of this object relative to <code>nameCtx</code>,
     *        or null if no name is specified.
     * @param nameCtx The context relative to which the <code>name</code>
     *        parameter is specified, or null if <code>name</code> is relative
     *        to the default initial context.
     * @param environment The possibly null environment that is used in creating
     *        the object.
     * @return The object created; null if an object cannot be created.
     * @throws Exception if this object factory encountered an exception while
     *         attempting to create an object, and no other object factories are
     *         to be tried.
     */
    @Override
    public Object getObjectInstance(final Object obj, final Name name, final Context nameCtx, final Hashtable<?, ?> environment)
            throws Exception {

        Reference ref = (Reference) obj;

        String dsname = (String) ref.get("datasource.name").getContent();
        ConnectionManager ds = ConnectionManager.getConnectionManager(dsname);
        if (ds == null) {
            // The DataSource was not in the EasyBeans Server: Create it.
            logger.debug("Creating a new Connection Manager for {0}", dsname);
            try {
                // build a new datasource for another server
                ds = new ConnectionManager();
                ds.setDSName(dsname);
                ds.setUrl((String) ref.get("datasource.url").getContent());
                ds.setClassName((String) ref.get("datasource.classname").getContent());
                ds.setUserName((String) ref.get("datasource.username").getContent());
                ds.setPassword((String) ref.get("datasource.password").getContent());
                ds.setTransactionIsolation((String) ref.get("datasource.isolationlevel").getContent());
                ds.poolConfigure((String) ref.get("connchecklevel").getContent(),
                        (String) ref.get("connmaxage").getContent(), (String) ref.get("maxopentime").getContent(),
                        (String) ref.get("connteststmt").getContent(), (String) ref.get("pstmtmax").getContent(),
                        (String) ref.get("minconpool").getContent(), (String) ref.get("maxconpool").getContent(),
                        (String) ref.get("maxwaittime").getContent(), (String) ref.get("maxwaiters").getContent(),
                        (String) ref.get("samplingperiod").getContent());
            } catch (Exception e) {
                logger.error("DataSourceFactory error", e);
            }
        }
        return ds;
    }
}
