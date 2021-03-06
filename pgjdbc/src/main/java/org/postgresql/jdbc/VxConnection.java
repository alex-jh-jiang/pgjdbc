/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.jdbc;

import org.postgresql.Driver;
import org.postgresql.PGNotification;
import org.postgresql.PGProperty;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.postgresql.core.BaseStatement;
import org.postgresql.core.CachedQuery;
import org.postgresql.core.ConnectionFactory;
import org.postgresql.core.Encoding;
import org.postgresql.core.Oid;
import org.postgresql.core.Provider;
import org.postgresql.core.Query;
import org.postgresql.core.QueryExecutor;
import org.postgresql.core.ReplicationProtocol;
import org.postgresql.core.ResultHandlerBase;
import org.postgresql.core.ServerVersion;
import org.postgresql.core.SqlCommand;
import org.postgresql.core.TransactionState;
import org.postgresql.core.TypeInfo;
import org.postgresql.core.Utils;
import org.postgresql.core.Version;
import org.postgresql.core.VxBaseResultSet;
import org.postgresql.fastpath.Fastpath;
import org.postgresql.largeobject.LargeObjectManager;
import org.postgresql.replication.PGReplicationConnection;
import org.postgresql.replication.PGReplicationConnectionImpl;
import org.postgresql.util.GT;
import org.postgresql.util.HostSpec;
import org.postgresql.util.LruCache;
import org.postgresql.util.PGBinaryObject;
import org.postgresql.util.PGobject;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.io.IOException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.ClientInfoStatus;
import java.sql.Clob;
//import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
//import java.sql.PreparedStatement;
//import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLPermission;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
//import java.sql.Statement;
import java.sql.Struct;
import java.sql.Types;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.ea.async.Async.await;

public class VxConnection {

	private static final Logger LOGGER = Logger.getLogger(VxConnection.class.getName());

	private static final SQLPermission SQL_PERMISSION_ABORT = new SQLPermission("callAbort");
	private static final SQLPermission SQL_PERMISSION_NETWORK_TIMEOUT = new SQLPermission("setNetworkTimeout");

	//
	// Data initialized on construction:
	//
	private final Properties _clientInfo;
	
	//
  // Data initialized on construction info:
  //
  private final Properties _info;

	/* URL we were created via */
	private final String creatingURL;

	private Throwable openStackTrace;

	/* Actual network handler */
	private final QueryExecutor queryExecutor;

	/* Query that runs COMMIT */
	private final Query commitQuery;
	/* Query that runs ROLLBACK */
	private final Query rollbackQuery;

	private final TypeInfo _typeCache;

	private boolean disableColumnSanitiser = false;

	// Default statement prepare threshold.
	protected int prepareThreshold;

	/**
	 * Default fetch size for statement
	 *
	 * @see PGProperty#DEFAULT_ROW_FETCH_SIZE
	 */
	protected int defaultFetchSize;

	// Default forcebinary option.
	protected boolean forcebinary = false;

	private int rsHoldability = java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
	private int savepointId = 0;
	// Connection's autocommit state.
	private boolean autoCommit = true;
	// Connection's readonly state.
	private boolean readOnly = false;

	// Bind String to UNSPECIFIED or VARCHAR?
	private final boolean bindStringAsVarchar;

	// Current warnings; there might be more on queryExecutor too.
	private SQLWarning firstWarning = null;

	// Timer for scheduling TimerTasks for this connection.
	// Only instantiated if a task is actually scheduled.
	private volatile Timer cancelTimer = null;

	private VxPreparedStatement checkConnectionQuery;
	/**
	 * Replication protocol in current version postgresql(10devel) supports a
	 * limited number of commands.
	 */
	private final boolean replicationConnection;

	private final LruCache<FieldMetadata.Key, FieldMetadata> fieldMetadataCache;
	
	final CachedQuery borrowQuery(String sql) throws SQLException {
		return queryExecutor.borrowQuery(sql);
	}

	final CachedQuery borrowCallableQuery(String sql) throws SQLException {
		return queryExecutor.borrowCallableQuery(sql);
	}

	private CachedQuery borrowReturningQuery(String sql, String[] columnNames) throws SQLException {
		return queryExecutor.borrowReturningQuery(sql, columnNames);
	}

	public CachedQuery createQuery(String sql, boolean escapeProcessing, boolean isParameterized, String... columnNames)
			throws SQLException {
		return queryExecutor.createQuery(sql, escapeProcessing, isParameterized, columnNames);
	}

	void releaseQuery(CachedQuery cachedQuery) {
		queryExecutor.releaseQuery(cachedQuery);
	}

	public void setFlushCacheOnDeallocate(boolean flushCacheOnDeallocate) {
		queryExecutor.setFlushCacheOnDeallocate(flushCacheOnDeallocate);
		LOGGER.log(Level.FINE, "  setFlushCacheOnDeallocate = {0}", flushCacheOnDeallocate);
	}

	//
	// Ctor.
	//
	public VxConnection(QueryExecutor queryExecutor, Properties info, String url) throws SQLException {
		// Print out the driver version number
		LOGGER.log(Level.FINE, org.postgresql.util.DriverInfo.DRIVER_FULL_NAME);

		this._info = info;
		this.creatingURL = url;

		setDefaultFetchSize(PGProperty.DEFAULT_ROW_FETCH_SIZE.getInt(info));

		setPrepareThreshold(PGProperty.PREPARE_THRESHOLD.getInt(info));
		if (prepareThreshold == -1) {
			setForceBinary(true);
		}

		// Now make the initial connection and set up local state
		this.queryExecutor = queryExecutor;

		// WARNING for unsupported servers (8.1 and lower are not supported)
		if (LOGGER.isLoggable(Level.WARNING) && !haveMinimumServerVersion(ServerVersion.v8_2)) {
			LOGGER.log(Level.WARNING, "Unsupported Server Version: {0}", queryExecutor.getServerVersion());
		}

		// Set read-only early if requested
		if (PGProperty.READ_ONLY.getBoolean(info)) {
			setReadOnly(true);
		}

		boolean binaryTransfer = PGProperty.BINARY_TRANSFER.getBoolean(info);
		// Formats that currently have binary protocol support
		Set<Integer> binaryOids = new HashSet<Integer>();
		if (binaryTransfer && queryExecutor.getProtocolVersion() >= 3) {
			binaryOids.add(Oid.BYTEA);
			binaryOids.add(Oid.INT2);
			binaryOids.add(Oid.INT4);
			binaryOids.add(Oid.INT8);
			binaryOids.add(Oid.FLOAT4);
			binaryOids.add(Oid.FLOAT8);
			binaryOids.add(Oid.TIME);
			binaryOids.add(Oid.DATE);
			binaryOids.add(Oid.TIMETZ);
			binaryOids.add(Oid.TIMESTAMP);
			binaryOids.add(Oid.TIMESTAMPTZ);
			binaryOids.add(Oid.INT2_ARRAY);
			binaryOids.add(Oid.INT4_ARRAY);
			binaryOids.add(Oid.INT8_ARRAY);
			binaryOids.add(Oid.FLOAT4_ARRAY);
			binaryOids.add(Oid.FLOAT8_ARRAY);
			binaryOids.add(Oid.FLOAT8_ARRAY);
			binaryOids.add(Oid.VARCHAR_ARRAY);
			binaryOids.add(Oid.TEXT_ARRAY);
			binaryOids.add(Oid.POINT);
			binaryOids.add(Oid.BOX);
			binaryOids.add(Oid.UUID);
		}

		binaryOids.addAll(getOidSet(PGProperty.BINARY_TRANSFER_ENABLE.get(info)));
		binaryOids.removeAll(getOidSet(PGProperty.BINARY_TRANSFER_DISABLE.get(info)));

		// split for receive and send for better control
		Set<Integer> useBinarySendForOids = new HashSet<Integer>();
		useBinarySendForOids.addAll(binaryOids);

		Set<Integer> useBinaryReceiveForOids = new HashSet<Integer>();
		useBinaryReceiveForOids.addAll(binaryOids);

		/*
		 * Does not pass unit tests because unit tests expect setDate to have
		 * millisecond accuracy whereas the binary transfer only supports date accuracy.
		 */
		useBinarySendForOids.remove(Oid.DATE);

		queryExecutor.setBinaryReceiveOids(useBinaryReceiveForOids);
		queryExecutor.setBinarySendOids(useBinarySendForOids);

		if (LOGGER.isLoggable(Level.FINEST)) {
			LOGGER.log(Level.FINEST, "    types using binary send = {0}", oidsToString(useBinarySendForOids));
			LOGGER.log(Level.FINEST, "    types using binary receive = {0}", oidsToString(useBinaryReceiveForOids));
			LOGGER.log(Level.FINEST, "    integer date/time = {0}", queryExecutor.getIntegerDateTimes());
		}

		//
		// String -> text or unknown?
		//

		String stringType = PGProperty.STRING_TYPE.get(info);
		if (stringType != null) {
			if (stringType.equalsIgnoreCase("unspecified")) {
				bindStringAsVarchar = false;
			} else if (stringType.equalsIgnoreCase("varchar")) {
				bindStringAsVarchar = true;
			} else {
				throw new PSQLException(GT.tr("Unsupported value for stringtype parameter: {0}", stringType),
						PSQLState.INVALID_PARAMETER_VALUE);
			}
		} else {
			bindStringAsVarchar = true;
		}

		// Initialize timestamp stuff
		timestampUtils = new TimestampUtils(!queryExecutor.getIntegerDateTimes(), new Provider<TimeZone>() {
			@Override
			public TimeZone get() {
				return queryExecutor.getTimeZone();
			}
		});

		// Initialize common queries.
		// isParameterized==true so full parse is performed and the engine knows the
		// query
		// is not a compound query with ; inside, so it could use parse/bind/exec
		// messages
		commitQuery = createQuery("COMMIT", false, true).query;
		rollbackQuery = createQuery("ROLLBACK", false, true).query;

		int unknownLength = PGProperty.UNKNOWN_LENGTH.getInt(info);

		// Initialize object handling
		_typeCache = createTypeInfo(this.createConnection(), unknownLength);
		initObjectTypes(info);

		if (PGProperty.LOG_UNCLOSED_CONNECTIONS.getBoolean(info)) {
			openStackTrace = new Throwable("Connection was created at this point:");
		}
		this.disableColumnSanitiser = PGProperty.DISABLE_COLUMN_SANITISER.getBoolean(info);

		TypeInfo types1 = getTypeInfo();
		if (haveMinimumServerVersion(ServerVersion.v8_3)) {
			types1.addCoreType("uuid", Oid.UUID, Types.OTHER, "java.util.UUID", Oid.UUID_ARRAY);
		}

		TypeInfo types = getTypeInfo();
		if (haveMinimumServerVersion(ServerVersion.v8_3)) {
			types.addCoreType("xml", Oid.XML, Types.SQLXML, "java.sql.SQLXML", Oid.XML_ARRAY);
		}

		this._clientInfo = new Properties();
		if (haveMinimumServerVersion(ServerVersion.v9_0)) {
			String appName = PGProperty.APPLICATION_NAME.get(info);
			if (appName == null) {
				appName = "";
			}
			this._clientInfo.put("ApplicationName", appName);
		}

		fieldMetadataCache = new LruCache<FieldMetadata.Key, FieldMetadata>(
				Math.max(0, PGProperty.DATABASE_METADATA_CACHE_FIELDS.getInt(info)),
				Math.max(0, PGProperty.DATABASE_METADATA_CACHE_FIELDS_MIB.getInt(info) * 1024 * 1024), false);

		replicationConnection = PGProperty.REPLICATION.get(info) != null;
	}

	private Set<Integer> getOidSet(String oidList) throws PSQLException {
		Set<Integer> oids = new HashSet<Integer>();
		StringTokenizer tokenizer = new StringTokenizer(oidList, ",");
		while (tokenizer.hasMoreTokens()) {
			String oid = tokenizer.nextToken();
			oids.add(Oid.valueOf(oid));
		}
		return oids;
	}

	private String oidsToString(Set<Integer> oids) {
		StringBuilder sb = new StringBuilder();
		for (Integer oid : oids) {
			sb.append(Oid.toString(oid));
			sb.append(',');
		}
		if (sb.length() > 0) {
			sb.setLength(sb.length() - 1);
		} else {
			sb.append(" <none>");
		}
		return sb.toString();
	}

	private final TimestampUtils timestampUtils;

	public TimestampUtils getTimestampUtils() {
		return timestampUtils;
	}

	/**
	 * The current type mappings
	 */
	protected Map<String, Class<?>> typemap;

	public VxStatement createStatement() throws SQLException {
		// We now follow the spec and default to TYPE_FORWARD_ONLY.
		return createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
	}

	public VxPreparedStatement prepareStatement(String sql) throws SQLException {
		return prepareStatement(sql, VxBaseResultSet.TYPE_FORWARD_ONLY, VxBaseResultSet.CONCUR_READ_ONLY);
	}

	public VxCallableStatement prepareCall(String sql) throws SQLException {
		return prepareCall(sql, java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
	}

	public Map<String, Class<?>> getTypeMap() throws SQLException {
		checkClosed();
		return typemap;
	}

	public QueryExecutor getQueryExecutor() {
		return queryExecutor;
	}

	public ReplicationProtocol getReplicationProtocol() {
		return queryExecutor.getReplicationProtocol();
	}

	/**
	 * This adds a warning to the warning chain.
	 *
	 * @param warn
	 *            warning to add
	 */
	public void addWarning(SQLWarning warn) {
		// Add the warning to the chain
		if (firstWarning != null) {
			firstWarning.setNextWarning(warn);
		} else {
			firstWarning = warn;
		}

	}

	public CompletableFuture<VxResultSet> execSQLQuery(String s) throws SQLException {
		return execSQLQuery(s, java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
	}

	public CompletableFuture<VxResultSet> execSQLQuery(String s, int resultSetType, int resultSetConcurrency) throws SQLException {
	  VxStatement stat = (VxStatement) createStatement(resultSetType, resultSetConcurrency);
		boolean hasResultSet = await(stat.executeWithFlags(s, QueryExecutor.QUERY_SUPPRESS_BEGIN));

		while (!hasResultSet && stat.getUpdateCount() != -1) {
			hasResultSet = stat.getMoreResults();
		}

		if (!hasResultSet) {
			throw new PSQLException(GT.tr("No results were returned by the query."), PSQLState.NO_DATA);
		}

		// Transfer warnings to the connection, since the user never
		// has a chance to see the statement itself.
		SQLWarning warnings = stat.getWarnings();
		if (warnings != null) {
			addWarning(warnings);
		}

		return CompletableFuture.completedFuture(stat.getResultSet());
	}

	public CompletableFuture<Void> execSQLUpdate(String s) throws SQLException {
		BaseStatement stmt = (BaseStatement) createStatement();
		if (await(stmt.executeWithFlags(s, QueryExecutor.QUERY_NO_METADATA | QueryExecutor.QUERY_NO_RESULTS
				| QueryExecutor.QUERY_SUPPRESS_BEGIN))) {
			// throw new PSQLException(GT.tr("A result was returned when none was
			// expected."),
			// PSQLState.TOO_MANY_RESULTS);
			new PSQLException(GT.tr("A result was returned when none was expected."), PSQLState.TOO_MANY_RESULTS);
		} else {

			// Transfer warnings to the connection, since the user never
			// has a chance to see the statement itself.
			SQLWarning warnings = stmt.getWarnings();
			if (warnings != null) {
				addWarning(warnings);
			}
			stmt.close();
		}
		
		return CompletableFuture.completedFuture(null);
	}

	/**
	 * In SQL, a result table can be retrieved through a cursor that is named. The
	 * current row of a result can be updated or deleted using a positioned
	 * update/delete statement that references the cursor name.
	 * <p>
	 * We do not support positioned update/delete, so this is a no-op.
	 *
	 * @param cursor
	 *            the cursor name
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	public void setCursorName(String cursor) throws SQLException {
		checkClosed();
		// No-op.
	}

	/**
	 * getCursorName gets the cursor name.
	 *
	 * @return the current cursor name
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	public String getCursorName() throws SQLException {
		checkClosed();
		return null;
	}

	/**
	 * We are required to bring back certain information by the DatabaseMetaData
	 * class. These functions do that.
	 * <p>
	 * Method getURL() brings back the URL (good job we saved it)
	 *
	 * @return the url
	 * @throws SQLException
	 *             just in case...
	 */
	public String getURL() throws SQLException {
		return creatingURL;
	}

	/**
	 * Method getUserName() brings back the User Name (again, we saved it)
	 *
	 * @return the user name
	 * @throws SQLException
	 *             just in case...
	 */
	public String getUserName() throws SQLException {
		return queryExecutor.getUser();
	}

	public Fastpath getFastpathAPI() throws SQLException {
		checkClosed();
		if (fastpath == null) {
			fastpath = new Fastpath(this.createConnection());
		}
		return fastpath;
	}

	// This holds a reference to the Fastpath API if already open
	private Fastpath fastpath = null;

	public LargeObjectManager getLargeObjectAPI() throws SQLException {
		checkClosed();
		if (largeobject == null) {
			largeobject = new LargeObjectManager(this.createConnection());
		}
		return largeobject;
	}

	// This holds a reference to the LargeObject API if already open
	private LargeObjectManager largeobject = null;

	/*
	 * This method is used internally to return an object based around
	 * org.postgresql's more unique data types.
	 *
	 * <p>It uses an internal HashMap to get the handling class. If the type is not
	 * supported, then an instance of org.postgresql.util.PGobject is returned.
	 *
	 * You can use the getValue() or setValue() methods to handle the returned
	 * object. Custom objects can have their own methods.
	 *
	 * @return PGobject for this type, and set to value
	 *
	 * @exception SQLException if value is not correct for this type
	 */
	public Object getObject(String type, String value, byte[] byteValue) throws SQLException {
		if (typemap != null) {
			Class<?> c = typemap.get(type);
			if (c != null) {
				// Handle the type (requires SQLInput & SQLOutput classes to be implemented)
				throw new PSQLException(GT.tr("Custom type maps are not supported."), PSQLState.NOT_IMPLEMENTED);
			}
		}

		PGobject obj = null;

		if (LOGGER.isLoggable(Level.FINEST)) {
			LOGGER.log(Level.FINEST, "Constructing object from type={0} value=<{1}>", new Object[] { type, value });
		}

		try {
			Class<? extends PGobject> klass = _typeCache.getPGobject(type);

			// If className is not null, then try to instantiate it,
			// It must be basetype PGobject

			// This is used to implement the org.postgresql unique types (like lseg,
			// point, etc).

			if (klass != null) {
				obj = klass.newInstance();
				obj.setType(type);
				if (byteValue != null && obj instanceof PGBinaryObject) {
					PGBinaryObject binObj = (PGBinaryObject) obj;
					binObj.setByteValue(byteValue, 0);
				} else {
					obj.setValue(value);
				}
			} else {
				// If className is null, then the type is unknown.
				// so return a PGobject with the type set, and the value set
				obj = new PGobject();
				obj.setType(type);
				obj.setValue(value);
			}

			return obj;
		} catch (SQLException sx) {
			// rethrow the exception. Done because we capture any others next
			throw sx;
		} catch (Exception ex) {
			throw new PSQLException(GT.tr("Failed to create object for: {0}.", type), PSQLState.CONNECTION_FAILURE, ex);
		}
	}

	protected TypeInfo createTypeInfo(BaseConnection conn, int unknownLength) {
		return new TypeInfoCache(conn, unknownLength);
	}

	public TypeInfo getTypeInfo() {
		return _typeCache;
	}

	public void addDataType(String type, String name) {
		try {
			addDataType(type, Class.forName(name).asSubclass(PGobject.class));
		} catch (Exception e) {
			throw new RuntimeException("Cannot register new type: " + e);
		}
	}

	public void addDataType(String type, Class<? extends PGobject> klass) throws SQLException {
		checkClosed();
		_typeCache.addDataType(type, klass);
	}

	// This initialises the objectTypes hash map
	private void initObjectTypes(Properties info) throws SQLException {
		// Add in the types that come packaged with the driver.
		// These can be overridden later if desired.
		addDataType("box", org.postgresql.geometric.PGbox.class);
		addDataType("circle", org.postgresql.geometric.PGcircle.class);
		addDataType("line", org.postgresql.geometric.PGline.class);
		addDataType("lseg", org.postgresql.geometric.PGlseg.class);
		addDataType("path", org.postgresql.geometric.PGpath.class);
		addDataType("point", org.postgresql.geometric.PGpoint.class);
		addDataType("polygon", org.postgresql.geometric.PGpolygon.class);
		addDataType("money", org.postgresql.util.PGmoney.class);
		addDataType("interval", org.postgresql.util.PGInterval.class);

		Enumeration<?> e = info.propertyNames();
		while (e.hasMoreElements()) {
			String propertyName = (String) e.nextElement();
			if (propertyName.startsWith("datatype.")) {
				String typeName = propertyName.substring(9);
				String className = info.getProperty(propertyName);
				Class<?> klass;

				try {
					klass = Class.forName(className);
				} catch (ClassNotFoundException cnfe) {
					throw new PSQLException(
							GT.tr("Unable to load the class {0} responsible for the datatype {1}", className, typeName),
							PSQLState.SYSTEM_ERROR, cnfe);
				}

				addDataType(typeName, klass.asSubclass(PGobject.class));
			}
		}
	}

	/**
	 * <B>Note:</B> even though {@code Statement} is automatically closed when it is
	 * garbage collected, it is better to close it explicitly to lower resource
	 * consumption.
	 *
	 * {@inheritDoc}
	 */
	public void close() throws SQLException {
		if (queryExecutor == null) {
			// This might happen in case constructor throws an exception (e.g. host being
			// not available).
			// When that happens the connection is still registered in the finalizer queue,
			// so it gets finalized
			return;
		}
		releaseTimer();
		queryExecutor.close();
		openStackTrace = null;
	}

	public String nativeSQL(String sql) throws SQLException {
		checkClosed();
		CachedQuery cachedQuery = queryExecutor.createQuery(sql, false, true);

		return cachedQuery.query.getNativeSql();
	}

	public synchronized SQLWarning getWarnings() throws SQLException {
		checkClosed();
		SQLWarning newWarnings = queryExecutor.getWarnings(); // NB: also clears them.
		if (firstWarning == null) {
			firstWarning = newWarnings;
		} else {
			firstWarning.setNextWarning(newWarnings); // Chain them on.
		}

		return firstWarning;
	}

	public synchronized void clearWarnings() throws SQLException {
		checkClosed();
		queryExecutor.getWarnings(); // Clear and discard.
		firstWarning = null;
	}

	public void setReadOnly(boolean readOnly) throws SQLException {
		checkClosed();
		if (queryExecutor.getTransactionState() != TransactionState.IDLE) {
			throw new PSQLException(
					GT.tr("Cannot change transaction read-only property in the middle of a transaction."),
					PSQLState.ACTIVE_SQL_TRANSACTION);
		}

		if (readOnly != this.readOnly) {
			String readOnlySql = "SET SESSION CHARACTERISTICS AS TRANSACTION "
					+ (readOnly ? "READ ONLY" : "READ WRITE");
			try {
				execSQLUpdate(readOnlySql).get();
			} catch (InterruptedException | ExecutionException e) {
				throw new SQLException(e);
			} // nb: no BEGIN triggered.
		}

		this.readOnly = readOnly;
		LOGGER.log(Level.FINE, "  setReadOnly = {0}", readOnly);
	}

	public boolean isReadOnly() throws SQLException {
		checkClosed();
		return readOnly;
	}

	public CompletableFuture<Void> setAutoCommit(boolean autoCommit) throws SQLException {
		checkClosed();

		if (this.autoCommit == autoCommit) {
			return CompletableFuture.completedFuture(null);
		}

		if (!this.autoCommit) {
			await(commit());
		}

		this.autoCommit = autoCommit;
		LOGGER.log(Level.FINE, "  setAutoCommit = {0}", autoCommit);
		return CompletableFuture.completedFuture(null);
	}

	public boolean getAutoCommit() throws SQLException {
		checkClosed();
		return this.autoCommit;
	}

	private CompletableFuture<Void> executeTransactionCommand(Query query) throws SQLException {
		int flags = QueryExecutor.QUERY_NO_METADATA | QueryExecutor.QUERY_NO_RESULTS
				| QueryExecutor.QUERY_SUPPRESS_BEGIN;
		if (prepareThreshold == 0) {
			flags |= QueryExecutor.QUERY_ONESHOT;
		}

		try {
			await(getQueryExecutor().execute(query, null, new TransactionCommandHandler(), 0, 0, flags));
		} catch (SQLException e) {
			// Don't retry composite queries as it might get partially executed
			if (query.getSubqueries() != null || !queryExecutor.willHealOnRetry(e)) {
				throw e;
			}
			query.close();
			// retry
			await(getQueryExecutor().execute(query, null, new TransactionCommandHandler(), 0, 0, flags));
		}

		return CompletableFuture.completedFuture(null);
	}

	public CompletableFuture<Void> commit() throws SQLException {
		checkClosed();

		if (autoCommit) {
			throw new PSQLException(GT.tr("Cannot commit when autoCommit is enabled."),
					PSQLState.NO_ACTIVE_SQL_TRANSACTION);
		}

		if (queryExecutor.getTransactionState() != TransactionState.IDLE) {
		  await(executeTransactionCommand(commitQuery));
		}
		
		return CompletableFuture.completedFuture(null);
	}

	protected void checkClosed() throws SQLException {
		if (isClosed()) {
			throw new PSQLException(GT.tr("This connection has been closed."), PSQLState.CONNECTION_DOES_NOT_EXIST);
		}
	}

	public CompletableFuture<Void> rollback() throws SQLException {
		checkClosed();

		if (autoCommit) {
			throw new PSQLException(GT.tr("Cannot rollback when autoCommit is enabled."),
					PSQLState.NO_ACTIVE_SQL_TRANSACTION);
		}

		if (queryExecutor.getTransactionState() != TransactionState.IDLE) {
		  await(executeTransactionCommand(rollbackQuery));
		}
		
		return CompletableFuture.completedFuture(null);
	}

	public TransactionState getTransactionState() {
		return queryExecutor.getTransactionState();
	}

	public CompletableFuture<Integer> getTransactionIsolation() throws SQLException {
		checkClosed();

		String level = null;
		VxResultSet rs = null;
		rs = await(execSQLQuery("SHOW TRANSACTION ISOLATION LEVEL"));
		if (await(rs.next())) {
			level = await(rs.getString(1));
		}
		rs.close();

		// nb: no BEGIN triggered

		// TODO revisit: throw exception instead of silently eating the error in unknown
		// cases?
		if (level == null) {
			return CompletableFuture.completedFuture(java.sql.Connection.TRANSACTION_READ_COMMITTED); // Best guess.
		}

		level = level.toUpperCase(Locale.US);
		if (level.equals("READ COMMITTED")) {
			return CompletableFuture.completedFuture(java.sql.Connection.TRANSACTION_READ_COMMITTED);
		}
		if (level.equals("READ UNCOMMITTED")) {
			return CompletableFuture.completedFuture(java.sql.Connection.TRANSACTION_READ_UNCOMMITTED);
		}
		if (level.equals("REPEATABLE READ")) {
			return CompletableFuture.completedFuture(java.sql.Connection.TRANSACTION_REPEATABLE_READ);
		}
		if (level.equals("SERIALIZABLE")) {
			return CompletableFuture.completedFuture(java.sql.Connection.TRANSACTION_SERIALIZABLE);
		}

		return CompletableFuture.completedFuture(java.sql.Connection.TRANSACTION_READ_COMMITTED); // Best guess.
	}

	public void setTransactionIsolation(int level) throws SQLException {
		checkClosed();

		if (queryExecutor.getTransactionState() != TransactionState.IDLE) {
			throw new PSQLException(GT.tr("Cannot change transaction isolation level in the middle of a transaction."),
					PSQLState.ACTIVE_SQL_TRANSACTION);
		}

		String isolationLevelName = getIsolationLevelName(level);
		if (isolationLevelName == null) {
			throw new PSQLException(GT.tr("Transaction isolation level {0} not supported.", level),
					PSQLState.NOT_IMPLEMENTED);
		}

		String isolationLevelSQL = "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL " + isolationLevelName;
		try {
			execSQLUpdate(isolationLevelSQL).get();
		} catch (InterruptedException | ExecutionException e) {
			throw new SQLException(e);
		} // nb: no BEGIN triggered
		LOGGER.log(Level.FINE, "  setTransactionIsolation = {0}", isolationLevelName);
	}

	protected String getIsolationLevelName(int level) {
		switch (level) {
		case java.sql.Connection.TRANSACTION_READ_COMMITTED:
			return "READ COMMITTED";
		case java.sql.Connection.TRANSACTION_SERIALIZABLE:
			return "SERIALIZABLE";
		case java.sql.Connection.TRANSACTION_READ_UNCOMMITTED:
			return "READ UNCOMMITTED";
		case java.sql.Connection.TRANSACTION_REPEATABLE_READ:
			return "REPEATABLE READ";
		default:
			return null;
		}
	}

	public void setCatalog(String catalog) throws SQLException {
		checkClosed();
		// no-op
	}

	public String getCatalog() throws SQLException {
		checkClosed();
		return queryExecutor.getDatabase();
	}

	/**
	 * Overrides finalize(). If called, it closes the connection.
	 * <p>
	 * This was done at the request of
	 * <a href="mailto:rachel@enlarion.demon.co.uk">Rachel Greenham</a> who hit a
	 * problem where multiple clients didn't close the connection, and once a
	 * fortnight enough clients were open to kill the postgres server.
	 */
	protected void finalize() throws Throwable {
		try {
			if (openStackTrace != null) {
				LOGGER.log(Level.WARNING, GT.tr("Finalizing a Connection that was never closed:"), openStackTrace);
			}

			close();
		} finally {
			super.finalize();
		}
	}

	/**
	 * Get server version number
	 *
	 * @return server version number
	 */
	public String getDBVersionNumber() {
		return queryExecutor.getServerVersion();
	}

	/**
	 * Get server major version
	 *
	 * @return server major version
	 */
	public int getServerMajorVersion() {
		try {
			StringTokenizer versionTokens = new StringTokenizer(queryExecutor.getServerVersion(), "."); // aaXbb.ccYdd
			return integerPart(versionTokens.nextToken()); // return X
		} catch (NoSuchElementException e) {
			return 0;
		}
	}

	/**
	 * Get server minor version
	 *
	 * @return server minor version
	 */
	public int getServerMinorVersion() {
		try {
			StringTokenizer versionTokens = new StringTokenizer(queryExecutor.getServerVersion(), "."); // aaXbb.ccYdd
			versionTokens.nextToken(); // Skip aaXbb
			return integerPart(versionTokens.nextToken()); // return Y
		} catch (NoSuchElementException e) {
			return 0;
		}
	}

	public boolean haveMinimumServerVersion(int ver) {
		return queryExecutor.getServerVersionNum() >= ver;
	}

	public boolean haveMinimumServerVersion(Version ver) {
		return haveMinimumServerVersion(ver.getVersionNum());
	}

	public Encoding getEncoding() {
		return queryExecutor.getEncoding();
	}

	public byte[] encodeString(String str) throws SQLException {
		try {
			return getEncoding().encode(str);
		} catch (IOException ioe) {
			throw new PSQLException(GT.tr("Unable to translate data into the desired encoding."), PSQLState.DATA_ERROR,
					ioe);
		}
	}

	public String escapeString(String str) throws SQLException {
		return Utils.escapeLiteral(null, str, queryExecutor.getStandardConformingStrings()).toString();
	}

	public boolean getStandardConformingStrings() {
		return queryExecutor.getStandardConformingStrings();
	}

	// This is a cache of the DatabaseMetaData instance for this connection
	protected java.sql.DatabaseMetaData metadata;

	public boolean isClosed() throws SQLException {
		return queryExecutor.isClosed();
	}

	public CompletableFuture<Void> cancelQuery() throws SQLException {
		checkClosed();
		return queryExecutor.sendQueryCancel();
	}

	public CompletableFuture<PGNotification[]> getNotifications() throws SQLException {
		return getNotifications(-1);
	}

	public CompletableFuture<PGNotification[]> getNotifications(int timeoutMillis) throws SQLException {
		checkClosed();
		await(getQueryExecutor().processNotifies(timeoutMillis));
		// Backwards-compatibility hand-holding.
		PGNotification[] notifications = queryExecutor.getNotifications();
		return CompletableFuture.completedFuture(notifications.length == 0 ? null : notifications);
	}

	/**
	 * Handler for transaction queries
	 */
	private class TransactionCommandHandler extends ResultHandlerBase {
		public void handleCompletion() throws SQLException {
			SQLWarning warning = getWarning();
			if (warning != null) {
				VxConnection.this.addWarning(warning);
			}
			super.handleCompletion();
		}
	}

	public int getPrepareThreshold() {
		return prepareThreshold;
	}

	public void setDefaultFetchSize(int fetchSize) throws SQLException {
		if (fetchSize < 0) {
			throw new PSQLException(GT.tr("Fetch size must be a value greater to or equal to 0."),
					PSQLState.INVALID_PARAMETER_VALUE);
		}

		this.defaultFetchSize = fetchSize;
		LOGGER.log(Level.FINE, "  setDefaultFetchSize = {0}", fetchSize);
	}

	public int getDefaultFetchSize() {
		return defaultFetchSize;
	}

	public void setPrepareThreshold(int newThreshold) {
		this.prepareThreshold = newThreshold;
		LOGGER.log(Level.FINE, "  setPrepareThreshold = {0}", newThreshold);
	}

	public boolean getForceBinary() {
		return forcebinary;
	}

	public void setForceBinary(boolean newValue) {
		this.forcebinary = newValue;
		LOGGER.log(Level.FINE, "  setForceBinary = {0}", newValue);
	}

	public void setTypeMapImpl(Map<String, Class<?>> map) throws SQLException {
		typemap = map;
	}

	public Logger getLogger() {
		return LOGGER;
	}

	public int getProtocolVersion() {
		return queryExecutor.getProtocolVersion();
	}

	public boolean getStringVarcharFlag() {
		return bindStringAsVarchar;
	}

	private CopyManager copyManager = null;

	public CopyManager getCopyAPI() throws SQLException {
		checkClosed();
		if (copyManager == null) {
			copyManager = new CopyManager(this.createConnection());
		}
		return copyManager;
	}

	public boolean binaryTransferSend(int oid) {
		return queryExecutor.useBinaryForSend(oid);
	}

	public int getBackendPID() {
		return queryExecutor.getBackendPID();
	}

	public boolean isColumnSanitiserDisabled() {
		return this.disableColumnSanitiser;
	}

	public void setDisableColumnSanitiser(boolean disableColumnSanitiser) {
		this.disableColumnSanitiser = disableColumnSanitiser;
		LOGGER.log(Level.FINE, "  setDisableColumnSanitiser = {0}", disableColumnSanitiser);
	}

	public PreferQueryMode getPreferQueryMode() {
		return queryExecutor.getPreferQueryMode();
	}

	public AutoSave getAutosave() {
		return queryExecutor.getAutoSave();
	}

	public void setAutosave(AutoSave autoSave) {
		queryExecutor.setAutoSave(autoSave);
		LOGGER.log(Level.FINE, "  setAutosave = {0}", autoSave.value());
	}

	protected void abort() {
		queryExecutor.abort();
	}

	private synchronized Timer getTimer() {
		if (cancelTimer == null) {
			cancelTimer = Driver.getSharedTimer().getTimer();
		}
		return cancelTimer;
	}

	private synchronized void releaseTimer() {
		if (cancelTimer != null) {
			cancelTimer = null;
			Driver.getSharedTimer().releaseTimer();
		}
	}

	public void addTimerTask(TimerTask timerTask, long milliSeconds) {
		Timer timer = getTimer();
		timer.schedule(timerTask, milliSeconds);
	}

	public void purgeTimerTasks() {
		Timer timer = cancelTimer;
		if (timer != null) {
			timer.purge();
		}
	}

	public String escapeIdentifier(String identifier) throws SQLException {
		return Utils.escapeIdentifier(null, identifier).toString();
	}

	public String escapeLiteral(String literal) throws SQLException {
		return Utils.escapeLiteral(null, literal, queryExecutor.getStandardConformingStrings()).toString();
	}

	public LruCache<FieldMetadata.Key, FieldMetadata> getFieldMetadataCache() {
		return fieldMetadataCache;
	}

	public PGReplicationConnection getReplicationAPI() throws SQLException {
		return new PGReplicationConnectionImpl(this.createConnection());
	}

	private static void appendArray(StringBuilder sb, Object elements, char delim) {
		sb.append('{');

		int nElements = java.lang.reflect.Array.getLength(elements);
		for (int i = 0; i < nElements; i++) {
			if (i > 0) {
				sb.append(delim);
			}

			Object o = java.lang.reflect.Array.get(elements, i);
			if (o == null) {
				sb.append("NULL");
			} else if (o.getClass().isArray()) {
				final PrimitiveArraySupport arraySupport = PrimitiveArraySupport.getArraySupport(o);
				if (arraySupport != null) {
					arraySupport.appendArray(sb, delim, o);
				} else {
					appendArray(sb, o, delim);
				}
			} else {
				String s = o.toString();
				PgArray.escapeArrayElement(sb, s);
			}
		}
		sb.append('}');
	}

	// Parse a "dirty" integer surrounded by non-numeric characters
	private static int integerPart(String dirtyString) {
		int start;
		int end;

		for (start = 0; start < dirtyString.length() && !Character.isDigit(dirtyString.charAt(start)); ++start) {
			;
		}

		for (end = start; end < dirtyString.length() && Character.isDigit(dirtyString.charAt(end)); ++end) {
			;
		}

		if (start == end) {
			return 0;
		}

		return Integer.parseInt(dirtyString.substring(start, end));
	}

	public VxStatement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		checkClosed();
		return new VxStatement(this, resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	public VxPreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		checkClosed();
		return new VxPreparedStatement(this, sql, resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	public VxCallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		checkClosed();
		return new VxCallableStatement(this, sql, resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	public DatabaseMetaData getMetaData() throws SQLException {
		checkClosed();
		if (metadata == null) {
			metadata = new PgDatabaseMetaData(this.createConnection());
		}
		return metadata;
	}

	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		setTypeMapImpl(map);
		LOGGER.log(Level.FINE, "  setTypeMap = {0}", map);
	}

	protected Array makeArray(int oid, String fieldString) throws SQLException {
		return new PgArray(this.createConnection(), oid, fieldString);
	}

	protected Blob makeBlob(long oid) throws SQLException {
		return new PgBlob(this.createConnection(), oid);
	}

	protected Clob makeClob(long oid) throws SQLException {
		return new PgClob(new PgConnection(this.queryExecutor, this._info, this.creatingURL), oid);
	}

	protected SQLXML makeSQLXML() throws SQLException {
		return new PgSQLXML(this.createConnection());
	}

	public Clob createClob() throws SQLException {
		checkClosed();
		throw org.postgresql.Driver.notImplemented(this.getClass(), "createClob()");
	}

	public Blob createBlob() throws SQLException {
		checkClosed();
		throw org.postgresql.Driver.notImplemented(this.getClass(), "createBlob()");
	}

	public NClob createNClob() throws SQLException {
		checkClosed();
		throw org.postgresql.Driver.notImplemented(this.getClass(), "createNClob()");
	}

	public SQLXML createSQLXML() throws SQLException {
		checkClosed();
		return makeSQLXML();
	}

	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		checkClosed();
		throw org.postgresql.Driver.notImplemented(this.getClass(), "createStruct(String, Object[])");
	}

	public Array createArrayOf(String typeName, Object elements) throws SQLException {
		checkClosed();

		final TypeInfo typeInfo = getTypeInfo();

		final int oid = typeInfo.getPGArrayType(typeName);
		final char delim = typeInfo.getArrayDelimiter(oid);

		if (oid == Oid.UNSPECIFIED) {
			throw new PSQLException(GT.tr("Unable to find server array type for provided name {0}.", typeName),
					PSQLState.INVALID_NAME);
		}

		if (elements == null) {
			return makeArray(oid, null);
		}

		final String arrayString;

		final PrimitiveArraySupport arraySupport = PrimitiveArraySupport.getArraySupport(elements);

		if (arraySupport != null) {
			// if the oid for the given type matches the default type, we might be
			// able to go straight to binary representation
			if (oid == arraySupport.getDefaultArrayTypeOid(typeInfo) && arraySupport.supportBinaryRepresentation()
					&& getPreferQueryMode() != PreferQueryMode.SIMPLE) {
				return new PgArray(this.createConnection(), oid, arraySupport.toBinaryRepresentation(this.createConnection(), elements));
			}
			arrayString = arraySupport.toArrayString(delim, elements);
		} else {
			final Class<?> clazz = elements.getClass();
			if (!clazz.isArray()) {
				throw new PSQLException(GT.tr("Invalid elements {0}", elements), PSQLState.INVALID_PARAMETER_TYPE);
			}
			StringBuilder sb = new StringBuilder();
			appendArray(sb, elements, delim);
			arrayString = sb.toString();
		}

		return makeArray(oid, arrayString);
	}

	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		checkClosed();

		int oid = getTypeInfo().getPGArrayType(typeName);
		if (oid == Oid.UNSPECIFIED) {
			throw new PSQLException(GT.tr("Unable to find server array type for provided name {0}.", typeName),
					PSQLState.INVALID_NAME);
		}

		if (elements == null) {
			return makeArray(oid, null);
		}

		char delim= getTypeInfo().getArrayDelimiter(oid);
		StringBuilder sb = new StringBuilder();
		appendArray(sb, elements, delim);

		return makeArray(oid, sb.toString());
	}

	public CompletableFuture<Boolean> isValid(int timeout) throws SQLException {
		if (timeout < 0) {
			throw new PSQLException(GT.tr("Invalid timeout ({0}<0).", timeout), PSQLState.INVALID_PARAMETER_VALUE);
		}
		if (isClosed()) {
			return CompletableFuture.completedFuture(false);
		}
		try {
			if (replicationConnection) {
			  VxStatement statement = createStatement();
				await(statement.execute("IDENTIFY_SYSTEM"));
				statement.close();
			} else {
				if (checkConnectionQuery == null) {
					checkConnectionQuery = prepareStatement("");
				}
				checkConnectionQuery.setQueryTimeout(timeout);
				await(checkConnectionQuery.executeUpdate());
			}
			return CompletableFuture.completedFuture(true);
		} catch (SQLException e) {
			if (PSQLState.IN_FAILED_SQL_TRANSACTION.getState().equals(e.getSQLState())) {
				// "current transaction aborted", assume the connection is up and running
				return CompletableFuture.completedFuture(true);
			}
			LOGGER.log(Level.WARNING, GT.tr("Validating connection."), e);
		}
		return CompletableFuture.completedFuture(false);
	}

	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		try {
			checkClosed();
		} catch (final SQLException cause) {
			Map<String, ClientInfoStatus> failures = new HashMap<String, ClientInfoStatus>();
			failures.put(name, ClientInfoStatus.REASON_UNKNOWN);
			throw new SQLClientInfoException(GT.tr("This connection has been closed."), failures, cause);
		}

		if (haveMinimumServerVersion(ServerVersion.v9_0) && "ApplicationName".equals(name)) {
			if (value == null) {
				value = "";
			}
			final String oldValue = queryExecutor.getApplicationName();
			if (value.equals(oldValue)) {
				return;
			}

			try {
				StringBuilder sql = new StringBuilder("SET application_name = '");
				Utils.escapeLiteral(sql, value, getStandardConformingStrings());
				sql.append("'");
				try {
					execSQLUpdate(sql.toString()).get();
				} catch (InterruptedException | ExecutionException e) {
					throw new SQLException(e);
				}
			} catch (SQLException sqle) {
				Map<String, ClientInfoStatus> failures = new HashMap<String, ClientInfoStatus>();
				failures.put(name, ClientInfoStatus.REASON_UNKNOWN);
				throw new SQLClientInfoException(GT.tr("Failed to set ClientInfo property: {0}", "ApplicationName"),
						sqle.getSQLState(), failures, sqle);
			}
			if (LOGGER.isLoggable(Level.FINE)) {
				LOGGER.log(Level.FINE, "  setClientInfo = {0} {1}", new Object[] { name, value });
			}
			_clientInfo.put(name, value);
			return;
		}

		addWarning(new SQLWarning(GT.tr("ClientInfo property not supported."), PSQLState.NOT_IMPLEMENTED.getState()));
	}

	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		try {
			checkClosed();
		} catch (final SQLException cause) {
			Map<String, ClientInfoStatus> failures = new HashMap<String, ClientInfoStatus>();
			for (Map.Entry<Object, Object> e : properties.entrySet()) {
				failures.put((String) e.getKey(), ClientInfoStatus.REASON_UNKNOWN);
			}
			throw new SQLClientInfoException(GT.tr("This connection has been closed."), failures, cause);
		}

		Map<String, ClientInfoStatus> failures = new HashMap<String, ClientInfoStatus>();
		for (String name : new String[] { "ApplicationName" }) {
			try {
				setClientInfo(name, properties.getProperty(name, null));
			} catch (SQLClientInfoException e) {
				failures.putAll(e.getFailedProperties());
			}
		}

		if (!failures.isEmpty()) {
			throw new SQLClientInfoException(GT.tr("One ore more ClientInfo failed."),
					PSQLState.NOT_IMPLEMENTED.getState(), failures);
		}
	}

	public String getClientInfo(String name) throws SQLException {
		checkClosed();
		_clientInfo.put("ApplicationName", queryExecutor.getApplicationName());
		return _clientInfo.getProperty(name);
	}

	public Properties getClientInfo() throws SQLException {
		checkClosed();
		_clientInfo.put("ApplicationName", queryExecutor.getApplicationName());
		return _clientInfo;
	}

	public <T> T createQueryObject(Class<T> ifc) throws SQLException {
		checkClosed();
		throw org.postgresql.Driver.notImplemented(this.getClass(), "createQueryObject(Class<T>)");
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		checkClosed();
		return iface.isAssignableFrom(getClass());
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		checkClosed();
		if (iface.isAssignableFrom(getClass())) {
			return iface.cast(this);
		}
		throw new SQLException("Cannot unwrap to " + iface.getName());
	}

	public CompletableFuture<String> getSchema() throws SQLException {
		checkClosed();
		VxStatement stmt = createStatement();
		try {
			VxResultSet rs = await(stmt.executeQuery("select current_schema()"));
			try {
				if (!await(rs.next())) {
					return null; // Is it ever possible?
				}
				return rs.getString(1);
			} finally {
				rs.close();
			}
		} finally {
			stmt.close();
		}
	}

	public CompletableFuture<Void> setSchema(String schema) throws SQLException {
		checkClosed();
		VxStatement stmt = createStatement();
		try {
			if (schema == null) {
				await(stmt.executeUpdate("SET SESSION search_path TO DEFAULT"));
			} else {
				StringBuilder sb = new StringBuilder();
				sb.append("SET SESSION search_path TO '");
				Utils.escapeLiteral(sb, schema, getStandardConformingStrings());
				sb.append("'");
				await(stmt.executeUpdate(sb.toString()));
				LOGGER.log(Level.FINE, "  setSchema = {0}", schema);
			}
		} finally {
			stmt.close();
		}
		
		return CompletableFuture.completedFuture(null);
	}

	public class AbortCommand implements Runnable {
		public void run() {
			abort();
		}
	}

	public void abort(Executor executor) throws SQLException {
		if (isClosed()) {
			return;
		}

		SQL_PERMISSION_ABORT.checkGuard(this);

		AbortCommand command = new AbortCommand();
		if (executor != null) {
			executor.execute(command);
		} else {
			command.run();
		}
	}

	public void setNetworkTimeout(Executor executor /* not used */, int milliseconds) throws SQLException {
		checkClosed();

		if (milliseconds < 0) {
			throw new PSQLException(GT.tr("Network timeout must be a value greater than or equal to 0."),
					PSQLState.INVALID_PARAMETER_VALUE);
		}

		SecurityManager securityManager = System.getSecurityManager();
		if (securityManager != null) {
			securityManager.checkPermission(SQL_PERMISSION_NETWORK_TIMEOUT);
		}

		try {
			queryExecutor.setNetworkTimeout(milliseconds);
		} catch (IOException ioe) {
			throw new PSQLException(GT.tr("Unable to set network timeout."), PSQLState.COMMUNICATION_ERROR, ioe);
		}
	}

	public int getNetworkTimeout() throws SQLException {
		checkClosed();

		try {
			return queryExecutor.getNetworkTimeout();
		} catch (IOException ioe) {
			throw new PSQLException(GT.tr("Unable to get network timeout."), PSQLState.COMMUNICATION_ERROR, ioe);
		}
	}

	public void setHoldability(int holdability) throws SQLException {
		checkClosed();

		switch (holdability) {
		case java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT:
			rsHoldability = holdability;
			break;
		case java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT:
			rsHoldability = holdability;
			break;
		default:
			throw new PSQLException(GT.tr("Unknown ResultSet holdability setting: {0}.", holdability),
					PSQLState.INVALID_PARAMETER_VALUE);
		}
		LOGGER.log(Level.FINE, "  setHoldability = {0}", holdability);
	}

	public int getHoldability() throws SQLException {
		checkClosed();
		return rsHoldability;
	}

	public CompletableFuture<Savepoint> setSavepoint() throws SQLException {
		checkClosed();

		String pgName;
		if (getAutoCommit()) {
			throw new PSQLException(GT.tr("Cannot establish a savepoint in auto-commit mode."),
					PSQLState.NO_ACTIVE_SQL_TRANSACTION);
		}

		PSQLSavepoint savepoint = new PSQLSavepoint(savepointId++);
		pgName = savepoint.getPGName();

		// Note we can't use execSQLUpdate because we don't want
		// to suppress BEGIN.
		VxStatement stmt = createStatement();
		await(stmt.executeUpdate("SAVEPOINT " + pgName));
		stmt.close();

		return CompletableFuture.completedFuture(savepoint);
	}

	public CompletableFuture<Savepoint> setSavepoint(String name) throws SQLException {
		checkClosed();

		if (getAutoCommit()) {
			throw new PSQLException(GT.tr("Cannot establish a savepoint in auto-commit mode."),
					PSQLState.NO_ACTIVE_SQL_TRANSACTION);
		}

		PSQLSavepoint savepoint = new PSQLSavepoint(name);

		// Note we can't use execSQLUpdate because we don't want
		// to suppress BEGIN.
		VxStatement stmt = createStatement();
		await(stmt.executeUpdate("SAVEPOINT " + savepoint.getPGName()));
		stmt.close();

		return CompletableFuture.completedFuture(savepoint);
	}

	public void rollback(Savepoint savepoint) throws SQLException {
		checkClosed();

		PSQLSavepoint pgSavepoint = (PSQLSavepoint) savepoint;
		try {
			execSQLUpdate("ROLLBACK TO SAVEPOINT " + pgSavepoint.getPGName()).get();
		} catch (InterruptedException | ExecutionException e) {
			throw new SQLException(e);
		}
	}

	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		checkClosed();

		PSQLSavepoint pgSavepoint = (PSQLSavepoint) savepoint;
		try {
			execSQLUpdate("RELEASE SAVEPOINT " + pgSavepoint.getPGName()).get();
		} catch (InterruptedException | ExecutionException e) {
			throw new SQLException(e);
		}
		pgSavepoint.invalidate();
	}

	public VxStatement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		checkClosed();
		return createStatement(resultSetType, resultSetConcurrency, getHoldability());
	}

	public VxPreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException {
		checkClosed();
		return prepareStatement(sql, resultSetType, resultSetConcurrency, getHoldability());
	}

	public VxCallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		checkClosed();
		return prepareCall(sql, resultSetType, resultSetConcurrency, getHoldability());
	}

	public VxPreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		if (autoGeneratedKeys != java.sql.Statement.RETURN_GENERATED_KEYS) {
			return prepareStatement(sql);
		}

		return prepareStatement(sql, (String[]) null);
	}

	public VxPreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		if (columnIndexes != null && columnIndexes.length == 0) {
			return prepareStatement(sql);
		}

		checkClosed();
		throw new PSQLException(GT.tr("Returning autogenerated keys is not supported."), PSQLState.NOT_IMPLEMENTED);
	}

	public VxPreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		if (columnNames != null && columnNames.length == 0) {
			return prepareStatement(sql);
		}

		CachedQuery cachedQuery = borrowReturningQuery(sql, columnNames);
		VxPreparedStatement ps = new VxPreparedStatement(this, cachedQuery, java.sql.ResultSet.TYPE_FORWARD_ONLY,
		    java.sql.ResultSet.CONCUR_READ_ONLY, getHoldability());
		Query query = cachedQuery.query;
		SqlCommand sqlCommand = query.getSqlCommand();
		if (sqlCommand != null) {
			ps.wantsGeneratedKeysAlways = sqlCommand.isReturningKeywordPresent();
		} else {
			// If composite query is given, just ignore "generated keys" arguments
		}
		return ps;
	}
	
	public PgConnection createConnection() throws SQLException {
	  return new PgConnection(this.queryExecutor, this._info, this.creatingURL);
	}
}
