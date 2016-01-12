package jailer.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;

public class JailerDatabaseMetaData implements DatabaseMetaData{
	private final DatabaseMetaData realDatabaseMetaData;
	private final String url;
	
	public JailerDatabaseMetaData(DatabaseMetaData realDatabaseMetaData, String url){
		this.realDatabaseMetaData = realDatabaseMetaData;
		this.url = url;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return realDatabaseMetaData.isWrapperFor(iface);
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return realDatabaseMetaData.unwrap(iface);
	}

	@Override
	public boolean allProceduresAreCallable() throws SQLException {
		return realDatabaseMetaData.allProceduresAreCallable();
	}

	@Override
	public boolean allTablesAreSelectable() throws SQLException {
		return realDatabaseMetaData.allTablesAreSelectable();
	}

	@Override
	public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
		return realDatabaseMetaData.autoCommitFailureClosesAllResultSets();
	}

	@Override
	public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
		return realDatabaseMetaData.dataDefinitionCausesTransactionCommit();
	}

	@Override
	public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
		return realDatabaseMetaData.dataDefinitionIgnoredInTransactions();
	}

	@Override
	public boolean deletesAreDetected(int type) throws SQLException {
		return realDatabaseMetaData.deletesAreDetected(type);
	}

	@Override
	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
		return realDatabaseMetaData.doesMaxRowSizeIncludeBlobs();
	}

	@Override
	public boolean generatedKeyAlwaysReturned() throws SQLException {
		return realDatabaseMetaData.generatedKeyAlwaysReturned();
	}

	@Override
	public ResultSet getAttributes(String catalog, String schemaPattern,
			String typeNamePattern, String attributeNamePattern)
			throws SQLException {
		return realDatabaseMetaData.getAttributes(catalog, schemaPattern, typeNamePattern, attributeNamePattern);
	}

	@Override
	public ResultSet getBestRowIdentifier(String catalog, String schema,
			String table, int scope, boolean nullable) throws SQLException {
		return realDatabaseMetaData.getBestRowIdentifier(catalog, schema, table, scope, nullable);
	}

	@Override
	public String getCatalogSeparator() throws SQLException {
		return realDatabaseMetaData.getCatalogSeparator();
	}

	@Override
	public String getCatalogTerm() throws SQLException {
		return realDatabaseMetaData.getCatalogTerm();
	}

	@Override
	public ResultSet getCatalogs() throws SQLException {
		return realDatabaseMetaData.getCatalogs();
	}

	@Override
	public ResultSet getClientInfoProperties() throws SQLException {
		return realDatabaseMetaData.getClientInfoProperties();
	}

	@Override
	public ResultSet getColumnPrivileges(String catalog, String schema,
			String table, String columnNamePattern) throws SQLException {
		return realDatabaseMetaData.getColumnPrivileges(catalog, schema, table, columnNamePattern);
	}

	@Override
	public ResultSet getColumns(String catalog, String schemaPattern,
			String tableNamePattern, String columnNamePattern)
			throws SQLException {
		return realDatabaseMetaData.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
	}

	@Override
	public Connection getConnection() throws SQLException {
		return realDatabaseMetaData.getConnection();
	}

	@Override
	public ResultSet getCrossReference(String parentCatalog,
			String parentSchema, String parentTable, String foreignCatalog,
			String foreignSchema, String foreignTable) throws SQLException {
		return realDatabaseMetaData.getCrossReference(parentCatalog, parentSchema, parentTable, foreignCatalog, foreignSchema, foreignTable);
	}

	@Override
	public int getDatabaseMajorVersion() throws SQLException {
		return realDatabaseMetaData.getDatabaseMajorVersion();
	}

	@Override
	public int getDatabaseMinorVersion() throws SQLException {
		return realDatabaseMetaData.getDatabaseMinorVersion();
	}

	@Override
	public String getDatabaseProductName() throws SQLException {
		return realDatabaseMetaData.getDatabaseProductName();
	}

	@Override
	public String getDatabaseProductVersion() throws SQLException {
		return realDatabaseMetaData.getDatabaseProductVersion();
	}

	@Override
	public int getDefaultTransactionIsolation() throws SQLException {
		return realDatabaseMetaData.getDefaultTransactionIsolation();
	}

	@Override
	public int getDriverMajorVersion() {
		return realDatabaseMetaData.getDriverMajorVersion();
	}

	@Override
	public int getDriverMinorVersion() {
		return realDatabaseMetaData.getDriverMinorVersion();
	}

	@Override
	public String getDriverName() throws SQLException {
		return realDatabaseMetaData.getDriverName();
	}

	@Override
	public String getDriverVersion() throws SQLException {
		return realDatabaseMetaData.getDriverVersion();
	}

	@Override
	public ResultSet getExportedKeys(String catalog, String schema, String table)
			throws SQLException {
		return realDatabaseMetaData.getExportedKeys(catalog, schema, table);
	}

	@Override
	public String getExtraNameCharacters() throws SQLException {
		return realDatabaseMetaData.getExtraNameCharacters();
	}

	@Override
	public ResultSet getFunctionColumns(String catalog, String schemaPattern,
			String functionNamePattern, String columnNamePattern)
			throws SQLException {
		return realDatabaseMetaData.getFunctionColumns(catalog, schemaPattern, functionNamePattern, columnNamePattern);
	}

	@Override
	public ResultSet getFunctions(String catalog, String schemaPattern,
			String functionNamePattern) throws SQLException {
		return realDatabaseMetaData.getFunctions(catalog, schemaPattern, functionNamePattern);
	}

	@Override
	public String getIdentifierQuoteString() throws SQLException {
		return realDatabaseMetaData.getIdentifierQuoteString();
	}

	@Override
	public ResultSet getImportedKeys(String catalog, String schema, String table)
			throws SQLException {
		return realDatabaseMetaData.getImportedKeys(catalog, schema, table);
	}

	@Override
	public ResultSet getIndexInfo(String catalog, String schema, String table,
			boolean unique, boolean approximate) throws SQLException {
		return realDatabaseMetaData.getIndexInfo(catalog, schema, table, unique, approximate);
	}

	@Override
	public int getJDBCMajorVersion() throws SQLException {
		return realDatabaseMetaData.getJDBCMajorVersion();
	}

	@Override
	public int getJDBCMinorVersion() throws SQLException {
		return realDatabaseMetaData.getJDBCMinorVersion();
	}

	@Override
	public int getMaxBinaryLiteralLength() throws SQLException {
		return realDatabaseMetaData.getMaxBinaryLiteralLength();
	}

	@Override
	public int getMaxCatalogNameLength() throws SQLException {
		return realDatabaseMetaData.getMaxCatalogNameLength();
	}

	@Override
	public int getMaxCharLiteralLength() throws SQLException {
		return realDatabaseMetaData.getMaxCharLiteralLength();
	}

	@Override
	public int getMaxColumnNameLength() throws SQLException {
		return realDatabaseMetaData.getMaxColumnNameLength();
	}

	@Override
	public int getMaxColumnsInGroupBy() throws SQLException {
		return realDatabaseMetaData.getMaxColumnsInGroupBy();
	}

	@Override
	public int getMaxColumnsInIndex() throws SQLException {
		return realDatabaseMetaData.getMaxColumnsInIndex();
	}

	@Override
	public int getMaxColumnsInOrderBy() throws SQLException {
		return realDatabaseMetaData.getMaxColumnsInOrderBy();
	}

	@Override
	public int getMaxColumnsInSelect() throws SQLException {
		return realDatabaseMetaData.getMaxColumnsInSelect();
	}

	@Override
	public int getMaxColumnsInTable() throws SQLException {
		return realDatabaseMetaData.getMaxColumnsInTable();
	}

	@Override
	public int getMaxConnections() throws SQLException {
		return realDatabaseMetaData.getMaxConnections();
	}

	@Override
	public int getMaxCursorNameLength() throws SQLException {
		return realDatabaseMetaData.getMaxCursorNameLength();
	}

	@Override
	public int getMaxIndexLength() throws SQLException {
		return realDatabaseMetaData.getMaxIndexLength();
	}

	@Override
	public int getMaxProcedureNameLength() throws SQLException {
		return realDatabaseMetaData.getMaxProcedureNameLength();
	}

	@Override
	public int getMaxRowSize() throws SQLException {
		return realDatabaseMetaData.getMaxRowSize();
	}

	@Override
	public int getMaxSchemaNameLength() throws SQLException {
		return realDatabaseMetaData.getMaxSchemaNameLength();
	}

	@Override
	public int getMaxStatementLength() throws SQLException {
		return realDatabaseMetaData.getMaxStatementLength();
	}

	@Override
	public int getMaxStatements() throws SQLException {
		return realDatabaseMetaData.getMaxStatements();
	}

	@Override
	public int getMaxTableNameLength() throws SQLException {
		return realDatabaseMetaData.getMaxTableNameLength();
	}

	@Override
	public int getMaxTablesInSelect() throws SQLException {
		return realDatabaseMetaData.getMaxTablesInSelect();
	}

	@Override
	public int getMaxUserNameLength() throws SQLException {
		return realDatabaseMetaData.getMaxUserNameLength();
	}

	@Override
	public String getNumericFunctions() throws SQLException {
		return realDatabaseMetaData.getNumericFunctions();
	}

	@Override
	public ResultSet getPrimaryKeys(String catalog, String schema, String table)
			throws SQLException {
		return realDatabaseMetaData.getPrimaryKeys(catalog, schema, table);
	}

	@Override
	public ResultSet getProcedureColumns(String catalog, String schemaPattern,
			String procedureNamePattern, String columnNamePattern)
			throws SQLException {
		return realDatabaseMetaData.getProcedureColumns(catalog, schemaPattern, procedureNamePattern, columnNamePattern);
	}

	@Override
	public String getProcedureTerm() throws SQLException {
		return realDatabaseMetaData.getProcedureTerm();
	}

	@Override
	public ResultSet getProcedures(String catalog, String schemaPattern,
			String procedureNamePattern) throws SQLException {
		return realDatabaseMetaData.getProcedures(catalog, schemaPattern, procedureNamePattern);
	}

	@Override
	public ResultSet getPseudoColumns(String catalog, String schemaPattern,
			String tableNamePattern, String columnNamePattern)
			throws SQLException {
		return realDatabaseMetaData.getPseudoColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		return realDatabaseMetaData.getResultSetHoldability();
	}

	@Override
	public RowIdLifetime getRowIdLifetime() throws SQLException {
		return realDatabaseMetaData.getRowIdLifetime();
	}

	@Override
	public String getSQLKeywords() throws SQLException {
		return realDatabaseMetaData.getSQLKeywords();
	}

	@Override
	public int getSQLStateType() throws SQLException {
		return realDatabaseMetaData.getSQLStateType();
	}

	@Override
	public String getSchemaTerm() throws SQLException {
		return realDatabaseMetaData.getSchemaTerm();
	}

	@Override
	public ResultSet getSchemas() throws SQLException {
		return realDatabaseMetaData.getSchemas();
	}

	@Override
	public ResultSet getSchemas(String catalog, String schemaPattern)
			throws SQLException {
		return realDatabaseMetaData.getSchemas(catalog, schemaPattern);
	}

	@Override
	public String getSearchStringEscape() throws SQLException {
		return realDatabaseMetaData.getSearchStringEscape();
	}

	@Override
	public String getStringFunctions() throws SQLException {
		return realDatabaseMetaData.getStringFunctions();
	}

	@Override
	public ResultSet getSuperTables(String catalog, String schemaPattern,
			String tableNamePattern) throws SQLException {
		return realDatabaseMetaData.getSuperTables(catalog, schemaPattern, tableNamePattern);
	}

	@Override
	public ResultSet getSuperTypes(String catalog, String schemaPattern,
			String typeNamePattern) throws SQLException {
		return realDatabaseMetaData.getSuperTypes(catalog, schemaPattern, typeNamePattern);
	}

	@Override
	public String getSystemFunctions() throws SQLException {
		return realDatabaseMetaData.getSystemFunctions();
	}

	@Override
	public ResultSet getTablePrivileges(String catalog, String schemaPattern,
			String tableNamePattern) throws SQLException {
		return realDatabaseMetaData.getTablePrivileges(catalog, schemaPattern, tableNamePattern);
	}

	@Override
	public ResultSet getTableTypes() throws SQLException {
		return realDatabaseMetaData.getTableTypes();
	}

	@Override
	public ResultSet getTables(String catalog, String schemaPattern,
			String tableNamePattern, String[] types) throws SQLException {
		return realDatabaseMetaData.getTables(catalog, schemaPattern, tableNamePattern, types);
	}

	@Override
	public String getTimeDateFunctions() throws SQLException {
		return realDatabaseMetaData.getTimeDateFunctions();
	}

	@Override
	public ResultSet getTypeInfo() throws SQLException {
		return realDatabaseMetaData.getTypeInfo();
	}

	@Override
	public ResultSet getUDTs(String catalog, String schemaPattern,
			String typeNamePattern, int[] types) throws SQLException {
		return realDatabaseMetaData.getUDTs(catalog, schemaPattern, typeNamePattern, types);
	}

	@Override
	public String getURL() throws SQLException {
		return this.url;
	}

	@Override
	public String getUserName() throws SQLException {
		return null;
	}

	@Override
	public ResultSet getVersionColumns(String catalog, String schema,
			String table) throws SQLException {
		return realDatabaseMetaData.getVersionColumns(catalog, schema, table);
	}

	@Override
	public boolean insertsAreDetected(int type) throws SQLException {
		return realDatabaseMetaData.insertsAreDetected(type);
	}

	@Override
	public boolean isCatalogAtStart() throws SQLException {
		return realDatabaseMetaData.isCatalogAtStart();
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return realDatabaseMetaData.isReadOnly();
	}

	@Override
	public boolean locatorsUpdateCopy() throws SQLException {
		return realDatabaseMetaData.locatorsUpdateCopy();
	}

	@Override
	public boolean nullPlusNonNullIsNull() throws SQLException {
		return realDatabaseMetaData.nullPlusNonNullIsNull();
	}

	@Override
	public boolean nullsAreSortedAtEnd() throws SQLException {
		return realDatabaseMetaData.nullsAreSortedAtEnd();
	}

	@Override
	public boolean nullsAreSortedAtStart() throws SQLException {
		return realDatabaseMetaData.nullsAreSortedAtStart();
	}

	@Override
	public boolean nullsAreSortedHigh() throws SQLException {
		return realDatabaseMetaData.nullsAreSortedHigh();
	}

	@Override
	public boolean nullsAreSortedLow() throws SQLException {
		return realDatabaseMetaData.nullsAreSortedLow();
	}

	@Override
	public boolean othersDeletesAreVisible(int type) throws SQLException {
		return realDatabaseMetaData.othersDeletesAreVisible(type);
	}

	@Override
	public boolean othersInsertsAreVisible(int type) throws SQLException {
		return realDatabaseMetaData.othersInsertsAreVisible(type);
	}

	@Override
	public boolean othersUpdatesAreVisible(int type) throws SQLException {
		return realDatabaseMetaData.othersUpdatesAreVisible(type);
	}

	@Override
	public boolean ownDeletesAreVisible(int type) throws SQLException {
		return realDatabaseMetaData.ownDeletesAreVisible(type);
	}

	@Override
	public boolean ownInsertsAreVisible(int type) throws SQLException {
		return realDatabaseMetaData.ownInsertsAreVisible(type);
	}

	@Override
	public boolean ownUpdatesAreVisible(int type) throws SQLException {
		return realDatabaseMetaData.ownUpdatesAreVisible(type);
	}

	@Override
	public boolean storesLowerCaseIdentifiers() throws SQLException {
		return realDatabaseMetaData.storesLowerCaseIdentifiers();
	}

	@Override
	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
		return realDatabaseMetaData.storesLowerCaseQuotedIdentifiers();
	}

	@Override
	public boolean storesMixedCaseIdentifiers() throws SQLException {
		return realDatabaseMetaData.storesMixedCaseIdentifiers();
	}

	@Override
	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
		return realDatabaseMetaData.storesMixedCaseQuotedIdentifiers();
	}

	@Override
	public boolean storesUpperCaseIdentifiers() throws SQLException {
		return realDatabaseMetaData.storesUpperCaseIdentifiers();
	}

	@Override
	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
		return realDatabaseMetaData.storesUpperCaseQuotedIdentifiers();
	}

	@Override
	public boolean supportsANSI92EntryLevelSQL() throws SQLException {
		return realDatabaseMetaData.supportsANSI92EntryLevelSQL();
	}

	@Override
	public boolean supportsANSI92FullSQL() throws SQLException {
		return realDatabaseMetaData.supportsANSI92FullSQL();
	}

	@Override
	public boolean supportsANSI92IntermediateSQL() throws SQLException {
		return realDatabaseMetaData.supportsANSI92IntermediateSQL();
	}

	@Override
	public boolean supportsAlterTableWithAddColumn() throws SQLException {
		return realDatabaseMetaData.supportsAlterTableWithAddColumn();
	}

	@Override
	public boolean supportsAlterTableWithDropColumn() throws SQLException {
		return realDatabaseMetaData.supportsAlterTableWithDropColumn();
	}

	@Override
	public boolean supportsBatchUpdates() throws SQLException {
		return realDatabaseMetaData.supportsBatchUpdates();
	}

	@Override
	public boolean supportsCatalogsInDataManipulation() throws SQLException {
		return realDatabaseMetaData.supportsCatalogsInDataManipulation();
	}

	@Override
	public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
		return realDatabaseMetaData.supportsCatalogsInIndexDefinitions();
	}

	@Override
	public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
		return realDatabaseMetaData.supportsCatalogsInPrivilegeDefinitions();
	}

	@Override
	public boolean supportsCatalogsInProcedureCalls() throws SQLException {
		return realDatabaseMetaData.supportsCatalogsInProcedureCalls();
	}

	@Override
	public boolean supportsCatalogsInTableDefinitions() throws SQLException {
		return realDatabaseMetaData.supportsCatalogsInTableDefinitions();
	}

	@Override
	public boolean supportsColumnAliasing() throws SQLException {
		return realDatabaseMetaData.supportsColumnAliasing();
	}

	@Override
	public boolean supportsConvert() throws SQLException {
		return realDatabaseMetaData.supportsConvert();
	}

	@Override
	public boolean supportsConvert(int fromType, int toType)
			throws SQLException {
		return realDatabaseMetaData.supportsConvert(fromType, toType);
	}

	@Override
	public boolean supportsCoreSQLGrammar() throws SQLException {
		return realDatabaseMetaData.supportsCoreSQLGrammar();
	}

	@Override
	public boolean supportsCorrelatedSubqueries() throws SQLException {
		return realDatabaseMetaData.supportsCorrelatedSubqueries();
	}

	@Override
	public boolean supportsDataDefinitionAndDataManipulationTransactions()
			throws SQLException {
		return realDatabaseMetaData.supportsDataDefinitionAndDataManipulationTransactions();
	}

	@Override
	public boolean supportsDataManipulationTransactionsOnly()
			throws SQLException {
		return realDatabaseMetaData.supportsDataManipulationTransactionsOnly();
	}

	@Override
	public boolean supportsDifferentTableCorrelationNames() throws SQLException {
		return realDatabaseMetaData.supportsDifferentTableCorrelationNames();
	}

	@Override
	public boolean supportsExpressionsInOrderBy() throws SQLException {
		return realDatabaseMetaData.supportsExpressionsInOrderBy();
	}

	@Override
	public boolean supportsExtendedSQLGrammar() throws SQLException {
		return realDatabaseMetaData.supportsExtendedSQLGrammar();
	}

	@Override
	public boolean supportsFullOuterJoins() throws SQLException {
		return realDatabaseMetaData.supportsFullOuterJoins();
	}

	@Override
	public boolean supportsGetGeneratedKeys() throws SQLException {
		return realDatabaseMetaData.supportsGetGeneratedKeys();
	}

	@Override
	public boolean supportsGroupBy() throws SQLException {
		return realDatabaseMetaData.supportsGroupBy();
	}

	@Override
	public boolean supportsGroupByBeyondSelect() throws SQLException {
		return realDatabaseMetaData.supportsGroupByBeyondSelect();
	}

	@Override
	public boolean supportsGroupByUnrelated() throws SQLException {
		return realDatabaseMetaData.supportsGroupByUnrelated();
	}

	@Override
	public boolean supportsIntegrityEnhancementFacility() throws SQLException {
		return realDatabaseMetaData.supportsIntegrityEnhancementFacility();
	}

	@Override
	public boolean supportsLikeEscapeClause() throws SQLException {
		return realDatabaseMetaData.supportsLikeEscapeClause();
	}

	@Override
	public boolean supportsLimitedOuterJoins() throws SQLException {
		return realDatabaseMetaData.supportsLimitedOuterJoins();
	}

	@Override
	public boolean supportsMinimumSQLGrammar() throws SQLException {
		return realDatabaseMetaData.supportsMinimumSQLGrammar();
	}

	@Override
	public boolean supportsMixedCaseIdentifiers() throws SQLException {
		return realDatabaseMetaData.supportsMixedCaseIdentifiers();
	}

	@Override
	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
		return realDatabaseMetaData.supportsMixedCaseQuotedIdentifiers();
	}

	@Override
	public boolean supportsMultipleOpenResults() throws SQLException {
		return realDatabaseMetaData.supportsMultipleOpenResults();
	}

	@Override
	public boolean supportsMultipleResultSets() throws SQLException {
		return realDatabaseMetaData.supportsMultipleResultSets();
	}

	@Override
	public boolean supportsMultipleTransactions() throws SQLException {
		return realDatabaseMetaData.supportsMultipleTransactions();
	}

	@Override
	public boolean supportsNamedParameters() throws SQLException {
		return realDatabaseMetaData.supportsNamedParameters();
	}

	@Override
	public boolean supportsNonNullableColumns() throws SQLException {
		return realDatabaseMetaData.supportsNonNullableColumns();
	}

	@Override
	public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
		return realDatabaseMetaData.supportsOpenCursorsAcrossCommit();
	}

	@Override
	public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
		return realDatabaseMetaData.supportsOpenCursorsAcrossRollback();
	}

	@Override
	public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
		return realDatabaseMetaData.supportsOpenStatementsAcrossCommit();
	}

	@Override
	public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
		return realDatabaseMetaData.supportsOpenStatementsAcrossRollback();
	}

	@Override
	public boolean supportsOrderByUnrelated() throws SQLException {
		return realDatabaseMetaData.supportsOrderByUnrelated();
	}

	@Override
	public boolean supportsOuterJoins() throws SQLException {
		return realDatabaseMetaData.supportsOuterJoins();
	}

	@Override
	public boolean supportsPositionedDelete() throws SQLException {
		return realDatabaseMetaData.supportsPositionedDelete();
	}

	@Override
	public boolean supportsPositionedUpdate() throws SQLException {
		return realDatabaseMetaData.supportsPositionedUpdate();
	}

	@Override
	public boolean supportsResultSetConcurrency(int type, int concurrency)
			throws SQLException {
		return realDatabaseMetaData.supportsResultSetConcurrency(type, concurrency);
	}

	@Override
	public boolean supportsResultSetHoldability(int holdability)
			throws SQLException {
		return realDatabaseMetaData.supportsResultSetHoldability(holdability);
	}

	@Override
	public boolean supportsResultSetType(int type) throws SQLException {
		return realDatabaseMetaData.supportsResultSetType(type);
	}

	@Override
	public boolean supportsSavepoints() throws SQLException {
		return realDatabaseMetaData.supportsSavepoints();
	}

	@Override
	public boolean supportsSchemasInDataManipulation() throws SQLException {
		return realDatabaseMetaData.supportsSchemasInDataManipulation();
	}

	@Override
	public boolean supportsSchemasInIndexDefinitions() throws SQLException {
		return realDatabaseMetaData.supportsSchemasInIndexDefinitions();
	}

	@Override
	public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
		return realDatabaseMetaData.supportsSchemasInPrivilegeDefinitions();
	}

	@Override
	public boolean supportsSchemasInProcedureCalls() throws SQLException {
		return realDatabaseMetaData.supportsSchemasInProcedureCalls();
	}

	@Override
	public boolean supportsSchemasInTableDefinitions() throws SQLException {
		return realDatabaseMetaData.supportsSchemasInTableDefinitions();
	}

	@Override
	public boolean supportsSelectForUpdate() throws SQLException {
		return realDatabaseMetaData.supportsSelectForUpdate();
	}

	@Override
	public boolean supportsStatementPooling() throws SQLException {
		return realDatabaseMetaData.supportsStatementPooling();
	}

	@Override
	public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
		return realDatabaseMetaData.supportsStoredFunctionsUsingCallSyntax();
	}

	@Override
	public boolean supportsStoredProcedures() throws SQLException {
		return realDatabaseMetaData.supportsStoredProcedures();
	}

	@Override
	public boolean supportsSubqueriesInComparisons() throws SQLException {
		return realDatabaseMetaData.supportsSubqueriesInComparisons();
	}

	@Override
	public boolean supportsSubqueriesInExists() throws SQLException {
		return realDatabaseMetaData.supportsSubqueriesInExists();
	}

	@Override
	public boolean supportsSubqueriesInIns() throws SQLException {
		return realDatabaseMetaData.supportsSubqueriesInIns();
	}

	@Override
	public boolean supportsSubqueriesInQuantifieds() throws SQLException {
		return realDatabaseMetaData.supportsSubqueriesInQuantifieds();
	}

	@Override
	public boolean supportsTableCorrelationNames() throws SQLException {
		return realDatabaseMetaData.supportsTableCorrelationNames();
	}

	@Override
	public boolean supportsTransactionIsolationLevel(int level)
			throws SQLException {
		return realDatabaseMetaData.supportsTransactionIsolationLevel(level);
	}

	@Override
	public boolean supportsTransactions() throws SQLException {
		return realDatabaseMetaData.supportsTransactions();
	}

	@Override
	public boolean supportsUnion() throws SQLException {
		return realDatabaseMetaData.supportsUnion();
	}

	@Override
	public boolean supportsUnionAll() throws SQLException {
		return realDatabaseMetaData.supportsUnionAll();
	}

	@Override
	public boolean updatesAreDetected(int type) throws SQLException {
		return realDatabaseMetaData.updatesAreDetected(type);
	}

	@Override
	public boolean usesLocalFilePerTable() throws SQLException {
		return realDatabaseMetaData.usesLocalFilePerTable();
	}

	@Override
	public boolean usesLocalFiles() throws SQLException {
		return realDatabaseMetaData.usesLocalFiles();
	}

}
