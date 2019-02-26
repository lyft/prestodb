/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.druid;

import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.ConnectionFactory;
import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcSplit;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.plugin.jdbc.ReadMapping;
import com.facebook.presto.plugin.jdbc.StandardReadMappings;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.avatica.remote.Driver;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static com.facebook.presto.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;
import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.CharType.createCharType;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.Math.max;
import static java.lang.Math.min;

public class DruidJdbcClient
        extends BaseJdbcClient
{
    @Inject
    public DruidJdbcClient(JdbcConnectorId connectorId, BaseJdbcConfig config, DruidConfig druidConfig)
            throws SQLException
    {
        // Escape char is `"` in druid
        super(connectorId, config, "\"", connectionFactory(config, druidConfig));
    }

    private static ConnectionFactory connectionFactory(BaseJdbcConfig config, DruidConfig druidConfig)
            throws SQLException
    {
        Properties connectionProperties = basicConnectionProperties(config);
        connectionProperties.setProperty("sqlTimeZone", druidConfig.getSqlTimeZone());
        return new DriverConnectionFactory(new Driver(), config.getConnectionUrl(), connectionProperties);
    }

    @Override
    public Set<String> getSchemaNames()
    {
        try (Connection connection = connectionFactory.openConnection();
                ResultSet schemas = connection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (schemas.next()) {
                String schemaName = schemas.getString("TABLE_SCHEM");
                // skip internal schemas
                if (!schemaName.equals("information_schema")) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void abortReadConnection(Connection connection)
            throws SQLException
    {
        connection.abort(directExecutor());
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {
        PreparedStatement statement = connection.prepareStatement(sql);
        return statement;
    }

    @Override
    protected ResultSet getTables(Connection connection, String schemaName, String tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        // For druid - catalog is empty.
        return metadata.getTables("",
                schemaName,
                tableName,
                null);
    }

    @Override
    public List<SchemaTableName> getTableNames(String schema)
    {
        try (Connection connection = connectionFactory.openConnection()) {
            try (ResultSet resultSet = getTables(connection, schema, null)) {
                ImmutableList.Builder<SchemaTableName> list = ImmutableList.builder();
                while (resultSet.next()) {
                    list.add(getSchemaTableName(resultSet));
                }
                return list.build();
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected SchemaTableName getSchemaTableName(ResultSet resultSet)
            throws SQLException
    {
        // Druid table/schema is case sensitive.
        return new SchemaTableName(
                resultSet.getString("TABLE_SCHEM"),
                resultSet.getString("TABLE_NAME"));
    }

    @Override
    public JdbcTableHandle getTableHandle(SchemaTableName schemaTableName)
    {
        try (Connection connection = connectionFactory.openConnection()) {
            String jdbcSchemaName = schemaTableName.getSchemaName();
            String jdbcTableName = schemaTableName.getTableName();
            try (ResultSet resultSet = getTables(connection, jdbcSchemaName, jdbcTableName)) {
                List<JdbcTableHandle> tableHandles = new ArrayList<>();
                while (resultSet.next()) {
                    tableHandles.add(new JdbcTableHandle(
                            connectorId,
                            schemaTableName,
                            "",
                            resultSet.getString("TABLE_SCHEM"),
                            resultSet.getString("TABLE_NAME")));
                }
                if (tableHandles.isEmpty()) {
                    return null;
                }
                if (tableHandles.size() > 1) {
                    throw new PrestoException(NOT_SUPPORTED, "Multiple tables matched: " + schemaTableName);
                }
                return getOnlyElement(tableHandles);
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public Optional<ReadMapping> toPrestoType(ConnectorSession session, JdbcTypeHandle typeHandle)
    {
        return jdbcTypeToPrestoType(typeHandle);
    }

    public static Optional<ReadMapping> jdbcTypeToPrestoType(JdbcTypeHandle type)
    {
        int columnSize = type.getColumnSize();
        switch (type.getJdbcType()) {
            case Types.BIT:
            case Types.BOOLEAN:
                return Optional.of(StandardReadMappings.booleanReadMapping());

            case Types.TINYINT:
                return Optional.of(StandardReadMappings.tinyintReadMapping());

            case Types.SMALLINT:
                return Optional.of(StandardReadMappings.smallintReadMapping());

            case Types.INTEGER:
                return Optional.of(StandardReadMappings.integerReadMapping());

            case Types.BIGINT:
                return Optional.of(StandardReadMappings.bigintReadMapping());

            case Types.REAL:
                return Optional.of(StandardReadMappings.realReadMapping());

            case Types.FLOAT:
            case Types.DOUBLE:
                return Optional.of(StandardReadMappings.doubleReadMapping());

            case Types.NUMERIC:
            case Types.DECIMAL:
                int decimalDigits = type.getDecimalDigits();
                int precision = columnSize + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
                if (precision > Decimals.MAX_PRECISION) {
                    return Optional.empty();
                }
                return Optional.of(StandardReadMappings.decimalReadMapping(createDecimalType(precision, max(decimalDigits, 0))));

            case Types.CHAR:
            case Types.NCHAR:
                // TODO this is wrong, we're going to construct malformed Slice representation if source > charLength
                int charLength = min(columnSize, CharType.MAX_LENGTH);
                return Optional.of(StandardReadMappings.charReadMapping(createCharType(charLength)));

            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                // TODO: This line is the reason for overriding this method. Druid Result set returning -1 columnSize.
                if (columnSize > VarcharType.MAX_LENGTH || columnSize == -1) {
                    return Optional.of(StandardReadMappings.varcharReadMapping(createUnboundedVarcharType()));
                }
                return Optional.of(StandardReadMappings.varcharReadMapping(createVarcharType(columnSize)));

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return Optional.of(StandardReadMappings.varbinaryReadMapping());

            case Types.DATE:
                return Optional.of(StandardReadMappings.dateReadMapping());

            case Types.TIME:
                return Optional.of(StandardReadMappings.timeReadMapping());

            case Types.TIMESTAMP:
                return Optional.of(StandardReadMappings.timestampReadMapping());
        }
        return Optional.empty();
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        try (Connection connection = connectionFactory.openConnection()) {
            try (ResultSet resultSet = getColumns(tableHandle, connection.getMetaData())) {
                List<JdbcColumnHandle> columns = new ArrayList<>();
                while (resultSet.next()) {
                    JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                            resultSet.getInt("DATA_TYPE"),
                            resultSet.getInt("COLUMN_SIZE"),
                            resultSet.getInt("DECIMAL_DIGITS"));
                    Optional<ReadMapping> columnMapping = toPrestoType(session, typeHandle);
                    // skip unsupported column types
                    if (columnMapping.isPresent()) {
                        String columnName = resultSet.getString("COLUMN_NAME");
                        columns.add(new JdbcColumnHandle(connectorId, columnName, typeHandle, columnMapping.get().getType()));
                    }
                }
                if (columns.isEmpty()) {
                    // In rare cases (e.g. PostgreSQL) a table might have no columns.
                    throw new TableNotFoundException(tableHandle.getSchemaTableName());
                }
                return ImmutableList.copyOf(columns);
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    private ResultSet getColumns(JdbcTableHandle tableHandle, DatabaseMetaData metadata)
            throws SQLException
    {
        // TODO: escape is not letting druid find columns.
        return metadata.getColumns(
            tableHandle.getCatalogName(),
            tableHandle.getSchemaName(),
            tableHandle.getTableName(),
            null);
    }

    /**
     * Overriding this method till this is fixed : https://issues.apache.org/jira/browse/CALCITE-2873
     * @param connection
     * @param split
     * @param columnHandles
     * @return
     * @throws SQLException
     */
    @Override
    public PreparedStatement buildSql(Connection connection, JdbcSplit split, List<JdbcColumnHandle> columnHandles)
            throws SQLException
    {
        return new DruidQueryBuilder(identifierQuote).buildSql(
            this,
            connection,
            split.getCatalogName(),
            split.getSchemaName(),
            split.getTableName(),
            columnHandles,
            split.getTupleDomain());
    }
}
