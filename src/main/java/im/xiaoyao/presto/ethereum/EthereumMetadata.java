package im.xiaoyao.presto.ethereum;

import com.facebook.presto.common.type.*;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Marker;
import com.facebook.presto.common.predicate.Range;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import im.xiaoyao.presto.ethereum.handle.EthereumColumnHandle;
import im.xiaoyao.presto.ethereum.handle.EthereumTableHandle;
import im.xiaoyao.presto.ethereum.handle.EthereumTableLayoutHandle;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import jdk.internal.net.http.common.Pair;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameter;

import javax.inject.Inject;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static im.xiaoyao.presto.ethereum.handle.EthereumHandleResolver.convertColumnHandle;
import static im.xiaoyao.presto.ethereum.handle.EthereumHandleResolver.convertTableHandle;
import static java.util.Objects.requireNonNull;

public class EthereumMetadata implements ConnectorMetadata {
    private static final Logger log = Logger.get(EthereumMetadata.class);

    private static final String DEFAULT_SCHEMA = "default";
    public static final int H8_BYTE_HASH_STRING_LENGTH = 2 + 8 * 2;
    public static final int H32_BYTE_HASH_STRING_LENGTH = 2 + 32 * 2;
    public static final int H256_BYTE_HASH_STRING_LENGTH = 2 + 256 * 2;
    public static final int H20_BYTE_HASH_STRING_LENGTH = 2 + 20 * 2;

    private final Web3j web3j;

    @Inject
    public EthereumMetadata(
            EthereumWeb3jProvider provider
    ) {
        this.web3j = requireNonNull(provider, "provider is null").getWeb3j();
    }

    /**
     * Get schemas
     * @param session
     * @return available schemas
     */
    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return Collections.singletonList(DEFAULT_SCHEMA);
    }

    /**
     * Get handle for table
     * @param session
     * @param schemaTableName table name
     * @return table handle
     */
    @Override
    public EthereumTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName) {
        if (EthereumTable.BLOCK.getName().equals(schemaTableName.getTableName())) {
            return new EthereumTableHandle(DEFAULT_SCHEMA, EthereumTable.BLOCK.getName());
        } else if (EthereumTable.TRANSACTION.getName().equals(schemaTableName.getTableName())) {
            return new EthereumTableHandle(DEFAULT_SCHEMA, EthereumTable.TRANSACTION.getName());
        } else if (EthereumTable.ERC20.getName().equals(schemaTableName.getTableName())) {
            return new EthereumTableHandle(DEFAULT_SCHEMA, EthereumTable.ERC20.getName());
        } else {
            throw new IllegalArgumentException("Unknown Table Name " + schemaTableName.getTableName());
        }
    }

    /**
     * Returns all tables
     * @param session
     * @param schemaName schema to return the tables of
     * @return list of tables
     */
    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        return ImmutableList.of(new SchemaTableName(DEFAULT_SCHEMA, EthereumTable.BLOCK.getName()),
                new SchemaTableName(DEFAULT_SCHEMA, EthereumTable.TRANSACTION.getName()),
                new SchemaTableName(DEFAULT_SCHEMA, EthereumTable.ERC20.getName()));
    }


    /**
     * Returns all tables
     * @param session
     * @param schemaNameOrNull schema to return the tables of
     * @return list of tables
     */
    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        return listTables(session, Optional.ofNullable(schemaNameOrNull));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        List<SchemaTableName> tableNames = prefix.getSchemaName() == null
                ? listTables(session, (String) null)
                : ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));

        return tableNames.stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        (tableName) -> getTableMetadata(tableName).getColumns())
                );
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle
    ) {
        convertTableHandle(tableHandle);
        return convertColumnHandle(columnHandle).getColumnMetadata();
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns
    ) {
        ImmutableList.Builder<EthereumBlockRange> builder = ImmutableList.builder();

        Optional<Map<ColumnHandle, Domain>> domains = constraint.getSummary().getDomains();
        if (domains.isPresent()) {
            Map<ColumnHandle, Domain> columnHandleDomainMap = domains.get();
            for (Map.Entry<ColumnHandle, Domain> entry : columnHandleDomainMap.entrySet()) {
                if (entry.getKey() instanceof EthereumColumnHandle
                        && (((EthereumColumnHandle) entry.getKey()).getName().equals("block_number")
                        || ((EthereumColumnHandle) entry.getKey()).getName().equals("tx_blockNumber")
                        || ((EthereumColumnHandle) entry.getKey()).getName().equals("erc20_blockNumber"))) {
                    entry.getValue().getValues().getRanges().getOrderedRanges().forEach(r -> {
                        Marker low = r.getLow();
                        Marker high = r.getHigh();
                        builder.add(EthereumBlockRange.fromMarkers(low, high));
                    });
                } else if (entry.getKey() instanceof EthereumColumnHandle
                        && (((EthereumColumnHandle) entry.getKey()).getName().equals("block_hash")
                        || ((EthereumColumnHandle) entry.getKey()).getName().equals("tx_blockHash"))) {
                    entry.getValue().getValues().getRanges().getOrderedRanges().stream()
                            .filter(Range::isSingleValue).forEach(r -> {
                                String blockHash = ((Slice) r.getSingleValue()).toStringUtf8();
                                try {
                                    long blockNumber = web3j.ethGetBlockByHash(blockHash, true).send().getBlock().getNumber().longValue();
                                    builder.add(new EthereumBlockRange(blockNumber, blockNumber));
                                }
                                catch (IOException e) {
                                    throw new IllegalStateException("Unable to getting block by hash " + blockHash);
                                }
                            });
                    log.info(entry.getValue().getValues().toString(null));
                } else if (entry.getKey() instanceof EthereumColumnHandle
                        && (((EthereumColumnHandle) entry.getKey()).getName().equals("block_timestamp"))) {
                    entry.getValue().getValues().getRanges().getOrderedRanges().forEach(r -> {
                        Marker low = r.getLow();
                        Marker high = r.getHigh();
                        try {
                            long startBlock = low.isLowerUnbounded() ? 1L : findBlockByTimestamp((Long) low.getValue(), -1L);
                            long endBlock = high.isUpperUnbounded() ? -1L : findBlockByTimestamp((Long) high.getValue(), 1L);
                            builder.add(new EthereumBlockRange(startBlock, endBlock));
                        }
                        catch (IOException e) {
                            throw new IllegalStateException("Unable to find block by timestamp");
                        }
                    });
                    log.info(entry.getValue().getValues().toString(null));
                }
            }
        }

        EthereumTableHandle handle = convertTableHandle(table);
        ConnectorTableLayout layout = new ConnectorTableLayout(new EthereumTableLayoutHandle(handle, builder.build()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle) {
        return new ConnectorTableLayout(handle);
    }

    /**
     * Return the columns and their types
     * @param session
     * @param tableHandle table to get columns of
     * @return list of columns
     */
    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle) {
        return getTableMetadata(convertTableHandle(tableHandle).toSchemaTableName());
    }

    /**
     * Return the columns and their types
     * @param schemaTableName table to get columns of
     * @return list of columns
     */
    private ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName) {
        List<ColumnMetadata> columnMetadata = getColumnsWithTypes(schemaTableName.getTableName()).stream()
                .map(column -> new ColumnMetadata(column.first, column.second))
                .collect(Collectors.toList());

        return new ConnectorTableMetadata(schemaTableName, columnMetadata);
    }

    /**
     * Return the columns and their types
     * @param session
     * @param tableHandle table to get columns of
     * @return list of columns
     */
    @SuppressWarnings("ValueOfIncrementOrDecrementUsed")
    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        AtomicInteger index = new AtomicInteger();
        return getColumnsWithTypes(convertTableHandle(tableHandle).getTableName()).stream()
                .map(column -> new EthereumColumnHandle(index.getAndIncrement(), column.first, column.second))
                .collect(Collectors.toMap(EthereumColumnHandle::getName, Function.identity()));
    }

    /**
     * Return the columns and their types
     * @param table table to get columns of
     * @return list of columns
     */
    private List<Pair<String, Type>> getColumnsWithTypes(String table) {
        ImmutableList.Builder<Pair<String, Type>> builder = ImmutableList.builder();

        if (EthereumTable.BLOCK.getName().equals(table)) {
            builder.add(new Pair<>("block_number", BigintType.BIGINT));
            builder.add(new Pair<>("block_hash", VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH)));
            builder.add(new Pair<>("block_parentHash", VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH)));
            builder.add(new Pair<>("block_nonce", VarcharType.createVarcharType(H8_BYTE_HASH_STRING_LENGTH)));
            builder.add(new Pair<>("block_sha3Uncles", VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH)));
            builder.add(new Pair<>("block_logsBloom", VarcharType.createVarcharType(H256_BYTE_HASH_STRING_LENGTH)));
            builder.add(new Pair<>("block_transactionsRoot", VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH)));
            builder.add(new Pair<>("block_stateRoot", VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH)));
            builder.add(new Pair<>("block_miner", VarcharType.createVarcharType(H20_BYTE_HASH_STRING_LENGTH)));
            builder.add(new Pair<>("block_difficulty", BigintType.BIGINT));
            builder.add(new Pair<>("block_totalDifficulty", BigintType.BIGINT));
            builder.add(new Pair<>("block_size", IntegerType.INTEGER));
            builder.add(new Pair<>("block_extraData", VarcharType.VARCHAR));
            builder.add(new Pair<>("block_gasLimit", DoubleType.DOUBLE));
            builder.add(new Pair<>("block_gasUsed", DoubleType.DOUBLE));
            builder.add(new Pair<>("block_timestamp", BigintType.BIGINT));
            builder.add(new Pair<>("block_transactions", new ArrayType(VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH))));
            builder.add(new Pair<>("block_uncles", new ArrayType(VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH))));
        } else if (EthereumTable.TRANSACTION.getName().equals(table)) {
            builder.add(new Pair<>("tx_hash", VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH)));
            builder.add(new Pair<>("tx_nonce", BigintType.BIGINT));
            builder.add(new Pair<>("tx_blockHash", VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH)));
            builder.add(new Pair<>("tx_blockNumber", BigintType.BIGINT));
            builder.add(new Pair<>("tx_transactionIndex", IntegerType.INTEGER));
            builder.add(new Pair<>("tx_from", VarcharType.createVarcharType(H20_BYTE_HASH_STRING_LENGTH)));
            builder.add(new Pair<>("tx_to", VarcharType.createVarcharType(H20_BYTE_HASH_STRING_LENGTH)));
            builder.add(new Pair<>("tx_value", DoubleType.DOUBLE));
            builder.add(new Pair<>("tx_gas", DoubleType.DOUBLE));
            builder.add(new Pair<>("tx_gasPrice", DoubleType.DOUBLE));
            builder.add(new Pair<>("tx_input", VarcharType.VARCHAR));
        } else if (EthereumTable.ERC20.getName().equals(table)) {
            builder.add(new Pair<>("erc20_token", VarcharType.createUnboundedVarcharType()));
            builder.add(new Pair<>("erc20_from", VarcharType.createVarcharType(H20_BYTE_HASH_STRING_LENGTH)));
            builder.add(new Pair<>("erc20_to", VarcharType.createVarcharType(H20_BYTE_HASH_STRING_LENGTH)));
            builder.add(new Pair<>("erc20_value", DoubleType.DOUBLE));
            builder.add(new Pair<>("erc20_txHash", VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH)));
            builder.add(new Pair<>("erc20_blockNumber", BigintType.BIGINT));
        } else {
            throw new IllegalArgumentException("Unknown Table Name " + table);
        }

        return builder.build();
    }

    private long findBlockByTimestamp(long timestamp, long offset) throws IOException {
        long startBlock = 1L;
        long currentBlock = web3j.ethBlockNumber().send().getBlockNumber().longValue();

        if (currentBlock <= 1) {
            return currentBlock;
        }

        long low = startBlock;
        long high = currentBlock;
        long middle = low + (high - low) / 2;

        while(low <= high) {
            middle = low + (high - low) / 2;
            long ts = web3j.ethGetBlockByNumber(DefaultBlockParameter.valueOf(BigInteger.valueOf(middle)), false).send().getBlock().getTimestamp().longValue();

            if (ts < timestamp) {
                low = middle + 1;
            } else if (ts > timestamp) {
                high = middle - 1;
            } else {
                return middle;
            }
        }
        return middle + offset;
    }
}
