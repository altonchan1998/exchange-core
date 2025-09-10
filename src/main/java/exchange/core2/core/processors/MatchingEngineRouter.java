/*
 * Copyright 2019 Maksim Zheravin
 *
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
package exchange.core2.core.processors;

import exchange.core2.collections.objpool.ObjectsPool;
import exchange.core2.core.common.CoreSymbolSpecification;
import exchange.core2.core.common.SymbolType;
import exchange.core2.core.common.api.binary.BatchAddAccountsCommand;
import exchange.core2.core.common.api.binary.BatchAddSymbolsCommand;
import exchange.core2.core.common.api.reports.ReportQuery;
import exchange.core2.core.common.api.reports.ReportResult;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.cmd.OrderCommandType;
import exchange.core2.core.common.config.*;
import exchange.core2.core.orderbook.IOrderBook;
import exchange.core2.core.orderbook.OrderBookEventsHelper;
import exchange.core2.core.processors.journaling.DiskSerializationProcessorConfiguration;
import exchange.core2.core.processors.journaling.ISerializationProcessor;
import exchange.core2.core.utils.SerializationUtils;
import exchange.core2.core.utils.UnsafeUtils;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Optional;

@Slf4j
@Getter
public final class MatchingEngineRouter implements WriteBytesMarshallable {

    public static final ISerializationProcessor.SerializedModuleType MODULE_ME =
            ISerializationProcessor.SerializedModuleType.MATCHING_ENGINE_ROUTER;

    // state
    private final BinaryCommandsProcessor binaryCommandsProcessor;

    // symbol->OB
    private final IntObjectHashMap<IOrderBook> orderBooks;

    private final IOrderBook.OrderBookFactory orderBookFactory;

    private final OrderBookEventsHelper eventsHelper;

    // local objects pool for order books
    private final ObjectsPool objectsPool;

    // sharding by symbolId
    private final int shardId;
    private final long shardMask;

    private final String exchangeId; // TODO validate
    private final Path folder;

    private final boolean cfgMarginTradingEnabled;

    private final boolean cfgSendL2ForEveryCmd;
    private final int cfgL2RefreshDepth;

    private final ISerializationProcessor serializationProcessor;

    private final LoggingConfiguration loggingCfg;
    private final boolean logDebug;

    public MatchingEngineRouter(final int shardId,
                                final long numShards,
                                final ISerializationProcessor serializationProcessor,
                                final IOrderBook.OrderBookFactory orderBookFactory,
                                final SharedPool sharedPool,
                                final ExchangeConfiguration exchangeCfg) {

        if (Long.bitCount(numShards) != 1) {
            throw new IllegalArgumentException("Invalid number of shards " + numShards + " - must be power of 2");
        }

        final InitialStateConfiguration initStateCfg = exchangeCfg.getInitStateCfg();

        this.exchangeId = initStateCfg.getExchangeId();
        this.folder = Paths.get(DiskSerializationProcessorConfiguration.DEFAULT_FOLDER);

        this.shardId = shardId;
        this.shardMask = numShards - 1;
        this.serializationProcessor = serializationProcessor;
        this.orderBookFactory = orderBookFactory;
        this.eventsHelper = new OrderBookEventsHelper(sharedPool::getChain);

        // 從 exchangeCfg 獲取日誌配置，檢查是否啟用匹配引擎的詳細日誌（LOGGING_MATCHING_DEBUG）
        this.loggingCfg = exchangeCfg.getLoggingCfg();
        this.logDebug = loggingCfg.getLoggingLevels().contains(LoggingConfiguration.LoggingLevel.LOGGING_MATCHING_DEBUG);

        // 初始化物件池（ObjectsPool），為不同物件類型分配預設容量
        final HashMap<Integer, Integer> objectsPoolConfig = new HashMap<>();

        // 1048576（1024*1024）個訂單物件
        objectsPoolConfig.put(ObjectsPool.DIRECT_ORDER, 1024 * 1024);

        // 65536（1024*64）個桶物件（推測用於訂單簿的價格層級）
        objectsPoolConfig.put(ObjectsPool.DIRECT_BUCKET, 1024 * 64);

        // Adaptive Radix Tree 節點，分別為 4、16、48、256 子節點的版本，容量遞減（因為較大的 ART 節點較少使用）
        objectsPoolConfig.put(ObjectsPool.ART_NODE_4, 1024 * 32);
        objectsPoolConfig.put(ObjectsPool.ART_NODE_16, 1024 * 16);
        objectsPoolConfig.put(ObjectsPool.ART_NODE_48, 1024 * 8);
        objectsPoolConfig.put(ObjectsPool.ART_NODE_256, 1024 * 4);
        this.objectsPool = new ObjectsPool(objectsPoolConfig);

        // 檢查是否存在匹配引擎（MODULE_ME）的快照
        if (ISerializationProcessor.canLoadFromSnapshot(serializationProcessor, initStateCfg, shardId, MODULE_ME)) {
            // 載入快照
            final DeserializedData deserialized = serializationProcessor.loadData(
                    initStateCfg.getSnapshotId(),
                    ISerializationProcessor.SerializedModuleType.MATCHING_ENGINE_ROUTER,
                    shardId,
                    // 傳入一個 lambda 函數處理反序列化
                    bytesIn -> {

                        // 驗證 shardId 和 shardMask 是否正確
                        if (shardId != bytesIn.readInt()) {
                            throw new IllegalStateException("wrong shardId");
                        }
                        if (shardMask != bytesIn.readLong()) {
                            throw new IllegalStateException("wrong shardMask");
                        }

                        // 初始化 BinaryCommandsProcessor，負責處理二進位命令（如訂單操作）和報表查詢
                        final BinaryCommandsProcessor bcp = new BinaryCommandsProcessor(
                                this::handleBinaryMessage,
                                this::handleReportQuery,
                                sharedPool,
                                exchangeCfg.getReportsQueriesCfg(),
                                bytesIn,
                                shardId + 1024);

                        // 反序列化訂單簿
                        final IntObjectHashMap<IOrderBook> ob = SerializationUtils.readIntHashMap(
                                bytesIn,
                                bytes -> IOrderBook.create(bytes, objectsPool, eventsHelper, loggingCfg));

                        return DeserializedData.builder().binaryCommandsProcessor(bcp).orderBooks(ob).build();
                    });

            // 將反序列化的 binaryCommandsProcessor 和 orderBooks 分配到欄位
            this.binaryCommandsProcessor = deserialized.binaryCommandsProcessor;
            this.orderBooks = deserialized.orderBooks;

        } else {
            // 初始化空的 BinaryCommandsProcessor 和空的 IntObjectHashMap（用於儲存訂單簿）
            this.binaryCommandsProcessor = new BinaryCommandsProcessor(
                    this::handleBinaryMessage,
                    this::handleReportQuery,
                    sharedPool,
                    exchangeCfg.getReportsQueriesCfg(),
                    shardId + 1024);

            this.orderBooks = new IntObjectHashMap<>();
        }

        final OrdersProcessingConfiguration ordersProcCfg = exchangeCfg.getOrdersProcessingCfg();
        
        // 檢查是否啟用保證金交易（margin trading），影響風險檢查邏輯
        this.cfgMarginTradingEnabled = ordersProcCfg.getMarginTradingMode() == OrdersProcessingConfiguration.MarginTradingMode.MARGIN_TRADING_ENABLED;

        final PerformanceConfiguration perfCfg = exchangeCfg.getPerformanceCfg();
        
        // 是否為每個命令發送 L2 市場資料（訂單簿深度更新）
        this.cfgSendL2ForEveryCmd = perfCfg.isSendL2ForEveryCmd();

        // L2 市場資料的刷新深度（例如，前 N 個價格層級）
        this.cfgL2RefreshDepth = perfCfg.getL2RefreshDepth();
    }

    public void processOrder(long seq, OrderCommand cmd) {

        final OrderCommandType command = cmd.command;

        // 檢查命令是否為訂單相關操作
        if (command == OrderCommandType.MOVE_ORDER
                || command == OrderCommandType.CANCEL_ORDER
                || command == OrderCommandType.PLACE_ORDER
                || command == OrderCommandType.REDUCE_ORDER
                || command == OrderCommandType.ORDER_BOOK_REQUEST) {
            // 檢查命令的符號（symbol，如交易對 ID）是否屬於當前分片（shardId）
            if (symbolForThisHandler(cmd.symbol)) {
                processMatchingCommand(cmd);
            }

        // 檢查命令是否為二進位資料相關命令
        } else if (command == OrderCommandType.BINARY_DATA_QUERY || command == OrderCommandType.BINARY_DATA_COMMAND) {

            // 處理二進位資料命令或查詢
            final CommandResultCode resultCode = binaryCommandsProcessor.acceptBinaryFrame(cmd);

            // 僅在 shardId == 0 時更新 cmd.resultCode，表示只有主分片（shard 0）負責設置結果
            if (shardId == 0) {
                cmd.resultCode = resultCode;
            }

        // 檢查命令是否為重置命令
        } else if (command == OrderCommandType.RESET) {
            // 清空所有訂單簿（orderBooks.clear()）和重置命令處理器
            orderBooks.clear();
            binaryCommandsProcessor.reset();

            // 僅在 shardId == 0 時設置 cmd.resultCode = SUCCESS，確保只有主分片回報成功
            if (shardId == 0) {
                cmd.resultCode = CommandResultCode.SUCCESS;
            }

        // 檢查命令是否為無操作命令
        } else if (command == OrderCommandType.NOP) {

            // 處理 NOP（無操作）命令，僅在 shardId == 0 時設置成功結果
            if (shardId == 0) {
                cmd.resultCode = CommandResultCode.SUCCESS;
            }

        // 檢查命令是否為持久化狀態命令
        } else if (command == OrderCommandType.PERSIST_STATE_MATCHING) {

            // 將當前匹配引擎狀態（this）保存為快照。
            final boolean isSuccess = serializationProcessor.storeData(
                    cmd.orderId,
                    seq,
                    cmd.timestamp,
                    ISerializationProcessor.SerializedModuleType.MATCHING_ENGINE_ROUTER,
                    shardId,
                    this);
            
            // 設置命令結果：
            // 如果 isSuccess 為 true，設置 CommandResultCode.ACCEPTED
            // 如果 isSuccess 為 false，設置 CommandResultCode.STATE_PERSIST_MATCHING_ENGINE_FAILED
            // Send ACCEPTED because this is a first command in series. Risk engine is second - so it will return SUCCESS
            UnsafeUtils.setResultVolatile(cmd, isSuccess, CommandResultCode.ACCEPTED, CommandResultCode.STATE_PERSIST_MATCHING_ENGINE_FAILED);
        }

    }

    // 傳入 BinaryCommandsProcessor 的 completeMessagesHandler {@BinaryCommandsProcessor#completeMessagesHandler}
    private void handleBinaryMessage(Object message) {

        // 只處理BatchAddSymbolsCommand
        if (message instanceof BatchAddSymbolsCommand) {
            // 從訊息中提取符號規格映射（IntObjectHashMap<CoreSymbolSpecification>）
            final IntObjectHashMap<CoreSymbolSpecification> symbols = ((BatchAddSymbolsCommand) message).getSymbols();

            // 添加新的交易對（如貨幣對）
            symbols.forEach(this::addSymbol);
        } else if (message instanceof BatchAddAccountsCommand) {
            // 匹配引擎不處理 BatchAddAccountsCommand
        }
    }

    // 傳入 BinaryCommandsProcessor 的 reportQueriesHandler {@BinaryCommandsProcessor#reportQueriesHandler}
    // 作用：處理報表查詢，返回結果（Optional<R>，其中 R 繼承 ReportResult）
    private <R extends ReportResult> Optional<R> handleReportQuery(ReportQuery<R> reportQuery) {
        return reportQuery.process(this);
    }


    private boolean symbolForThisHandler(final long symbol) {
        return (shardMask == 0) || ((symbol & shardMask) == shardId);
    }


    private void addSymbol(final CoreSymbolSpecification spec) {

//        log.debug("ME add symbolSpecification: {}", symbolSpecification);
        
        // 檢查保證金交易：
        // 如果符號類型不是貨幣對 且 未啟用保證金交易，記錄警告但不拋出異常
        if (spec.type != SymbolType.CURRENCY_EXCHANGE_PAIR && !cfgMarginTradingEnabled) {
            log.warn("Margin symbols are not allowed: {}", spec);
        }

        // 檢查訂單簿：
        // 檢查是否已存在訂單簿
        if (orderBooks.get(spec.symbolId) == null) {
            // 如果不存在，調用 orderBookFactory.create 創建新訂單簿
            orderBooks.put(spec.symbolId, orderBookFactory.create(spec, objectsPool, eventsHelper, loggingCfg));
        } else {
            // 如果已存在，記錄警告，防止覆蓋現有訂單簿。
            log.warn("OrderBook for symbol id={} already exists! Can not add symbol: {}", spec.symbolId, spec);
        }
    }

    // 作用：處理訂單相關命令，更新訂單簿並生成市場資料。
    private void processMatchingCommand(final OrderCommand cmd) {
        // 獲取符號對應的訂單簿
        final IOrderBook orderBook = orderBooks.get(cmd.symbol);


        if (orderBook == null) {
            // 如果不存在，設置結果碼 = MATCHING_INVALID_ORDER_BOOK_ID。
            cmd.resultCode = CommandResultCode.MATCHING_INVALID_ORDER_BOOK_ID;
        } else {
            // 執行命令（如添加、取消或移動訂單）並設置結果碼
            cmd.resultCode = IOrderBook.processCommand(orderBook, cmd);

            // posting market data for risk processor makes sense only if command execution is successful, otherwise it will be ignored (possible garbage from previous cycle)
            // TODO don't need for EXCHANGE mode order books?
            // TODO doing this for many order books simultaneously can introduce hiccups
            if ((cfgSendL2ForEveryCmd || (cmd.serviceFlags & 1) != 0)
                    && cmd.command != OrderCommandType.ORDER_BOOK_REQUEST
                    && cmd.resultCode == CommandResultCode.SUCCESS) {
                // 如果滿足條件, 獲取 L2 市場資料快照（訂單簿的前 N 個價格層級，深度由 cfgL2RefreshDepth 控制）並存入cmd.marketData
                cmd.marketData = orderBook.getL2MarketDataSnapshot(cfgL2RefreshDepth);
            }
        }
    }

    // 作用：將 MatchingEngineRouter 的狀態序列化到 BytesOut（Chronicle-Wire 的輸出緩衝區），用於快照儲存
    @Override
    public void writeMarshallable(BytesOut bytes) {
        bytes.writeInt(shardId).writeLong(shardMask);
        binaryCommandsProcessor.writeMarshallable(bytes);

        // write orderBooks
        SerializationUtils.marshallIntHashMap(orderBooks, bytes);
    }

    @Builder
    @RequiredArgsConstructor
    private static class DeserializedData {
        private final BinaryCommandsProcessor binaryCommandsProcessor;
        private final IntObjectHashMap<IOrderBook> orderBooks;
    }
}
