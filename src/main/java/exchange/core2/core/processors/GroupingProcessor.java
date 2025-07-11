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

import com.lmax.disruptor.*;
import exchange.core2.core.common.CoreWaitStrategy;
import exchange.core2.core.common.MatcherTradeEvent;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.cmd.OrderCommandType;
import exchange.core2.core.common.config.PerformanceConfiguration;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicInteger;

import static exchange.core2.core.ExchangeCore.EVENTS_POOLING;

@Slf4j
public final class GroupingProcessor implements EventProcessor {
    private static final int IDLE = 0;
    private static final int HALTED = IDLE + 1;
    private static final int RUNNING = HALTED + 1;

    private static final int GROUP_SPIN_LIMIT = 1000;

    // TODO move into configuration
    private static final int L2_PUBLISH_INTERVAL_NS = 10_000_000;

    private final AtomicInteger running = new AtomicInteger(IDLE);
    private final RingBuffer<OrderCommand> ringBuffer;
    private final SequenceBarrier sequenceBarrier;
    private final WaitSpinningHelper waitSpinningHelper;
    private final Sequence sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);

    // a pool for trade event storage (?)
    private final SharedPool sharedPool;

    // limit of messages in group
    private final int msgsInGroupLimit;
    // maximum duration of group in nanoseconds
    private final long maxGroupDurationNs;

    public GroupingProcessor(RingBuffer<OrderCommand> ringBuffer,
                             SequenceBarrier sequenceBarrier,
                             PerformanceConfiguration perfCfg,
                             CoreWaitStrategy coreWaitStrategy,
                             SharedPool sharedPool) {

        if (perfCfg.getMsgsInGroupLimit() > perfCfg.getRingBufferSize() / 4) {
            throw new IllegalArgumentException("msgsInGroupLimit should be less than quarter ringBufferSize");
        }

        this.ringBuffer = ringBuffer;
        this.sequenceBarrier = sequenceBarrier;
        this.waitSpinningHelper = new WaitSpinningHelper(ringBuffer, sequenceBarrier, GROUP_SPIN_LIMIT, coreWaitStrategy);
        this.msgsInGroupLimit = perfCfg.getMsgsInGroupLimit();
        this.maxGroupDurationNs = perfCfg.getMaxGroupDurationNs();
        this.sharedPool = sharedPool;
    }

    @Override
    public Sequence getSequence() {
        return sequence;
    }

    @Override
    public void halt() {
        running.set(HALTED);
        sequenceBarrier.alert();
    }

    @Override
    public boolean isRunning() {
        return running.get() != IDLE;
    }


    /**
     * It is ok to have another thread rerun this method after a halt().
     *
     * @throws IllegalStateException if this object instance is already running in a thread
     */
    @Override
    public void run() {
        if (running.compareAndSet(IDLE, RUNNING)) {
            sequenceBarrier.clearAlert();
            try {
                if (running.get() == RUNNING) {
                    processEvents();
                }
            } finally {
                running.set(IDLE);
            }
        } else {
            // This is a little bit of guess work.  The running state could of changed to HALTED by
            // this point.  However, Java does not have compareAndExchange which is the only way
            // to get it exactly correct.
            if (running.get() == RUNNING) {
                throw new IllegalStateException("Thread is already running");
            }
        }
    }

    private void processEvents() {
        long nextSequence = sequence.get() + 1L;

        // group id
        long groupCounter = 0;
        // number of messages in the current group
        long msgsInGroup = 0;

        // time to switch group (when message is not big enough and already waited for a while)
        // when idle, will check this time to decide whether to switch group or not
        // when busy, only switch group if message in group >= msgsInGroupLimit
        long groupLastNs = 0;

        // the time to mark triggerL2DataRequest to true
        long l2dataLastNs = 0;
        // a true flag will trigger updating cmd serviceFlags to 1
        boolean triggerL2DataRequest = false;

        final int tradeEventChainLengthTarget = sharedPool.getChainLength();
        MatcherTradeEvent tradeEventHead = null;
        MatcherTradeEvent tradeEventTail = null;
        int tradeEventCounter = 0; // counter

        boolean groupingEnabled = true;

        while (true) {
            try {

                // should spin and also check another barrier
                long availableSequence = waitSpinningHelper.tryWaitFor(nextSequence);

                // handling when nextSequence is available
                if (nextSequence <= availableSequence) {
                    // handle each command that is available in the ring buffer
                    while (nextSequence <= availableSequence) {

                        final OrderCommand cmd = ringBuffer.get(nextSequence);

                        nextSequence++;

                        // handle GROUPING_CONTROL command type for enabling/disabling grouping
                        if (cmd.command == OrderCommandType.GROUPING_CONTROL) {
                            groupingEnabled = cmd.orderId == 1;
                            cmd.resultCode = CommandResultCode.SUCCESS;
                        }

                        // skip handling if grouping is disabled
                        if (!groupingEnabled) {
                            // TODO pooling
                            cmd.matcherEvent = null;
                            cmd.marketData = null;
                            continue;
                        }

                        // some commands should trigger R2 stage to avoid unprocessed events that could affect accounting state
                        // directly switch group if command is RESET, PERSIST_STATE_MATCHING or GROUPING_CONTROL
                        if (cmd.command == OrderCommandType.RESET
                                || cmd.command == OrderCommandType.PERSIST_STATE_MATCHING
                                || cmd.command == OrderCommandType.GROUPING_CONTROL) {
                            groupCounter++;
                            msgsInGroup = 0;
                        }

                        // report/binary commands also should trigger R2 stage, but only for last message
                        // directly switch group if the command is in binary and the command is the last fragment (symbol == -1)
                        if ((cmd.command == OrderCommandType.BINARY_DATA_COMMAND || cmd.command == OrderCommandType.BINARY_DATA_QUERY) && cmd.symbol == -1) {
                            groupCounter++;
                            msgsInGroup = 0;
                        }

                        // update command's group id
                        cmd.eventsGroup = groupCounter;

                        // handle triggerL2DataRequest flag
                        if (triggerL2DataRequest) {
                            triggerL2DataRequest = false;
                            cmd.serviceFlags = 1;
                        } else {
                            cmd.serviceFlags = 0;
                        }

                        // take the attached event from the command, send to the shared pool when it is big enough, clean up the attached event from command
                        if (EVENTS_POOLING && cmd.matcherEvent != null) {

                            // insert command's matcherEvent into the trade event chain and update count of event
                            // update tail
                            if (tradeEventTail == null) {
                                tradeEventHead = cmd.matcherEvent;
                            } else {
                                tradeEventTail.nextEvent = cmd.matcherEvent;
                            }

                            tradeEventTail = cmd.matcherEvent;
                            tradeEventCounter++;

                            // find last element in the chain and update tail accordingly
                            while (tradeEventTail.nextEvent != null) {
                                tradeEventTail = tradeEventTail.nextEvent;
                                tradeEventCounter++;
                            }

                            if (tradeEventCounter >= tradeEventChainLengthTarget) {
                                // chain is big enough -> send to the shared pool
                                tradeEventCounter = 0;
                                sharedPool.putChain(tradeEventHead);
                                tradeEventTail = null;
                                tradeEventHead = null;
                            }

                        }
                        cmd.matcherEvent = null;

                        // TODO collect to shared buffer
                        cmd.marketData = null;

                        msgsInGroup++;

                        // switch group after each N messages
                        // avoid changing groups when PERSIST_STATE_MATCHING is already executing
                        if (msgsInGroup >= msgsInGroupLimit && cmd.command != OrderCommandType.PERSIST_STATE_RISK) {
                            groupCounter++;
                            msgsInGroup = 0;
                        }

                    }
                    sequence.set(availableSequence);
                    waitSpinningHelper.signalAllWhenBlocking();

                    // extend the group limit time
                    groupLastNs = System.nanoTime() + maxGroupDurationNs;

                } else {
                    // handling when next sequence is not available yet

                    // check if we need to switch group
                    final long t = System.nanoTime();
                    // if group is not empty and group limit time is exceeded, switch group
                    if (msgsInGroup > 0 && t > groupLastNs) {
                        // switch group after T microseconds elapsed, if group is non-empty
                        groupCounter++;
                        msgsInGroup = 0;
                    }

                    // check if we need to trigger L2 data request (mark triggerL2DataRequest to true + update next l2dataLastNs)
                    if (t > l2dataLastNs) {
                        // TODO fix order best price updating mechanism,
                        //  this does not work for multi-symbol configuration

                        l2dataLastNs = t + L2_PUBLISH_INTERVAL_NS; // trigger L2 data every 10ms
                        triggerL2DataRequest = true;
                    }
                }

            } catch (final AlertException ex) {
                if (running.get() != RUNNING) {
                    break;
                }
            } catch (final Throwable ex) {
                sequence.set(nextSequence);
                waitSpinningHelper.signalAllWhenBlocking();
                nextSequence++;
            }
        }
    }

    @Override
    public String toString() {
        return "GroupingProcessor{" +
                "GL=" + msgsInGroupLimit +
                '}';
    }
}