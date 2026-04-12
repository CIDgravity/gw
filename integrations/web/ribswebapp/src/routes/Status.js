import './Status.css';
import React, { useState, useEffect, useRef } from "react";
import RibsRPC from "../helpers/rpc";
import { CopyToClipboard } from 'react-copy-to-clipboard';
import {formatBytesBinary, formatNum, formatNum6, calcEMA, formatTimestamp} from "../helpers/fmt";
import { BarChart, Bar, XAxis, YAxis, Tooltip, CartesianGrid, Legend, ResponsiveContainer, LineChart, Line } from 'recharts';

const oneFil = 1000000000000000000

function formatUnixTime(unixSeconds) {
    if (!unixSeconds) return '-';
    return new Date(unixSeconds * 1000).toLocaleTimeString();
}

function formatCountdown(unixSeconds) {
    if (!unixSeconds) return '-';
    const diffMs = unixSeconds * 1000 - Date.now();
    if (diffMs <= 0) return 'now';
    const totalSeconds = Math.ceil(diffMs / 1000);
    if (totalSeconds < 60) return `${totalSeconds}s`;
    const minutes = Math.floor(totalSeconds / 60);
    const seconds = totalSeconds % 60;
    return `${minutes}m ${seconds}s`;
}

function formatLastAction(unixSeconds) {
    if (!unixSeconds) return 'Never';
    return formatTimestamp(unixSeconds);
}

function WalletInfoTile({ walletInfo }) {
    const [dropdownOpen, setDropdownOpen] = useState(false);
    const [amount, setAmount] = useState(oneFil);
    const [operationType, setOperationType] = useState('');
    const [balanceInfo, setBalanceInfo] = useState(null);
    const [loading, setLoading] = useState({});

    const truncateAddress = (address) => {
        const head = address.slice(0, 10);
        const tail = address.slice(-10);
        return `${head}...${tail}`;
    };

    const handleAddWithdrawClick = (type) => {
        if (operationType === type && dropdownOpen) {
            setDropdownOpen(false);
            setOperationType('');
            return;
        }
        setOperationType(type);
        setDropdownOpen(true);
    };

    const handleAmountChange = (event) => {
        setAmount(event.target.value);
    };

    const handleSubmit = async () => {
        try {
            let cid;
            if (operationType === 'add') {
                cid = await RibsRPC.call("WalletMarketAdd", [(amount*oneFil).toString()]);
            } else {
                cid = await RibsRPC.call("WalletMarketWithdraw", [(amount*oneFil).toString()]);
            }
            alert(`Operation successful. CID: ${cid['/']}`);
            setDropdownOpen(false);
        } catch (error) {
            console.error("Error during operation:", error);
            alert('Error during operation. Please try again.');
        }
    };

    const handleFaucetFil = async () => {
        setLoading(prev => ({...prev, fil: true}));
        try {
            await RibsRPC.call("RequestFaucetFil");
            alert('FIL faucet request submitted');
        } catch (error) {
            console.error("Faucet FIL error:", error);
            alert('Faucet request failed: ' + (error.message || error));
        }
        setLoading(prev => ({...prev, fil: false}));
    };

    const handleFaucetDatacap = async () => {
        setLoading(prev => ({...prev, datacap: true}));
        try {
            await RibsRPC.call("RequestFaucetDatacap");
            alert('Datacap faucet request submitted');
        } catch (error) {
            console.error("Faucet datacap error:", error);
            alert('Faucet request failed: ' + (error.message || error));
        }
        setLoading(prev => ({...prev, datacap: false}));
    };

    const handleMarketTopUp = async () => {
        setLoading(prev => ({...prev, market: true}));
        try {
            await RibsRPC.call("TopUpMarketBalance");
            alert('Market balance top-up submitted');
        } catch (error) {
            console.error("Market top-up error:", error);
            alert('Top-up failed: ' + (error.message || error));
        }
        setLoading(prev => ({...prev, market: false}));
    };

    // Fetch balance manager info
    useEffect(() => {
        const fetchBalanceInfo = async () => {
            try {
                const info = await RibsRPC.call("BalanceManagerInfo");
                setBalanceInfo(info);
            } catch (error) {
                console.error("Error fetching balance manager info:", error);
            }
        };
        fetchBalanceInfo();
        const intervalId = setInterval(fetchBalanceInfo, 5000);
        return () => clearInterval(intervalId);
    }, []);

    const formatFil = (fil) => {
        if (fil === undefined || fil === null) return '-';
        if (fil < 0.000001) return fil.toExponential(2) + ' FIL';
        if (fil < 0.001) return fil.toFixed(6) + ' FIL';
        return fil.toFixed(4) + ' FIL';
    };

    const formatTiB = (tib) => {
        if (tib === undefined || tib === null) return '-';
        return tib.toFixed(2) + ' TiB';
    };

    const getStatusColor = (belowThreshold) => {
        return belowThreshold ? '#ff6b6b' : '#4caf50';
    };

    return (
        <div>
            <h2>Wallet Info</h2>
            {walletInfo && (
                <table className="compact-table">
                    <tbody>
                    <tr>
                        <td>Address</td>
                        <td>{truncateAddress(walletInfo.Addr)}</td>
                    </tr>
                    <tr>
                        <td colSpan={2} style={{textAlign: 'center'}}>
                            <a target="_blank" rel="noreferrer" className="button-ish button-sm" style={{marginRight: '4px'}} href={`https://filfox.info/en/address/${walletInfo.Addr}`}>FilFox</a>
                            <a target="_blank" rel="noreferrer" className="button-ish button-sm" style={{marginRight: '4px'}} href={`https://datacapstats.io/clients/${walletInfo.IDAddr}`}>DcapStats</a>
                            <a target="_blank" rel="noreferrer" className="button-ish button-sm" style={{marginRight: '4px'}} href={`https://dag.parts/client/${walletInfo.IDAddr}`}>DagParts</a>
                        </td>
                    </tr>
                    <tr>
                        <td>Balance:</td>
                        <td>
                            <span style={{color: balanceInfo?.WalletBelowThreshold ? '#ff6b6b' : 'inherit'}}>
                                {walletInfo.Balance}
                            </span>
                            {balanceInfo?.FaucetEnabled && (
                                <>
                                    {' '}
                                    <button 
                                        className="button-sm" 
                                        onClick={handleFaucetFil}
                                        disabled={loading.fil}
                                        title={`Request FIL from faucet (threshold: ${formatFil(balanceInfo?.FaucetFilThreshold)})`}
                                    >
                                        {loading.fil ? '...' : 'Faucet'}
                                    </button>
                                </>
                            )}
                        </td>
                    </tr>
                    {balanceInfo?.FaucetEnabled && (
                        <tr>
                            <td style={{paddingLeft: '1em', fontSize: '0.85em', color: '#666'}}>Min:</td>
                            <td style={{fontSize: '0.85em', color: '#666'}}>{formatFil(balanceInfo?.FaucetFilThreshold)}</td>
                        </tr>
                    )}
                    <tr>
                        <td style={{paddingLeft: '1em', fontSize: '0.85em', color: '#666'}}>Last FIL request:</td>
                        <td style={{fontSize: '0.85em', color: '#666'}}>{formatLastAction(balanceInfo?.LastFaucetFilRequest)}</td>
                    </tr>
                    <tr>
                        <td>Market Balance:</td>
                        <td>
                            <span style={{color: balanceInfo?.MarketBelowThreshold ? '#ff6b6b' : 'inherit'}}>
                                {walletInfo.MarketBalance}
                            </span>
                            {' '}
                            <span className="segmented-control">
                                <button
                                    className={`button-sm segmented-button ${operationType === 'add' && dropdownOpen ? 'active' : ''}`}
                                    onClick={() => handleAddWithdrawClick('add')}
                                >
                                    {operationType === 'add' && dropdownOpen ? 'Add Selected' : 'Add'}
                                </button>
                            {' '}
                                <button
                                    className={`button-sm segmented-button ${operationType === 'withdraw' && dropdownOpen ? 'active' : ''}`}
                                    onClick={() => handleAddWithdrawClick('withdraw')}
                                >
                                    {operationType === 'withdraw' && dropdownOpen ? 'Withdraw Selected' : 'Withdraw'}
                                </button>
                            </span>
                            {' '}
                            <button 
                                className="button-sm" 
                                onClick={handleMarketTopUp}
                                disabled={loading.market}
                                title={`Top up to ${formatFil(balanceInfo?.MarketBalanceTarget)}`}
                            >
                                {loading.market ? '...' : 'TopUp'}
                            </button>
                        </td>
                    </tr>
                    {dropdownOpen && (
                        <tr>
                            <td colSpan="2">
                                <input
                                    type="text"
                                    value={amount}
                                    onChange={handleAmountChange}
                                    placeholder="Enter amount"
                                />
                                <button className="button-sm" onClick={handleSubmit}>{operationType === 'add' ? 'Add' : 'Withdraw'}</button>
                            </td>
                        </tr>
                    )}
                    <tr>
                        <td style={{paddingLeft: '1em', fontSize: '0.85em', color: '#666'}}>Min / Target:</td>
                        <td style={{fontSize: '0.85em', color: '#666'}}>
                            {formatFil(balanceInfo?.MarketBalanceMin)} / {formatFil(balanceInfo?.MarketBalanceTarget)}
                        </td>
                    </tr>
                    <tr>
                        <td style={{paddingLeft: '1em', fontSize: '0.85em', color: '#666'}}>Last TopUp:</td>
                        <td style={{fontSize: '0.85em', color: '#666'}}>{formatLastAction(balanceInfo?.LastMarketTopUp)}</td>
                    </tr>
                    <tr>
                        <td>Market Locked:</td>
                        <td>{walletInfo.MarketLocked}</td>
                    </tr>
                    <tr>
                        <td>DataCap:</td>
                        <td>
                            <span 
                                className="important-metric"
                                style={{color: balanceInfo?.DatacapBelowThreshold ? '#ff6b6b' : '#4caf50'}}
                            >
                                {walletInfo.DataCap}
                            </span>
                            {balanceInfo?.FaucetEnabled && (
                                <>
                                    {' '}
                                    <button 
                                        className="button-sm" 
                                        onClick={handleFaucetDatacap}
                                        disabled={loading.datacap}
                                        title={`Request datacap from faucet (threshold: ${formatTiB(balanceInfo?.DatacapThresholdTiB)})`}
                                    >
                                        {loading.datacap ? '...' : 'Faucet'}
                                    </button>
                                </>
                            )}
                        </td>
                    </tr>
                    {balanceInfo?.FaucetEnabled && (
                        <tr>
                            <td style={{paddingLeft: '1em', fontSize: '0.85em', color: '#666'}}>Min:</td>
                            <td style={{fontSize: '0.85em', color: '#666'}}>{formatTiB(balanceInfo?.DatacapThresholdTiB)}</td>
                        </tr>
                    )}
                    <tr>
                        <td style={{paddingLeft: '1em', fontSize: '0.85em', color: '#666'}}>Last datacap request:</td>
                        <td style={{fontSize: '0.85em', color: '#666'}}>{formatLastAction(balanceInfo?.LastFaucetDatacapRequest)}</td>
                    </tr>
                    </tbody>
                </table>
            )}
        </div>
    );
}

function GroupsTile() {
    const [groupStats, setGroupStats] = useState(null);

    const fetchStatus = async () => {
        try {
            const groupStats = await RibsRPC.call("GetGroupStats");
            setGroupStats(groupStats);
        } catch (error) {
            console.error("Error fetching group stats:", error);
        }
    };

    useEffect(() => {
        fetchStatus();
        const intervalId = setInterval(fetchStatus, 1000);

        return () => {
            clearInterval(intervalId);
        };
    }, []);

    return (
        <div style={{background: '#DEFCFF'}}>
            <h2>Data Stats</h2>
            <table className="compact-table">
                <tbody>
                <tr>
                    <td>Block Groups:</td>
                    <td>{groupStats?.GroupCount}</td>
                </tr>
                <tr>
                    <td>Total data size:</td>
                    <td className="important-metric">{formatBytesBinary(groupStats?.TotalDataSize)}</td>
                </tr>
                <tr>
                    <td>Local size:</td>
                    <td>{formatBytesBinary(groupStats?.NonOffloadedDataSize)}</td>
                </tr>
                <tr>
                    <td>Offloaded size:</td>
                    <td>{formatBytesBinary(groupStats?.OffloadedDataSize)}</td>
                </tr>
                <tr>
                    <td>Open (RO):</td>
                    <td>{groupStats?.OpenGroups ?? 0}</td>
                </tr>
                <tr>
                    <td>Open (RW):</td>
                    <td>{groupStats?.OpenWritable ?? 0}</td>
                </tr>
                </tbody>
            </table>
        </div>
    );
}

function TopIndexTile() {
    const [indexStats, setIndexStats] = useState({Entries: 0, Reads: 0, Writes: 0});
    const prevStatsRef = useRef({Reads: 0, Writes: 0});
    const [readRate, setReadRate] = useState(0);
    const [writeRate, setWriteRate] = useState(0);
    const smoothingFactor = 1 / 15; // Smooth EMA for 10Hz updates

    const fetchStatus = async () => {
        try {
            const stats = await RibsRPC.call("TopIndexStats");
            setIndexStats(stats);

            const reads = stats.Reads;
            const writes = stats.Writes;

            if (prevStatsRef.current.Reads !== undefined) {
                // Multiply by 10 to convert from per-100ms to per-second
                const readsRate = (reads - prevStatsRef.current.Reads) * 10;
                const writesRate = (writes - prevStatsRef.current.Writes) * 10;

                setReadRate(prevReadRate => calcEMA(readsRate, prevReadRate, smoothingFactor));
                setWriteRate(prevWriteRate => calcEMA(writesRate, prevWriteRate, smoothingFactor));
            }

            prevStatsRef.current = {Reads: reads, Writes: writes};
        } catch (error) {
            console.error("Error fetching status:", error);
        }
    };

    useEffect(() => {
        fetchStatus();
        const intervalId = setInterval(fetchStatus, 100); // 10Hz refresh

        return () => {
            clearInterval(intervalId);
        };
    }, []);

    return (
        <div>
            <h2>Top Index</h2>
            <table className="compact-table">
                <tbody>
                <tr>
                    <td>Entries:</td>
                    <td className="important-metric">{formatNum6(indexStats.Entries)}</td>
                </tr>
                <tr>
                    <td>Read rate:</td>
                    <td>{formatNum(Math.round(readRate))}/s</td>
                </tr>
                <tr>
                    <td>Write rate:</td>
                    <td>{formatNum(Math.round(writeRate))}/s</td>
                </tr>
                </tbody>
            </table>
        </div>
    );
}

function ParallelWritesTile() {
    const [parallelStats, setParallelStats] = useState({Enabled: false});
    const [lbMetrics, setLbMetrics] = useState({});
    const [writeRate, setWriteRate] = useState(0);
    const prevStatsRef = useRef({});
    const smoothingFactor = 1 / 15; // Smooth EMA for 10Hz updates

    const fetchStatus = async () => {
        try {
            // Fetch both in parallel to avoid blocking
            const [stats, lb] = await Promise.all([
                RibsRPC.call("ParallelWriteStats"),
                RibsRPC.call("LoadBalancerMetrics")
            ]);
            
            // Calculate write rate (per 100ms interval, multiply by 10 for per-second)
            if (prevStatsRef.current.TotalWrites !== undefined) {
                const writesDelta = stats.TotalWrites - prevStatsRef.current.TotalWrites;
                setWriteRate(prev => calcEMA(writesDelta * 10, prev, smoothingFactor));
            }
            prevStatsRef.current = stats;
            
            setParallelStats(stats);
            setLbMetrics(lb);
        } catch (error) {
            console.error("Error fetching parallel write stats:", error);
        }
    };

    useEffect(() => {
        fetchStatus();
        const intervalId = setInterval(fetchStatus, 100); // 10Hz refresh
        return () => clearInterval(intervalId);
    }, []);

    const getModeColor = () => {
        if (!parallelStats.Enabled) return '#FFE5E5'; // Light red for disabled
        return '#E5F5E5'; // Light green for enabled
    };

    return (
        <div style={{background: getModeColor()}}>
            <h2>Parallel Writes</h2>
            <table className="compact-table">
                <tbody>
                <tr>
                    <td>Mode:</td>
                    <td className="important-metric">
                        {parallelStats.Enabled ? 'Enabled' : 'Disabled'}
                    </td>
                </tr>
                {parallelStats.Enabled && (
                    <>
                    <tr>
                        <td>Writable Groups:</td>
                        <td>{lbMetrics.WritableGroupCount || 0}</td>
                    </tr>
                    <tr>
                        <td>Active Writers:</td>
                        <td>{lbMetrics.TotalActiveWriters || 0}</td>
                    </tr>
                    <tr>
                        <td>Session Affinities:</td>
                        <td>{lbMetrics.SessionAffinities || 0}</td>
                    </tr>
                    <tr>
                        <td>Write Rate:</td>
                        <td>{formatNum(Math.round(writeRate))}/s</td>
                    </tr>
                    <tr>
                        <td>Parallel/Legacy:</td>
                        <td>
                            {parallelStats.ParallelWrites || 0} / {parallelStats.LegacyWrites || 0}
                        </td>
                    </tr>
                    <tr>
                        <td>Affinity Hit Rate:</td>
                        <td>
                            {((parallelStats.AffinityHitRate || 0) * 100).toFixed(1)}%
                        </td>
                    </tr>
                    <tr>
                        <td>Avg Write Time:</td>
                        <td>{(parallelStats.AvgWriteTimeMs || 0).toFixed(2)}ms</td>
                    </tr>
                    <tr>
                        <td>Bytes Written:</td>
                        <td>{formatBytesBinary(parallelStats.BytesWritten || 0)}</td>
                    </tr>
                    </>
                )}
                </tbody>
            </table>
        </div>
    );
}

function DealsTile({ dealSummary, dealLoopStats }) {
    return (
        <div style={{background: '#FFF4DD'}}>
            <h2>Deals: {dealSummary.InProgress + dealSummary.Done}</h2>
            <table className="compact-table">
                <tbody>
                <tr>
                    <td>Total data size:</td>
                    <td>{formatBytesBinary(dealSummary.TotalDataSize)}</td>
                </tr>
                <tr>
                    <td>Total deal size:</td>
                    <td>{formatBytesBinary(dealSummary.TotalDealSize)}</td>
                </tr>
                <tr>
                    <td>Stored data size:</td>
                    <td className="important-metric">{formatBytesBinary(dealSummary.StoredDataSize)}</td>
                </tr>
                <tr>
                    <td>Stored deal size:</td>
                    <td className="important-metric">{formatBytesBinary(dealSummary.StoredDealSize)}</td>
                </tr>
                <tr>
                    <td>Deals in progress:</td>
                    <td>{dealSummary.InProgress}</td>
                </tr>
                <tr>
                    <td>Deals done:</td>
                    <td className="important-metric">{dealSummary.Done}</td>
                </tr>
                <tr>
                    <td>Deals failed:</td>
                    <td>{dealSummary.Failed}</td>
                </tr>
                <tr>
                    <td>Loop state:</td>
                    <td>{dealLoopStats?.Running ? 'Running' : 'Sleeping'}</td>
                </tr>
                <tr>
                    <td>Next deal check:</td>
                    <td>{formatCountdown(dealLoopStats?.NextCheckUnix)}</td>
                </tr>
                <tr>
                    <td>Last check:</td>
                    <td>{formatUnixTime(dealLoopStats?.LastEndUnix)}</td>
                </tr>
                <tr>
                    <td>Base interval:</td>
                    <td>{dealLoopStats?.BaseIntervalMs || 0}ms</td>
                </tr>
                <tr>
                    <td>Last duration:</td>
                    <td>{dealLoopStats?.LastDurationMs || 0}ms</td>
                </tr>
                <tr>
                    <td>Loop backoff:</td>
                    <td>{dealLoopStats?.CurrentBackoffMs || 0}ms</td>
                </tr>
                <tr>
                    <td>Consecutive failures:</td>
                    <td>{dealLoopStats?.ConsecutiveFailures || 0}</td>
                </tr>
                {!!dealLoopStats?.LastError && (
                    <tr>
                        <td>Last loop error:</td>
                        <td style={{color: '#f44336', fontSize: '0.85em'}}>{dealLoopStats.LastError}</td>
                    </tr>
                )}
                </tbody>
            </table>
        </div>
    );
}

function ProvidersTile({ reachableProviders }) {
    return (
        <div>
            <h2>Providers</h2>
            <table className="compact-table">
                <tbody>
                <tr>
                    <td>Reachable Providers:</td>
                    <td>{reachableProviders.length}</td>
                </tr>
                <tr>
                    <td>With boost-deals:</td>
                    <td>{reachableProviders.filter(p => p.BoostDeals).length}</td>
                </tr>
                <tr>
                    <td>With booster-bitswap:</td>
                    <td>{reachableProviders.filter(p => p.BoosterBitswap).length}</td>
                </tr>
                <tr>
                    <td>With booster-http:</td>
                    <td>{reachableProviders.filter(p => p.BoosterHttp).length}</td>
                </tr>
                <tr>
                    <td>With attempted deals:</td>
                    <td>{reachableProviders.filter(p => p.DealStarted).length}</td>
                </tr>
                <tr>
                    <td>With successful deals:</td>
                    <td>{reachableProviders.filter(p => p.DealSuccess).length}</td>
                </tr>
                <tr>
                    <td>On cooldown:</td>
                    <td>{reachableProviders.filter(p => p.DealCooldownUntil && p.DealCooldownUntil * 1000 > Date.now()).length}</td>
                </tr>
                <tr>
                    <td>With all rejected deals:</td>
                    <td>{reachableProviders.filter(p => p.DealStarted > 0 && p.DealRejected === p.DealStarted).length}</td>
                </tr>
                </tbody>
            </table>
        </div>
    );
}

function CarUploadStatsTile({ carUploadStats }) {
    const [chartData, setChartData] = useState([]);
    const [currentRate, setCurrentRate] = useState(0);
    const prevSampleRef = useRef(null);
    const rateEMARef = useRef(0);
    const smoothingFactor = 1 / 6;

    useEffect(() => {
        const now = Date.now();
        const totalBytes = carUploadStats.TotalBytes || carUploadStats.LastTotalBytes || 0;
        const activeRequests = carUploadStats.ActiveRequests || 0;

        let nextRate = 0;
        if (prevSampleRef.current) {
            const elapsedSeconds = (now - prevSampleRef.current.at) / 1000;
            const bytesDelta = Math.max(0, totalBytes - prevSampleRef.current.totalBytes);
            const instantRate = elapsedSeconds > 0 ? bytesDelta / elapsedSeconds : 0;
            if (rateEMARef.current === 0) {
                rateEMARef.current = instantRate;
            } else {
                rateEMARef.current = calcEMA(instantRate, rateEMARef.current, smoothingFactor);
            }
            nextRate = Math.round(rateEMARef.current);
        }

        prevSampleRef.current = { at: now, totalBytes };
        setCurrentRate(nextRate);
        setChartData(prev => [
            ...prev.slice(-59),
            {
                time: new Date(now).toLocaleTimeString(),
                rate: nextRate,
                activeRequests,
            }
        ]);
    }, [carUploadStats]);

    const modeLabel = !carUploadStats.Enabled
        ? 'Disabled'
        : carUploadStats.BuiltinServer
            ? 'Built-in LocalWeb'
            : carUploadStats.Module === 'local-web'
                ? 'Direct file serving'
                : carUploadStats.Module;

    return (
        <div className="car-upload-stats-tile">
            <h2>Car Upload Stats</h2>
            <table className="compact-table">
                <tbody>
                <tr>
                    <td>Mode</td>
                    <td>{modeLabel}</td>
                </tr>
                <tr>
                    <td>Active requests</td>
                    <td>{carUploadStats.ActiveRequests || 0}</td>
                </tr>
                <tr>
                    <td>Current rate</td>
                    <td>{formatBytesBinary(currentRate)}/s</td>
                </tr>
                <tr>
                    <td>Total bytes served</td>
                    <td>{formatBytesBinary(carUploadStats.TotalBytes || carUploadStats.LastTotalBytes || 0)}</td>
                </tr>
                </tbody>
            </table>

            {carUploadStats.BuiltinServer ? (
                <div className="car-upload-chart">
                    <ResponsiveContainer width="100%" height="100%">
                        <LineChart data={chartData} margin={{ top: 16, right: 16, left: 0, bottom: 8 }}>
                            <CartesianGrid strokeDasharray="3 3" />
                            <XAxis dataKey="time" minTickGap={32} />
                            <YAxis yAxisId="rate" tickFormatter={(value) => formatBytesBinary(value)} />
                            <YAxis yAxisId="active" orientation="right" allowDecimals={false} />
                            <Tooltip
                                formatter={(value, name) => {
                                    if (name === 'Rate') {
                                        return [`${formatBytesBinary(value)}/s`, name];
                                    }
                                    return [value, name];
                                }}
                            />
                            <Legend />
                            <Line yAxisId="rate" type="monotone" dataKey="rate" name="Rate" stroke="#1f77b4" dot={false} strokeWidth={2} isAnimationActive={false} />
                            <Line yAxisId="active" type="monotone" dataKey="activeRequests" name="Active Requests" stroke="#d62728" dot={false} strokeWidth={2} isAnimationActive={false} />
                        </LineChart>
                    </ResponsiveContainer>
                </div>
            ) : (
                <p className="status-note">Built-in LocalWeb request stats are only available when the gateway serves CAR files directly. In direct file-serving mode, nginx or another server may serve `cardata` without passing traffic through the gateway.</p>
            )}
        </div>
    );
}

function CrawlStateTile({ crawlState }) {
    const progressBarPercentage = crawlState.State === "querying providers" ? (crawlState.At / crawlState.Total) * 100 : 0;
    const showAt = ["listing market participants", "querying providers"].includes(crawlState.State);

    return (
        <div className="CrawlStateTile">
            <h2>Crawl State</h2>
            <table className="compact-table">
                <tbody>
                <tr>
                    <td><b>State:</b></td>
                    <td>{crawlState.State}</td>
                </tr>
                {showAt && (
                    <tr>
                        <td><b>At:</b></td>
                        <td>{crawlState.At}</td>
                    </tr>
                )}
                {crawlState.State === "querying providers" && (
                    <>
                        <tr>
                            <td><b>Progress:</b></td>
                            <td>{progressBarPercentage.toFixed(2)}%</td>
                        </tr>
                        <tr>
                            <td colSpan={2}>
                                <div className="progress-bar">
                                    <div className="progress-bar__fill" style={{ width: `${progressBarPercentage}%` }}></div>
                                </div>
                            </td>
                        </tr>
                        <tr>
                            <td><b>Total:</b></td>
                            <td>{crawlState.Total}</td>
                        </tr>
                        <tr>
                            <td><b>Reachable:</b></td>
                            <td>{crawlState.Reachable}</td>
                        </tr>
                        <tr>
                            <td><b>Boost:</b></td>
                            <td>{crawlState.Boost}</td>
                        </tr>
                        <tr>
                            <td><b>BBswap:</b></td>
                            <td>{crawlState.BBswap}</td>
                        </tr>
                        <tr>
                            <td><b>BHttp:</b></td>
                            <td>{crawlState.BHttp}</td>
                        </tr>
                    </>
                )}
                </tbody>
            </table>
        </div>
    );
}

function IoStats() {
    const [groupIOStats, setGroupIOStats] = useState({});
    const prevStatsRef = useRef({});
    const [rates, setRates] = useState({readBlocks: 0, writeBlocks: 0, readBytes: 0, writeBytes: 0});
    const [chartData, setChartData] = useState([]);
    const smoothingFactor = 1 / 15; // Smooth EMA for 10Hz updates

    const fetchStatus = async () => {
        try {
            const ioStats = await RibsRPC.call("GroupIOStats");

            const prevStats = prevStatsRef.current;
            const now = Date.now();
            const readBlocks = ioStats.ReadBlocks;
            const writeBlocks = ioStats.WriteBlocks;
            const readBytes = ioStats.ReadBytes;
            const writeBytes = ioStats.WriteBytes;

            if (prevStats.ReadBlocks !== undefined && prevStats.WriteBlocks !== undefined) {
                const elapsedSeconds = (now - prevStats.At) / 1000;
                const readBlocksRate = elapsedSeconds > 0 ? (readBlocks - prevStats.ReadBlocks) / elapsedSeconds : 0;
                const writeBlocksRate = elapsedSeconds > 0 ? (writeBlocks - prevStats.WriteBlocks) / elapsedSeconds : 0;
                const readBytesRate = elapsedSeconds > 0 ? (readBytes - prevStats.ReadBytes) / elapsedSeconds : 0;
                const writeBytesRate = elapsedSeconds > 0 ? (writeBytes - prevStats.WriteBytes) / elapsedSeconds : 0;

                setRates(prev => {
                    const nextRates = {
                    readBlocks: calcEMA(readBlocksRate, prev.readBlocks, smoothingFactor),
                    writeBlocks: calcEMA(writeBlocksRate, prev.writeBlocks, smoothingFactor),
                    readBytes: calcEMA(readBytesRate, prev.readBytes, smoothingFactor),
                    writeBytes: calcEMA(writeBytesRate, prev.writeBytes, smoothingFactor),
                    };

                    setChartData(prevChart => [
                        ...prevChart.slice(-89),
                        {
                            time: new Date(now).toLocaleTimeString(),
                            readBytes: Math.round(nextRates.readBytes),
                            writeBytes: Math.round(nextRates.writeBytes),
                        }
                    ]);

                    return nextRates;
                });
            }

            setGroupIOStats(ioStats);
            prevStatsRef.current = { ReadBlocks: readBlocks, WriteBlocks: writeBlocks, ReadBytes: readBytes, WriteBytes: writeBytes, At: now };
        } catch (error) {
            console.error("Error fetching status:", error);
        }
    };

    useEffect(() => {
        fetchStatus();
        const intervalId = setInterval(fetchStatus, 100); // 10Hz refresh

        return () => {
            clearInterval(intervalId);
        };
    }, []);

    return (
        <div style={{gridColumn: "span 2"}}>
            <h2>IO Stats</h2>
            <table className="compact-table">
                <tbody>
                <tr>
                    <td>Read Rate:</td>
                    <td>{formatNum(Math.round(rates.readBlocks))} Blk/s</td>
                </tr>
                <tr>
                    <td>Read Bytes:</td>
                    <td>{formatBytesBinary(Math.round(rates.readBytes))}/s</td>
                </tr>
                <tr>
                    <td>Write Rate:</td>
                    <td>{formatNum(Math.round(rates.writeBlocks))} Blk/s</td>
                </tr>
                <tr>
                    <td>Write Bytes:</td>
                    <td>{formatBytesBinary(Math.round(rates.writeBytes))}/s</td>
                </tr>
                </tbody>
            </table>

            <div className="status-chart">
                <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={chartData} margin={{ top: 16, right: 16, left: 0, bottom: 8 }}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis dataKey="time" minTickGap={32} />
                        <YAxis tickFormatter={(value) => formatBytesBinary(value)} />
                        <Tooltip formatter={(value, name) => [`${formatBytesBinary(value)}/s`, name === 'readBytes' ? 'Read' : 'Write']} />
                        <Legend formatter={(value) => value === 'readBytes' ? 'Read' : 'Write'} />
                        <Line type="monotone" dataKey="readBytes" stroke="#1f77b4" dot={false} strokeWidth={2} isAnimationActive={false} />
                        <Line type="monotone" dataKey="writeBytes" stroke="#2ca02c" dot={false} strokeWidth={2} isAnimationActive={false} />
                    </LineChart>
                </ResponsiveContainer>
            </div>
        </div>
    );
}

function RetrStats({retrStats}) {
    return (
        <div style={{background: '#f6f0ff'}}>
            <h2>Retrieval Stats</h2>
            <table className="compact-table">
                <tbody>
                <tr>
                    <td>Success:</td>
                    <td>{formatNum(retrStats.Success)}</td>
                </tr>
                <tr>
                    <td>Bytes:</td>
                    <td>{formatBytesBinary(retrStats.Bytes)}</td>
                </tr>
                <tr>
                    <td>Fail:</td>
                    <td>{formatNum(retrStats.Fail)}</td>
                </tr>
                <tr>
                    <td>Cache Hit:</td>
                    <td>{formatNum(retrStats.CacheHit)}</td>
                </tr>
                <tr>
                    <td>Cache Miss:</td>
                    <td>{formatNum(retrStats.CacheMiss)}</td>
                </tr>
                <tr>
                    <td>Active Retrievals:</td>
                    <td>{formatNum(retrStats.Active)}</td>
                </tr>
                <tr>
                    <td>HTTP Tries:</td>
                    <td>{formatNum(retrStats.HTTPTries)}</td>
                </tr>
                <tr>
                    <td>HTTP Success:</td>
                    <td>{formatNum(retrStats.HTTPSuccess)}</td>
                </tr>
                <tr>
                    <td>HTTP Bytes:</td>
                    <td>{formatBytesBinary(retrStats.HTTPBytes)}</td>
                </tr>
                </tbody>
            </table>
        </div>
    )
}

function StagingStats() {
    const prevStatsRef = useRef({});
    const readReqsEMARef = useRef(0);
    const readBytesEMARef = useRef(0);
    const uploadBytesEMARef = useRef(0);
    const redirectsEMARef = useRef(0);
    const smoothingFactor = 1 / 10;
    const [stagingStats, setStagingStats] = useState({});


    const fetchStatus = async () => {
        const stats = await RibsRPC.call("StagingStats");
        setStagingStats(stats);

        const prevStats = prevStatsRef.current;
        const readReqs = stats.ReadReqs;
        const readBytes = stats.ReadBytes;
        const uploadBytes = stats.UploadBytes;
        const redirects = stats.Redirects;

        if (prevStats.ReadReqs !== undefined) {
            const readReqsRate = readReqs - prevStats.ReadReqs;
            const readBytesRate = readBytes - prevStats.ReadBytes;
            const uploadBytesRate = uploadBytes - prevStats.UploadBytes;
            const redirectsRate = redirects - prevStats.Redirects;

            readReqsEMARef.current = calcEMA(
                readReqsRate,
                readReqsEMARef.current,
                smoothingFactor
            );
            readBytesEMARef.current = calcEMA(
                readBytesRate,
                readBytesEMARef.current,
                smoothingFactor
            );
            uploadBytesEMARef.current = calcEMA(
                uploadBytesRate,
                uploadBytesEMARef.current,
                smoothingFactor
            );
            redirectsEMARef.current = calcEMA(
                redirectsRate,
                redirectsEMARef.current,
                smoothingFactor
            );
        }

        prevStatsRef.current = { ReadReqs: readReqs, ReadBytes: readBytes, UploadBytes: uploadBytes, Redirects: redirects };
    };

    useEffect(() => {
        fetchStatus();
        const intervalId = setInterval(fetchStatus, 1000);

        return () => {
            clearInterval(intervalId);
        };
    }, []);

    return (
        <div>
            <h2>Staging Stats</h2>
            <table className="compact-table">
                <tbody>
                <tr>
                    <td>Read Requests:</td>
                    <td>{formatNum(stagingStats.ReadReqs)}</td>
                </tr>
                <tr>
                    <td>Read Rate:</td>
                    <td>{formatNum(Math.round(readReqsEMARef.current))}/s</td>
                </tr>
                <tr>
                    <td>Read Bytes/s:</td>
                    <td>{formatBytesBinary(Math.round(readBytesEMARef.current))}/s</td>
                </tr>
                <tr>
                    <td>Read Bytes:</td>
                    <td>{formatBytesBinary(stagingStats.ReadBytes)}</td>
                </tr>
                <tr>
                    <td>Upload Rate:</td>
                    <td>{formatBytesBinary(Math.round(uploadBytesEMARef.current))}/s</td>
                </tr>
                <tr>
                    <td>Uploaded Bytes:</td>
                    <td>{formatBytesBinary(stagingStats.UploadBytes)}</td>
                </tr>
                <tr>
                    <td>Active Uploads:</td>
                    <td>{stagingStats.UploadStarted - stagingStats.UploadDone - stagingStats.UploadErr}</td>
                </tr>
                {stagingStats.UploadErr > 0 && <tr>
                    <td>Upload Err:</td>
                    <td>{formatNum(stagingStats.UploadErr)}</td>
                </tr>}
                <tr>
                    <td>Redirects Rate:</td>
                    <td>{formatNum(Math.round(redirectsEMARef.current))}/s</td>
                </tr>
                </tbody>
            </table>
        </div>
    )
}

function P2PNodes() {
    const [nodes, setNodes] = useState({});

    const fetchStatus = async () => {
        try {
            const nodeStats = await RibsRPC.call("P2PNodes");
            setNodes(nodeStats)
        } catch (error) {
            console.error("Error fetching p2p node infos:", error);
        }
    };

    useEffect(() => {
        fetchStatus();
        const intervalId = setInterval(fetchStatus, 2500);

        return () => {
            clearInterval(intervalId);
        };
    }, []);

    return (
        <div style={{background: '#FFEDDD'}}>
            <h2>LibP2P Nodes</h2>
            <table className="compact-table">
                <tbody>
                {Object.keys(nodes).map((nodeName, index) => (
                    <tr key={index}>
                        <td colSpan={2}><h3>{nodeName}</h3></td>
                        <td colSpan={2}>
                            <CopyToClipboard text={nodes[nodeName].PeerID}>
                                <p title={`PeerID: ${nodes[nodeName].PeerID}\n\nListen Addresses: ${nodes[nodeName].Listen.join('\n')}`}>
                                    {`${nodes[nodeName].PeerID.slice(0, 10)}...`}
                                </p>
                            </CopyToClipboard>
                        </td>
                        <td colSpan={2}>Peers: {nodes[nodeName].Peers}</td>
                    </tr>
                ))}
                </tbody>
            </table>
        </div>
    );
}

function GoRuntimeStats() {
    const [stats, setStats] = useState({});
    const prevStatsRef = useRef({});
    const prevTimeRef = useRef(Date.now());
    const gcPausePercentEMARef = useRef(0);
    const smoothingFactor = 1 / 10;

    const fetchStats = async () => {
        try {
            const runtimeStats = await RibsRPC.call("RuntimeStats");
            const currentTime = Date.now();
            const elapsedTime = currentTime - prevTimeRef.current;
            prevTimeRef.current = currentTime;

            if (prevStatsRef.current.PauseTotalNs !== undefined) {
                const gcPauseTimeDelta = runtimeStats.PauseTotalNs - prevStatsRef.current.PauseTotalNs;
                const gcPauseTimePercent = gcPauseTimeDelta / (elapsedTime * 1e6); // convert ms to ns
                gcPausePercentEMARef.current = calcEMA(gcPauseTimePercent, gcPausePercentEMARef.current, smoothingFactor);
            }

            prevStatsRef.current = runtimeStats;
            setStats(runtimeStats);
        } catch (error) {
            console.error("Error fetching Go runtime stats:", error);
        }
    };

    useEffect(() => {
        fetchStats();
        const intervalId = setInterval(fetchStats, 2500);

        return () => {
            clearInterval(intervalId);
        };
    }, []);

    return (
        <div>
            <h2>Go Runtime Stats</h2>
            <table className="compact-table">
                <tbody>
                <tr>
                    <td>Alloc:</td>
                    <td>{formatBytesBinary(stats.Alloc)}</td>
                </tr>
                <tr>
                    <td>TotalAlloc:</td>
                    <td>{formatBytesBinary(stats.TotalAlloc)}</td>
                </tr>
                <tr>
                    <td>HeapAlloc:</td>
                    <td>{formatBytesBinary(stats.HeapAlloc)}</td>
                </tr>
                <tr>
                    <td>HeapSys:</td>
                    <td>{formatBytesBinary(stats.HeapSys)}</td>
                </tr>
                <tr>
                    <td>HeapObjects:</td>
                    <td>{formatNum(stats.HeapObjects)}</td>
                </tr>
                <tr>
                    <td>Number of GC:</td>
                    <td>{formatNum(stats.NumGC)}</td>
                </tr>
                <tr>
                    <td>GC Pause (% of time):</td>
                    <td>{formatNum(gcPausePercentEMARef.current * 100, 3)}%</td>
                </tr>
                </tbody>
            </table>
        </div>
    );
}


function getPercentage(value, total) {
    if (total === 0) {
        return 0;
    } else {
        return ((value / total) * 100).toFixed(2);
    }
}

function RetrCheckerStats({stats}) {
    return (
        <div>
            <h2>Retrieval Checker</h2>
            <table className="compact-table">
                <tbody>
                <tr>
                    <td>Progress:</td>
                    <td>{stats.Success+stats.Fail} / {stats.ToDo} ({getPercentage(stats.Success+stats.Fail, stats.ToDo)}%)</td>
                </tr>
                <tr>
                    <td colSpan={2}>
                        <div className="progress-bar">
                            <div className="progress-bar__fill" style={{ width: `${getPercentage(stats.Success+stats.Fail, stats.ToDo)}%` }}></div>
                        </div>
                    </td>
                </tr>
                <tr>
                    <td>Success (current):</td>
                    <td>{stats.Success} ({getPercentage(stats.Success, stats.ToDo)}%)</td>
                </tr>
                <tr>
                    <td>Fail (current):</td>
                    <td>{stats.Fail} ({getPercentage(stats.Fail, stats.ToDo)}%)</td>
                </tr><tr>
                    <td>Success:</td>
                    <td>{stats.SuccessAll} ({getPercentage(stats.SuccessAll, stats.ToDo)}%)</td>
                </tr>
                <tr>
                    <td>Fail:</td>
                    <td>{stats.FailAll} ({getPercentage(stats.FailAll, stats.ToDo)}%)</td>
                </tr>
                </tbody>
            </table>
        </div>
    )
}

function WorkerStats({stats}) {
    const prevStatsRef = useRef({});
    const prevTimeRef = useRef(Date.now());
    const commPBytesRateRef = useRef(0);
    const [chartData, setChartData] = useState([]);
    const smoothingFactor = 1 / 10;

    useEffect(() => {
        const prevStats = prevStatsRef.current;
        const currentTime = Date.now();
        const elapsedTime = (currentTime - prevTimeRef.current) / 1000; // convert ms to s
        prevTimeRef.current = currentTime;

        if (prevStats.CommPBytes !== undefined) {
            const commPBytesRate = (stats.CommPBytes - prevStats.CommPBytes) / elapsedTime;
            commPBytesRateRef.current = calcEMA(
                commPBytesRate,
                commPBytesRateRef.current,
                smoothingFactor
            );

            setChartData(prev => [
                ...prev.slice(-89),
                {
                    time: new Date(currentTime).toLocaleTimeString(),
                    rate: Math.round(commPBytesRateRef.current),
                }
            ]);
        }

        prevStatsRef.current = stats;
    }, [stats]);

    return (
        <div>
            <h2>Worker Stats</h2>
            <table className="compact-table">
                <tbody>
                <tr>
                    <td>Use:</td>
                    <td>{stats.InFinalize+stats.InCommP} / {stats.Available}</td>
                </tr>
                <tr>
                    <td colSpan={2}>
                        <div className="progress-bar">
                            <div className="progress-bar__fill" style={{ width: `${(stats.InFinalize+stats.InCommP) / stats.Available * 100}%` }}></div>
                        </div>
                    </td>
                </tr>
                <tr>
                    <td>Group Finalize:</td>
                    <td>{stats.InFinalize}</td>
                </tr>
                <tr>
                    <td>Compute Data CID</td>
                    <td>{stats.InCommP}</td>
                </tr><tr>
                    <td>Queued Tasks:</td>
                    <td>{stats.TaskQueue}</td>
                </tr>
                <tr>
                    <td>DataCID rate:</td>
                    <td>{formatBytesBinary(commPBytesRateRef.current)}/s</td>
                </tr>
                </tbody>
            </table>

            <div className="status-chart">
                <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={chartData} margin={{ top: 16, right: 16, left: 0, bottom: 8 }}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis dataKey="time" minTickGap={32} />
                        <YAxis tickFormatter={(value) => formatBytesBinary(value)} />
                        <Tooltip formatter={(value) => [`${formatBytesBinary(value)}/s`, 'DataCID Rate']} />
                        <Legend />
                        <Line type="monotone" dataKey="rate" name="DataCID Rate" stroke="#ff7f0e" dot={false} strokeWidth={2} isAnimationActive={false} />
                    </LineChart>
                </ResponsiveContainer>
            </div>
        </div>
    )
}

function DealCountsChart() {
    const [dealCounts, setDealCounts] = useState([]);

    const fetchData = async () => {
        try {
            const retrievableDealCounts = await RibsRPC.call('RetrievableDealCounts');
            const sealedDealCounts = await RibsRPC.call('SealedDealCounts');

            // Create a map to easily find sealed deals by count
            let allMap = Object.fromEntries(retrievableDealCounts.map(item => [item.Count, {Retrievable: item.Groups}]));
            allMap = sealedDealCounts.reduce((acc, item) => {
                acc[item.Count] = {...acc[item.Count], Sealed: item.Groups};
                return acc;
            }, allMap);

            const data = Object.entries(allMap).map(([count, groups]) => ({
                name: count,
                ...groups
            }));

            setDealCounts(data);
        } catch (error) {
            console.error('Error fetching deal counts:', error);
        }
    };

    useEffect(() => {
        fetchData();
        const intervalId = setInterval(fetchData, 2500);

        return () => {
            clearInterval(intervalId);
        };
    }, []);

    return (
        <div style={{height: "25em", gridColumn: "span 2"}}>
            <h2>Deals Per Group</h2>
            <ResponsiveContainer width="100%" height="100%">
                <BarChart
                    data={dealCounts}
                    margin={{ top: 20, right: 0, left: 0, bottom: 64 }}
                >
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="name" />
                    <YAxis />
                    <Tooltip />
                    <Legend />
                    <Bar dataKey="Retrievable" fill="#8884d8" isAnimationActive={false} />
                    <Bar dataKey="Sealed" fill="#82ca9d" isAnimationActive={false} />
                </BarChart>
            </ResponsiveContainer>
        </div>
    );
}

function RepairRetrievals() {
    const [queueStats, setQueueStats] = useState({Total: 0, Assigned: 0});
    const [repairStats, setRepairStats] = useState({});
    const [retrievalRate, setRetrievalRate] = useState(0);
    const prevProgressRef = useRef({at: 0, total: 0, ema: 0});

    const fetchStats = async () => {
        try {
            const queue = await RibsRPC.call("RepairQueue");
            const jobs = await RibsRPC.call("RepairStats");

            setQueueStats(queue);
            setRepairStats(jobs || {});

            const totalProgress = Object.values(jobs || {}).reduce((sum, job) => sum + (job.FetchProgress || 0), 0);
            const now = Date.now();
            if (prevProgressRef.current.at) {
                const elapsedSeconds = (now - prevProgressRef.current.at) / 1000;
                const delta = Math.max(0, totalProgress - prevProgressRef.current.total);
                const instantRate = elapsedSeconds > 0 ? delta / elapsedSeconds : 0;
                const ema = calcEMA(instantRate, prevProgressRef.current.ema || 0, 1 / 5);
                prevProgressRef.current.ema = ema;
                setRetrievalRate(Math.round(ema));
            }
            prevProgressRef.current.at = now;
            prevProgressRef.current.total = totalProgress;
        } catch (error) {
            console.error("Error fetching repair stats:", error);
        }
    };

    useEffect(() => {
        fetchStats();
        const intervalId = setInterval(fetchStats, 1000);
        return () => clearInterval(intervalId);
    }, []);

    const activeJobs = Object.values(repairStats || {}).length;

    return (
        <div>
            <h2>Repair Retrievals</h2>

            <table className="compact-table">
                <tbody>
                <tr>
                    <td>Queue</td>
                    <td>{queueStats.Total || 0} Deals</td>
                </tr>
                <tr>
                    <td>Active</td>
                    <td>{Math.max(queueStats.Assigned || 0, activeJobs)} Deals</td>
                </tr>
                <tr>
                    <td>Retrieval rate</td>
                    <td>{formatBytesBinary(retrievalRate)}/s</td>
                </tr>
                </tbody>
            </table>
        </div>
    )
}

function CIDGravityStatusTile() {
    const [status, setStatus] = useState(null);
    const [loading, setLoading] = useState(true);

    const fetchStatus = async () => {
        try {
            const result = await RibsRPC.call("CIDGravityStatus");
            setStatus(result);
            setLoading(false);
        } catch (error) {
            console.error("Error fetching CIDGravity status:", error);
            setStatus({
                connected: false,
                tokenValid: false,
                tokenConfigured: false,
                error: error.message || "Failed to fetch status"
            });
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchStatus();
        // Refresh every 30 seconds (less frequent since it makes API calls)
        const intervalId = setInterval(fetchStatus, 30000);
        return () => clearInterval(intervalId);
    }, []);

    const getStatusColor = () => {
        if (!status) return '#f5f5f5';
        if (!status.tokenConfigured) return '#FFE5E5'; // Light red - not configured
        if (status.tokenValid) return '#E5F5E5'; // Light green - all good
        if (status.connected) return '#FFF4DD'; // Light yellow - connected but token invalid
        return '#FFE5E5'; // Light red - not connected
    };

    const getStatusText = () => {
        if (!status) return 'Loading...';
        if (!status.tokenConfigured) return 'Not Configured';
        if (status.tokenValid) return 'Connected';
        if (status.connected) return 'Token Invalid';
        return 'Disconnected';
    };

    return (
        <div style={{background: getStatusColor()}}>
            <h2>CIDGravity</h2>
            {loading ? (
                <p>Loading...</p>
            ) : (
                <table className="compact-table">
                    <tbody>
                    <tr>
                        <td>Status:</td>
                        <td className="important-metric" style={{
                            color: status?.tokenValid ? '#4caf50' : 
                                   status?.connected ? '#ff9800' : '#f44336'
                        }}>
                            {getStatusText()}
                        </td>
                    </tr>
                    <tr>
                        <td>Token:</td>
                        <td>{status?.tokenConfigured ? 'Configured' : 'Not Set'}</td>
                    </tr>
                    {status?.connected && (
                        <tr>
                            <td>Response:</td>
                            <td>{status?.responseTimeMs}ms</td>
                        </tr>
                    )}
                    {status?.error && (
                        <tr>
                            <td>Error:</td>
                            <td style={{color: '#f44336', fontSize: '0.85em'}}>{status?.error}</td>
                        </tr>
                    )}
                    <tr>
                        <td colSpan={2} style={{fontSize: '0.8em', color: '#666', paddingTop: '8px'}}>
                            <a href="https://app.cidgravity.com" target="_blank" rel="noopener noreferrer" className="button-ish button-sm">
                                CIDGravity Dashboard
                            </a>
                        </td>
                    </tr>
                    </tbody>
                </table>
            )}
        </div>
    );
}

function CacheStatsTile() {
    const [cacheStats, setCacheStats] = useState(null);
    const [loading, setLoading] = useState(true);

    const fetchStats = async () => {
        try {
            const stats = await RibsRPC.call("CacheStats");
            setCacheStats(stats);
            setLoading(false);
        } catch (error) {
            console.error("Error fetching cache stats:", error);
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchStats();
        const intervalId = setInterval(fetchStats, 2000);
        return () => clearInterval(intervalId);
    }, []);

    const getHitRate = () => {
        if (!cacheStats || (cacheStats.hits + cacheStats.misses) === 0) return 0;
        return ((cacheStats.hits / (cacheStats.hits + cacheStats.misses)) * 100).toFixed(1);
    };

    const getL1UsagePercent = () => {
        if (!cacheStats || !cacheStats.l1Enabled || cacheStats.l1Capacity === 0) return 0;
        return ((cacheStats.l1Size / cacheStats.l1Capacity) * 100).toFixed(1);
    };

    const getL2UsagePercent = () => {
        if (!cacheStats || !cacheStats.l2Enabled || cacheStats.l2MaxSize === 0) return 0;
        return ((cacheStats.l2Size / cacheStats.l2MaxSize) * 100).toFixed(1);
    };

    return (
        <div style={{background: '#e8f5e9'}}>
            <h2>Cache</h2>
            {loading ? (
                <p>Loading...</p>
            ) : (
                <table className="compact-table">
                    <tbody>
                    <tr>
                        <td>Hit Rate:</td>
                        <td className="important-metric" style={{color: getHitRate() > 80 ? '#4caf50' : getHitRate() > 50 ? '#ff9800' : '#f44336'}}>
                            {getHitRate()}%
                        </td>
                    </tr>
                    <tr>
                        <td>Hits / Misses:</td>
                        <td>{formatNum(cacheStats?.hits || 0)} / {formatNum(cacheStats?.misses || 0)}</td>
                    </tr>

                    {/* L1 Cache Section */}
                    <tr>
                        <td colSpan={2} style={{paddingTop: '8px'}}><b>L1 (Memory)</b></td>
                    </tr>
                    {cacheStats?.l1Enabled ? (
                        <>
                        <tr>
                            <td>Size:</td>
                            <td>{formatBytesBinary(cacheStats?.l1Size || 0)} / {formatBytesBinary(cacheStats?.l1Capacity || 0)} ({getL1UsagePercent()}%)</td>
                        </tr>
                        <tr>
                            <td>Items:</td>
                            <td>{formatNum(cacheStats?.l1Items || 0)}</td>
                        </tr>
                        <tr>
                            <td>T1/T2:</td>
                            <td>{formatBytesBinary(cacheStats?.l1T1Size || 0)} / {formatBytesBinary(cacheStats?.l1T2Size || 0)}</td>
                        </tr>
                        <tr>
                            <td>Ghost B1/B2:</td>
                            <td>{cacheStats?.l1B1Len || 0} / {cacheStats?.l1B2Len || 0}</td>
                        </tr>
                        </>
                    ) : (
                        <tr><td colSpan={2} style={{color: '#999'}}>Disabled</td></tr>
                    )}

                    {/* L2 Cache Section */}
                    <tr>
                        <td colSpan={2} style={{paddingTop: '8px'}}><b>L2 (SSD)</b></td>
                    </tr>
                    {cacheStats?.l2Enabled ? (
                        <>
                        <tr>
                            <td>Size:</td>
                            <td>{formatBytesBinary(cacheStats?.l2Size || 0)} / {formatBytesBinary(cacheStats?.l2MaxSize || 0)} ({getL2UsagePercent()}%)</td>
                        </tr>
                        <tr>
                            <td>Items:</td>
                            <td>{formatNum(cacheStats?.l2Items || 0)}</td>
                        </tr>
                        <tr>
                            <td>Probation/Protected:</td>
                            <td>{formatBytesBinary(cacheStats?.l2ProbationSize || 0)} / {formatBytesBinary(cacheStats?.l2ProtectedSize || 0)}</td>
                        </tr>
                        <tr>
                            <td>Free Space:</td>
                            <td>{formatBytesBinary(cacheStats?.l2FreeSpace || 0)}</td>
                        </tr>
                        </>
                    ) : (
                        <tr><td colSpan={2} style={{color: '#999'}}>Disabled</td></tr>
                    )}
                    </tbody>
                </table>
            )}
        </div>
    );
}

// read/write busy time

// process stats [rpc]
// - fds
// - goroutines

// lotus rpc
// - calls

// - metamask integ?

// - market balance setting

// - commp compute rate
// - bsst compute rate??
// - local index rates

function Status() {
    const [walletInfo, setWalletInfo] = useState(null);
    const [groups, setGroups] = useState([]);
    const [crawlState, setCrawlState] = useState("");
    const [carUploadStats, setCarUploadStats] = useState({});
    const [reachableProviders, setReachableProviders] = useState([]);
    const [dealSummary, setDealSummary] = useState({});
    const [dealLoopStats, setDealLoopStats] = useState({});
    const [retrStats, setRetrStats] = useState({});
    const [retrChecker, setRetrChecker] = useState({})
    const [workerStats, setWorkerStats] = useState({})

    const fetchStatus = async () => {
        try {
            const walletInfo = await RibsRPC.call("WalletInfo");
            setWalletInfo(walletInfo);
            const groups = await RibsRPC.call("Groups");
            const crawlState = await RibsRPC.call("CrawlState");
            const carUploadStats = await RibsRPC.call("CarUploadStats");
            const reachableProviders = await RibsRPC.call("ReachableProviders");
            const dealSummary = await RibsRPC.call("DealSummary");
            const dealLoopStats = await RibsRPC.call("DealLoopStats");
            const retrStats = await RibsRPC.call("RetrStats");
            const retrCheckerStats = await RibsRPC.call("RetrChecker")
            const workerStats = await RibsRPC.call("WorkerStats")

            setGroups(groups);
            setCrawlState(crawlState);
            setCarUploadStats(carUploadStats);
            setReachableProviders(reachableProviders);
            setDealSummary(dealSummary);
            setDealLoopStats(dealLoopStats);
            setRetrStats(retrStats);
            setRetrChecker(retrCheckerStats);
            setWorkerStats(workerStats);
        } catch (error) {
            console.error("Error fetching status:", error);
        }
    };

    useEffect(() => {
        fetchStatus();
        const intervalId = setInterval(fetchStatus, 1000);

        return () => {
            clearInterval(intervalId);
        };
    }, []);

    return (
        <div className="">
            <div className="Status">
                <h1>Storage</h1>
                <div className="status-grid">
                    <GroupsTile groups={groups} />
                    <IoStats />
                    <TopIndexTile />
                    <ParallelWritesTile />
                </div>

                <h1><abbr title="Decentralized Storage Network">DSN</abbr></h1>
                <div className="status-grid">
                    <DealsTile dealSummary={dealSummary} dealLoopStats={dealLoopStats} />
                    <DealCountsChart />
                    <ProvidersTile reachableProviders={reachableProviders} />
                    <CarUploadStatsTile carUploadStats={carUploadStats} />
                    <CrawlStateTile crawlState={crawlState} />
                    <WalletInfoTile walletInfo={walletInfo} />
                    <CIDGravityStatusTile />
                </div>

                <h1>External Storage</h1>
                <div className="status-grid">
                    <RetrStats retrStats={retrStats} />
                    <CacheStatsTile />
                    <StagingStats />
                    <RetrCheckerStats stats={retrChecker} />
                </div>

                <h1>Replication Repair</h1>
                <div className="status-grid">
                    <RepairRetrievals />
                </div>

                <h1>Internals</h1>
                <div className="status-grid">
                    <P2PNodes />
                    <GoRuntimeStats />
                    <WorkerStats stats={workerStats} />
                </div>
            </div>
        </div>
    );
}

export default Status;
