import React, { useState, useEffect } from 'react';
import { Link, useParams } from 'react-router-dom';
import RibsRPC from "../helpers/rpc";
import {
    formatBytesBinary,
    formatFil,
    epochToDate,
    epochToDuration,
    formatTimestamp
} from "../helpers/fmt";
import "./Status.css";
import "./Provider.css";

function calculatePercentage(part, total) {
    if (!total) {
        return 0;
    }

    return Math.round((part / total) * 100);
}

function formatDealRowBackground(deal) {
    if (deal.Rejected) return '#fff7cc';
    if (deal.Failed) return '#f5c4c4';
    if (deal.Sealed) return '#caffcd';
    return '#f2f3ff';
}

function Provider() {
    const [provider, setProvider] = useState(null);
    const [headHeight, setHeadHeight] = useState(0);
    const { providerID } = useParams();

    const fetchProvider = async () => {
        try {
            let prov = providerID.slice(2, providerID.length);
            prov = parseInt(prov);

            const provideData = await RibsRPC.call("ProviderInfo", [prov]);
            setProvider(provideData);

            const head = await RibsRPC.callFil("ChainHead");
            setHeadHeight(head.Height);
        } catch (error) {
            console.error("Error fetching providers:", error);
        }
    };

    useEffect(() => {
        fetchProvider();
        const intervalId = setInterval(fetchProvider, 2500);

        return () => {
            clearInterval(intervalId);
        };
    }, []);

    const renderTransferProgress = (deal) => {
        if (!deal.TxSize) {
            return deal.BytesRecv ? formatBytesBinary(deal.BytesRecv) : '-';
        }

        const percent = calculatePercentage(deal.BytesRecv, deal.TxSize);

        return (
            <div className="prov-progress-container">
                <div className="prov-text-with-progress">
                    {formatBytesBinary(deal.BytesRecv)} / {formatBytesBinary(deal.TxSize)} ({percent}%)
                </div>
                <div className="provider-progress-bar">
                    <div className="provider-progress-bar__fill" style={{ width: `${percent}%` }}></div>
                </div>
            </div>
        );
    };

    const renderRetrievalStats = (deal) => {
        const total = deal.RetrSuccess + deal.RetrFail;
        if (!total && !deal.RetrTTFBMs) {
            return '-';
        }

        return (
            <div>
                <div>{deal.RetrSuccess} ok / {deal.RetrFail} fail</div>
                {deal.RetrTTFBMs > 0 && <div className="provider-subtle">TTFB {deal.RetrTTFBMs}ms</div>}
                {deal.NoRecentSuccess && <div className="provider-warning">No recent success</div>}
            </div>
        );
    };

    const retrievableTotal = (provider?.Meta?.RetrievDeals || 0) + (provider?.Meta?.UnretrievDeals || 0);
    const retrievablePct = calculatePercentage(provider?.Meta?.RetrievDeals || 0, retrievableTotal);

    return (
        <div className="Provider">
            {provider && (
                <>
                    <h2>Provider {providerID}</h2>

                    <table className="compact-table provider-meta-table">
                        <tbody>
                        <tr>
                            <td>Ping OK</td>
                            <td>{provider.Meta.PingOk ? "Yes" : "No"}</td>
                        </tr>
                        <tr>
                            <td>Boost Deals</td>
                            <td>{provider.Meta.BoostDeals ? "Yes" : "No"}</td>
                        </tr>
                        <tr>
                            <td>Booster HTTP</td>
                            <td>{provider.Meta.BoosterHttp ? "Yes" : "No"}</td>
                        </tr>
                        <tr>
                            <td>Booster Bitswap</td>
                            <td>{provider.Meta.BoosterBitswap ? "Yes" : "No"}</td>
                        </tr>
                        <tr>
                            <td>Ask Price</td>
                            <td>{formatFil(provider.Meta.AskPrice)}</td>
                        </tr>
                        <tr>
                            <td>Ask Verified Price</td>
                            <td>{formatFil(provider.Meta.AskVerifiedPrice)}</td>
                        </tr>
                        <tr>
                            <td>Ask Min Piece Size</td>
                            <td>{formatBytesBinary(provider.Meta.AskMinPieceSize)}</td>
                        </tr>
                        <tr>
                            <td>Ask Max Piece Size</td>
                            <td>{formatBytesBinary(provider.Meta.AskMaxPieceSize)}</td>
                        </tr>
                        <tr>
                            <td>Indexed Success</td>
                            <td>{provider.Meta.IndexedSuccess}</td>
                        </tr>
                        <tr>
                            <td>Indexed Fail</td>
                            <td>{provider.Meta.IndexedFail}</td>
                        </tr>
                        <tr>
                            <td>Deal Started</td>
                            <td>{provider.Meta.DealStarted}</td>
                        </tr>
                        <tr>
                            <td>Deal Success</td>
                            <td>{provider.Meta.DealSuccess}</td>
                        </tr>
                        <tr>
                            <td>Deal Fail</td>
                            <td>{provider.Meta.DealFail}</td>
                        </tr>
                        <tr>
                            <td>Deal Rejected</td>
                            <td>{provider.Meta.DealRejected}</td>
                        </tr>
                        <tr>
                            <td>Most Recent Deal</td>
                            <td>{provider.Meta.MostRecentDealStart ? formatTimestamp(provider.Meta.MostRecentDealStart) : "Never"}</td>
                        </tr>
                        <tr>
                            <td>Retrievable Deals</td>
                            <td>{provider.Meta.RetrievDeals} / {retrievableTotal} ({retrievablePct}%)</td>
                        </tr>
                        <tr>
                            <td>Deal Cooldown</td>
                            <td>{provider.Meta.DealCooldownUntil ? formatTimestamp(provider.Meta.DealCooldownUntil) : "Ready"}</td>
                        </tr>
                        <tr>
                            <td>Cooldown Reason</td>
                            <td>{provider.Meta.DealCooldownReason || "-"}</td>
                        </tr>
                        <tr>
                            <td>Cooldown Attempts</td>
                            <td>{provider.Meta.DealCooldownAttempts || 0}</td>
                        </tr>
                        <tr>
                            <td>Retr Probe Success</td>
                            <td>{provider.Meta.RetrProbeSuccess}</td>
                        </tr>
                        <tr>
                            <td>Retr Probe Fail</td>
                            <td>{provider.Meta.RetrProbeFail}</td>
                        </tr>
                        <tr>
                            <td>Retr Probe Blocks</td>
                            <td>{provider.Meta.RetrProbeBlocks}</td>
                        </tr>
                        <tr>
                            <td>Retr Probe Bytes</td>
                            <td>{formatBytesBinary(provider.Meta.RetrProbeBytes)}</td>
                        </tr>
                        <tr>
                            <td>Links</td>
                            <td>
                                <a href={`https://filfox.info/en/address/${providerID}`} target="_blank" rel="noreferrer">FilFox</a>
                            </td>
                        </tr>

                        </tbody>
                    </table>

                    <h4>Recent Deals</h4>

                    <table className="compact-table providers-table">
                        <thead>
                        <tr>
                            <th>UUID</th>
                            <th>Group</th>
                            <th>Status</th>
                            <th>Timing</th>
                            <th>Transfer</th>
                            <th>Retrieval</th>
                            <th>Error</th>
                            <th>Deal ID</th>
                            <th>Publish CID</th>
                        </tr>
                        </thead>
                        <tbody>
                        {provider.RecentDeals && provider.RecentDeals.map((deal) => (
                            <tr key={deal.UUID} style={{ background: formatDealRowBackground(deal) }}>
                                <td><abbr title={deal.UUID}>{deal.UUID.substring(0, 8)}... </abbr></td>
                                <td><Link to={`/groups/${deal.GroupID}`}>{deal.GroupID}</Link></td>
                                <td>
                                    <div>{deal.Status || '-'}</div>
                                    {deal.SealStatus && <div className="provider-subtle">{deal.SealStatus}</div>}
                                    <div className="provider-badges">
                                        {deal.Verified && <span className="provider-badge">Verified</span>}
                                        {deal.KeepUnsealed && <span className="provider-badge provider-badge-secondary">Unsealed</span>}
                                    </div>
                                </td>
                                <td className="provider-deals-nowrap">
                                    <div>
                                        <div>Proposed: <b>{formatTimestamp(deal.StartTime)}</b></div>
                                        <div>Start: <b>{epochToDate(deal.StartEpoch)}</b>, {epochToDuration(deal.StartEpoch - headHeight)}</div>
                                        <div>End: <b>{epochToDate(deal.EndEpoch)}</b>, {epochToDuration(deal.EndEpoch - headHeight)}</div>
                                    </div>
                                </td>
                                <td>{renderTransferProgress(deal)}</td>
                                <td>{renderRetrievalStats(deal)}</td>
                                <td className="provider-deals-error-col">{deal.Error && <pre>{deal.Error}</pre>}</td>
                                <td>{deal.DealID ? <a href={`https://filfox.info/en/deal/${deal.DealID}`} target="_blank" rel="noopener noreferrer">{deal.DealID}</a> : null}</td>
                                <td>{deal.PubCid ? <a href={`https://filfox.info/en/message/${deal.PubCid}`} target="_blank" rel="noopener noreferrer">bafy..{deal.PubCid.substr(-16)}</a> : null}</td>
                            </tr>
                        ))}
                        </tbody>
                    </table>
                </>
            )}
        </div>
    );
}

export default Provider;
