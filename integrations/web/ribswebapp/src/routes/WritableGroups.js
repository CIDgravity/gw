import React, { useState, useEffect } from "react";
import RibsRPC from "../helpers/rpc";
import { formatBytesBinary, formatNum } from "../helpers/fmt";
import "./Groups.css";

function WritableGroups() {
    const [writableGroups, setWritableGroups] = useState([]);
    const [parallelStats, setParallelStats] = useState({Enabled: false});

    const groups = Array.isArray(writableGroups) ? writableGroups : [];

    const fetchData = async () => {
        try {
            const groups = await RibsRPC.call("WritableGroups");
            const stats = await RibsRPC.call("ParallelWriteStats");
            setWritableGroups(Array.isArray(groups) ? groups : []);
            setParallelStats(stats);
        } catch (error) {
            console.error("Error fetching writable groups:", error);
        }
    };

    useEffect(() => {
        fetchData();
        const intervalId = setInterval(fetchData, 500);
        return () => clearInterval(intervalId);
    }, []);

    const renderProgressBar = (bytes, maxBytes) => {
        const percentage = (bytes / maxBytes) * 100;
        return (
            <div className="progress-bar">
                <div
                    className="progress-bar__fill"
                    style={{ width: `${percentage}%` }}
                ></div>
            </div>
        );
    };

    return (
        <div className="Groups">
            <h2>Writable Groups</h2>
            
            {!parallelStats.Enabled && (
                <div style={{background: '#FFE5E5', padding: '10px', marginBottom: '20px', borderRadius: '4px'}}>
                    <strong>Parallel Writes Disabled</strong>
                    <p>Enable parallel writes with RIBS_ENABLE_PARALLEL_WRITES=true to see real-time writer metrics.</p>
                </div>
            )}

            {groups.length === 0 ? (
                <p>No writable groups available. Groups become writable when they have space for new data.</p>
            ) : (
                <>
                    <div style={{marginBottom: '20px'}}>
                        <strong>Total Writable Groups: {groups.length}</strong>
                        {parallelStats.Enabled && (
                            <span style={{marginLeft: '20px'}}>
                                Total Active Writers: {groups.reduce((sum, g) => sum + (g.ActiveWriters || 0), 0)}
                            </span>
                        )}
                    </div>

                    {groups.map((group) => (
                        <div key={group.GroupKey} className="group" style={{
                            borderLeft: group.HasAffinity ? '4px solid #4CAF50' : '4px solid #ccc'
                        }}>
                            <div className="group-info">
                                <h3>
                                    Group {group.GroupKey}
                                    <span className="group-state" style={{background: '#E5F5E5'}}>Writable</span>
                                    {group.HasAffinity && (
                                        <span style={{
                                            marginLeft: '10px',
                                            fontSize: '0.8em',
                                            color: '#4CAF50',
                                            fontWeight: 'bold'
                                        }}>
                                            ★ Affinity
                                        </span>
                                    )}
                                </h3>
                                
                                <div style={{display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '10px'}}>
                                    <div>
                                        <p>Blocks: {formatNum(group.Blocks)}</p>
                                        <p>Bytes: {formatBytesBinary(group.Bytes)}</p>
                                        {renderProgressBar(group.Bytes, 29500 * 1024 * 1024)}
                                    </div>
                                    
                                    {parallelStats.Enabled && (
                                        <div style={{background: '#F5F5F5', padding: '10px', borderRadius: '4px'}}>
                                            <p><strong>Active Writers:</strong> {group.ActiveWriters || 0}</p>
                                            <p><strong>Available Space:</strong> {formatBytesBinary(group.AvailableSpace || 0)}</p>
                                        </div>
                                    )}
                                </div>
                            </div>
                        </div>
                    ))}
                </>
            )}
        </div>
    );
}

export default WritableGroups;
