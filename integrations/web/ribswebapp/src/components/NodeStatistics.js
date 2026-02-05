import { formatBytesBinary } from "../helpers/fmt";
import "./NodeStatistics.css";

function NodeStatistics({ topology }) {
    if (!topology) {
        return <div className="NodeStatistics-loading">Loading...</div>;
    }

    const { proxies = [], storageNodes = [] } = topology;

    const getStatusIcon = (status) => {
        switch (status) {
            case "healthy": return "🟢";
            case "degraded": return "🟡";
            case "unhealthy": return "🔴";
            default: return "⚪";
        }
    };

    return (
        <div className="NodeStatistics">
            <div className="NodeStatistics-section">
                <h4>Frontend Proxies</h4>
                <table className="NodeStatistics-table">
                    <thead>
                        <tr>
                            <th>Node</th>
                            <th>Address</th>
                            <th>Req/min</th>
                            <th>Connections</th>
                            <th>Avg Latency</th>
                            <th>Status</th>
                        </tr>
                    </thead>
                    <tbody>
                        {proxies.length === 0 ? (
                            <tr>
                                <td colSpan="6" className="NodeStatistics-empty">No proxies configured</td>
                            </tr>
                        ) : (
                            proxies.map((proxy) => (
                                <tr key={proxy.id}>
                                    <td>{proxy.id}</td>
                                    <td>{proxy.address}</td>
                                    <td>{((proxy.requestsPerSecond || 0) * 60).toFixed(0)}</td>
                                    <td>{proxy.activeConnections || 0}</td>
                                    <td>{(proxy.latencyMs || 0).toFixed(1)} ms</td>
                                    <td>{getStatusIcon(proxy.status)}</td>
                                </tr>
                            ))
                        )}
                    </tbody>
                </table>
            </div>

            <div className="NodeStatistics-section">
                <h4>Storage Nodes</h4>
                <table className="NodeStatistics-table">
                    <thead>
                        <tr>
                            <th>Node</th>
                            <th>Address</th>
                            <th>Storage</th>
                            <th>Objects</th>
                            <th>Req/min</th>
                            <th>Status</th>
                        </tr>
                    </thead>
                    <tbody>
                        {storageNodes.length === 0 ? (
                            <tr>
                                <td colSpan="6" className="NodeStatistics-empty">No storage nodes configured</td>
                            </tr>
                        ) : (
                            storageNodes.map((node) => (
                                <tr key={node.id}>
                                    <td>{node.id}</td>
                                    <td>{node.address}</td>
                                    <td>
                                        {formatBytesBinary(node.storageUsed || 0)} / {formatBytesBinary(node.storageTotal || 0)}
                                    </td>
                                    <td>{(node.objectsStored || 0).toLocaleString()}</td>
                                    <td>{((node.requestsPerSecond || 0) * 60).toFixed(0)}</td>
                                    <td>{getStatusIcon(node.status)}</td>
                                </tr>
                            ))
                        )}
                    </tbody>
                </table>
            </div>
        </div>
    );
}

export default NodeStatistics;
