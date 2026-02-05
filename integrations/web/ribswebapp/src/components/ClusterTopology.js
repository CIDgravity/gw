import "./ClusterTopology.css";

function ClusterTopology({ topology }) {
    if (!topology) {
        return <div className="ClusterTopology-loading">Loading topology...</div>;
    }

    const { proxies = [], storageNodes = [], dataFlows = [] } = topology;

    const getStatusColor = (status) => {
        switch (status) {
            case "healthy": return "#4caf50";
            case "degraded": return "#ff9800";
            case "unhealthy": return "#f44336";
            default: return "#9e9e9e";
        }
    };

    // Calculate positions
    const svgWidth = 800;
    const svgHeight = 380;
    
    // Load balancer position
    const lbY = 40;
    
    // Proxy layer position
    const proxyY = 130;
    const proxySpacing = Math.min(180, (svgWidth - 100) / Math.max(proxies.length, 1));
    const proxyStartX = (svgWidth - (proxies.length - 1) * proxySpacing) / 2;
    
    // Storage layer position
    const storageY = 280;
    const storageSpacing = Math.min(160, (svgWidth - 100) / Math.max(storageNodes.length, 1));
    const storageStartX = (svgWidth - (storageNodes.length - 1) * storageSpacing) / 2;

    return (
        <div className="ClusterTopology">
            <svg viewBox={`0 0 ${svgWidth} ${svgHeight}`} className="ClusterTopology-svg">
                {/* Background gradient definitions */}
                <defs>
                    <linearGradient id="proxyGradient" x1="0%" y1="0%" x2="0%" y2="100%">
                        <stop offset="0%" stopColor="#42a5f5" />
                        <stop offset="100%" stopColor="#1976d2" />
                    </linearGradient>
                    <linearGradient id="storageGradient" x1="0%" y1="0%" x2="0%" y2="100%">
                        <stop offset="0%" stopColor="#66bb6a" />
                        <stop offset="100%" stopColor="#388e3c" />
                    </linearGradient>
                    <linearGradient id="lbGradient" x1="0%" y1="0%" x2="0%" y2="100%">
                        <stop offset="0%" stopColor="#7e57c2" />
                        <stop offset="100%" stopColor="#512da8" />
                    </linearGradient>
                    <filter id="shadow" x="-20%" y="-20%" width="140%" height="140%">
                        <feDropShadow dx="2" dy="2" stdDeviation="2" floodOpacity="0.3"/>
                    </filter>
                </defs>

                {/* External Load Balancer */}
                <g transform={`translate(${svgWidth/2}, ${lbY})`}>
                    <rect x="-70" y="-18" width="140" height="36" rx="6" fill="url(#lbGradient)" filter="url(#shadow)" />
                    <text x="0" y="5" textAnchor="middle" fill="white" fontSize="12" fontWeight="bold">
                        🌐 Load Balancer
                    </text>
                </g>

                {/* Connection lines from LB to proxies */}
                {proxies.map((proxy, index) => {
                    const xPos = proxyStartX + (index * proxySpacing);
                    return (
                        <line
                            key={`lb-proxy-${proxy.id}`}
                            x1={svgWidth/2}
                            y1={lbY + 18}
                            x2={xPos}
                            y2={proxyY - 25}
                            stroke="#9e9e9e"
                            strokeWidth="2"
                            strokeDasharray="6,4"
                            opacity="0.6"
                        />
                    );
                })}

                {/* S3 Frontend Proxies */}
                {proxies.map((proxy, index) => {
                    const xPos = proxyStartX + (index * proxySpacing);
                    const statusColor = getStatusColor(proxy.status);
                    return (
                        <g key={proxy.id} transform={`translate(${xPos}, ${proxyY})`}>
                            {/* Status indicator ring */}
                            <circle cx="0" cy="0" r="38" fill="none" stroke={statusColor} strokeWidth="3" opacity="0.5" />
                            
                            {/* Main node box */}
                            <rect x="-55" y="-25" width="110" height="50" rx="8" fill="url(#proxyGradient)" filter="url(#shadow)" />
                            
                            {/* Icon and label */}
                            <text x="0" y="-7" textAnchor="middle" fill="white" fontSize="16">
                                🔀
                            </text>
                            <text x="0" y="8" textAnchor="middle" fill="white" fontSize="11" fontWeight="bold">
                                {proxy.id}
                            </text>
                            <text x="0" y="20" textAnchor="middle" fill="rgba(255,255,255,0.8)" fontSize="9">
                                S3 Frontend
                            </text>
                            
                            {/* Status dot */}
                            <circle cx="45" cy="-15" r="6" fill={statusColor} />
                        </g>
                    );
                })}

                {/* Connection lines from proxies to storage - mesh pattern */}
                {proxies.map((proxy, pIndex) => {
                    const proxyX = proxyStartX + (pIndex * proxySpacing);
                    return storageNodes.map((node, nIndex) => {
                        const nodeX = storageStartX + (nIndex * storageSpacing);
                        const flow = dataFlows.find(f => f.from === proxy.id && f.to === node.id);
                        const strokeWidth = flow ? Math.max(1.5, Math.min(flow.rate / 5, 4)) : 1;
                        const opacity = flow ? 0.7 : 0.25;
                        return (
                            <line
                                key={`proxy-storage-${proxy.id}-${node.id}`}
                                x1={proxyX}
                                y1={proxyY + 25}
                                x2={nodeX}
                                y2={storageY - 30}
                                stroke={flow ? "#4caf50" : "#bdbdbd"}
                                strokeWidth={strokeWidth}
                                opacity={opacity}
                            />
                        );
                    });
                })}

                {/* Kuri Storage Nodes */}
                {storageNodes.map((node, index) => {
                    const xPos = storageStartX + (index * storageSpacing);
                    const statusColor = getStatusColor(node.status);
                    const storagePercent = node.storageTotal > 0
                        ? (node.storageUsed / node.storageTotal * 100).toFixed(0)
                        : 0;
                    return (
                        <g key={node.id} transform={`translate(${xPos}, ${storageY})`}>
                            {/* Status indicator ring */}
                            <circle cx="0" cy="0" r="45" fill="none" stroke={statusColor} strokeWidth="3" opacity="0.5" />
                            
                            {/* Main node box */}
                            <rect x="-60" y="-30" width="120" height="60" rx="8" fill="url(#storageGradient)" filter="url(#shadow)" />
                            
                            {/* Icon and labels */}
                            <text x="0" y="-12" textAnchor="middle" fill="white" fontSize="16">
                                💾
                            </text>
                            <text x="0" y="3" textAnchor="middle" fill="white" fontSize="11" fontWeight="bold">
                                {node.id}
                            </text>
                            <text x="0" y="15" textAnchor="middle" fill="rgba(255,255,255,0.8)" fontSize="9">
                                Kuri Storage
                            </text>
                            <text x="0" y="26" textAnchor="middle" fill="rgba(255,255,255,0.7)" fontSize="8">
                                {storagePercent}% • {(node.requestsPerSecond || 0).toFixed(1)} req/s
                            </text>
                            
                            {/* Status dot */}
                            <circle cx="50" cy="-20" r="6" fill={statusColor} />
                        </g>
                    );
                })}

                {/* Layer labels */}
                <text x="15" y={proxyY} fill="#666" fontSize="10" fontWeight="500" opacity="0.7">
                    FRONTEND
                </text>
                <text x="15" y={storageY} fill="#666" fontSize="10" fontWeight="500" opacity="0.7">
                    STORAGE
                </text>
            </svg>

            <div className="ClusterTopology-legend">
                <div className="ClusterTopology-legend-item">
                    <span style={{color: "#4caf50"}}>●</span> Healthy
                </div>
                <div className="ClusterTopology-legend-item">
                    <span style={{color: "#ff9800"}}>●</span> Degraded
                </div>
                <div className="ClusterTopology-legend-item">
                    <span style={{color: "#f44336"}}>●</span> Unhealthy
                </div>
                <div className="ClusterTopology-legend-item">
                    <span style={{color: "#9e9e9e"}}>●</span> Unknown
                </div>
            </div>

            {proxies.length === 0 && storageNodes.length === 0 && (
                <div className="ClusterTopology-empty">
                    No cluster nodes configured. Set up FGW_BACKEND_NODES to see cluster topology.
                </div>
            )}
        </div>
    );
}

export default ClusterTopology;
