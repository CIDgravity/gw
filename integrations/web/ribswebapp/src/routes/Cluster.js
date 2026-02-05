import { useEffect, useState } from "react";
import RibsRPC from "../helpers/rpc";
import ClusterTopology from "../components/ClusterTopology";
import RequestThroughputChart from "../components/RequestThroughputChart";
import IOThroughputChart from "../components/IOThroughputChart";
import LatencyDistributionChart from "../components/LatencyDistributionChart";
import ErrorRateChart from "../components/ErrorRateChart";
import NodeStatistics from "../components/NodeStatistics";
import DataFlowOverview from "../components/DataFlowOverview";
import RecentEventsTimeline from "../components/RecentEventsTimeline";
import "./Cluster.css";

function Cluster() {
    const [topology, setTopology] = useState(null);
    const [throughput, setThroughput] = useState(null);
    const [ioThroughput, setIOThroughput] = useState(null);
    const [latency, setLatency] = useState(null);
    const [errorRates, setErrorRates] = useState(null);
    const [activeRequests, setActiveRequests] = useState(null);
    const [events, setEvents] = useState([]);
    const [autoRefresh, setAutoRefresh] = useState(true);

    const fetchData = async () => {
        try {
            // Fetch all cluster data
            const [topo, tp, io, lat, err, active, evts] = await Promise.all([
                RibsRPC.call("ClusterTopology", []),
                RibsRPC.call("RequestThroughput", ["5m"]),
                RibsRPC.call("IOThroughput", ["5m"]),
                RibsRPC.call("LatencyDistribution", ["5m"]),
                RibsRPC.call("ErrorRates", []),
                RibsRPC.call("ActiveRequests", []),
                RibsRPC.call("ClusterEvents", [10]),
            ]);

            setTopology(topo);
            setThroughput(tp);
            setIOThroughput(io);
            setLatency(lat);
            setErrorRates(err);
            setActiveRequests(active);
            setEvents(evts || []);
        } catch (error) {
            console.error("Failed to fetch cluster data:", error);
        }
    };

    useEffect(() => {
        fetchData();

        if (!autoRefresh) return;

        // Different update frequencies for different data
        const topologyInterval = setInterval(() => {
            RibsRPC.call("ClusterTopology", []).then(setTopology).catch(console.error);
        }, 5000);

        const metricsInterval = setInterval(() => {
            Promise.all([
                RibsRPC.call("RequestThroughput", ["5m"]),
                RibsRPC.call("IOThroughput", ["5m"]),
                RibsRPC.call("LatencyDistribution", ["5m"]),
                RibsRPC.call("ErrorRates", []),
            ]).then(([tp, io, lat, err]) => {
                setThroughput(tp);
                setIOThroughput(io);
                setLatency(lat);
                setErrorRates(err);
            }).catch(console.error);
        }, 1000);

        const activeInterval = setInterval(() => {
            RibsRPC.call("ActiveRequests", []).then(setActiveRequests).catch(console.error);
        }, 100);

        const eventsInterval = setInterval(() => {
            RibsRPC.call("ClusterEvents", [10]).then(setEvents).catch(console.error);
        }, 5000);

        return () => {
            clearInterval(topologyInterval);
            clearInterval(metricsInterval);
            clearInterval(activeInterval);
            clearInterval(eventsInterval);
        };
    }, [autoRefresh]);

    return (
        <div className="Cluster">
            <div className="Cluster-header">
                <h2>Cluster Monitoring</h2>
                <label className="Cluster-refresh-toggle">
                    <input
                        type="checkbox"
                        checked={autoRefresh}
                        onChange={(e) => setAutoRefresh(e.target.checked)}
                    />
                    Auto-refresh
                </label>
            </div>

            {/* Top section: Topology and Active Requests side by side */}
            <div className="Cluster-top-row">
                <div className="Cluster-topology-section">
                    <h3>Cluster Topology</h3>
                    <ClusterTopology topology={topology} />
                </div>
                <div className="Cluster-active-section">
                    <DataFlowOverview data={activeRequests} />
                    <div className="Cluster-node-tables">
                        <NodeStatistics topology={topology} />
                    </div>
                </div>
            </div>

            {/* Charts in a 2x2 grid */}
            <div className="Cluster-charts-grid">
                <div className="Cluster-chart">
                    <h4>Request Throughput (req/s)</h4>
                    <RequestThroughputChart data={throughput} />
                </div>
                <div className="Cluster-chart">
                    <h4>I/O Throughput (bytes/s)</h4>
                    <IOThroughputChart data={ioThroughput} />
                </div>
                <div className="Cluster-chart">
                    <h4>Latency Distribution (ms)</h4>
                    <LatencyDistributionChart data={latency} />
                </div>
                <div className="Cluster-chart">
                    <h4>Error Rate by Node (%)</h4>
                    <ErrorRateChart data={errorRates} />
                </div>
            </div>

            {/* Events at the bottom */}
            <div className="Cluster-events">
                <h3>Recent Events</h3>
                <RecentEventsTimeline events={events} />
            </div>
        </div>
    );
}

export default Cluster;
