import {
    AreaChart,
    Area,
    XAxis,
    YAxis,
    Tooltip,
    Legend,
    ResponsiveContainer,
    CartesianGrid,
    ReferenceLine,
} from "recharts";

function LatencyDistributionChart({ data }) {
    if (!data || !data.timestamps || data.timestamps.length === 0) {
        return <div className="Chart-empty">No latency data available</div>;
    }

    // Transform data for Recharts
    const chartData = data.timestamps.map((timestamp, index) => ({
        time: new Date(timestamp * 1000).toLocaleTimeString(),
        p50: data.p50[index] || 0,
        p95: data.p95[index] || 0,
        p99: data.p99[index] || 0,
    }));

    return (
        <div className="LatencyDistributionChart" style={{ width: "100%", height: 200 }}>
            <ResponsiveContainer>
                <AreaChart data={chartData} margin={{ top: 5, right: 20, left: 0, bottom: 5 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#e0e0e0" />
                    <XAxis
                        dataKey="time"
                        tick={{ fontSize: 10 }}
                        interval="preserveStartEnd"
                    />
                    <YAxis
                        tick={{ fontSize: 10 }}
                        unit="ms"
                    />
                    <Tooltip
                        contentStyle={{ fontSize: 12 }}
                        formatter={(value) => [`${value.toFixed(1)} ms`, ""]}
                    />
                    <Legend wrapperStyle={{ fontSize: 12 }} />
                    <ReferenceLine
                        y={350}
                        stroke="#f44336"
                        strokeDasharray="3 3"
                        label={{ value: "SLO", position: "insideTopRight", fontSize: 10 }}
                    />
                    <Area
                        type="monotone"
                        dataKey="p50"
                        stackId="1"
                        stroke="#4caf50"
                        fill="#4caf50"
                        fillOpacity={0.3}
                        name="p50 (median)"
                    />
                    <Area
                        type="monotone"
                        dataKey="p95"
                        stackId="1"
                        stroke="#ff9800"
                        fill="#ff9800"
                        fillOpacity={0.3}
                        name="p95"
                    />
                    <Area
                        type="monotone"
                        dataKey="p99"
                        stackId="1"
                        stroke="#f44336"
                        fill="#f44336"
                        fillOpacity={0.3}
                        name="p99"
                    />
                </AreaChart>
            </ResponsiveContainer>
        </div>
    );
}

export default LatencyDistributionChart;
