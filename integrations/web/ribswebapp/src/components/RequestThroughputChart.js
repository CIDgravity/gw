import {
    LineChart,
    Line,
    XAxis,
    YAxis,
    Tooltip,
    Legend,
    ResponsiveContainer,
    CartesianGrid,
} from "recharts";

function RequestThroughputChart({ data }) {
    if (!data || !data.timestamps || data.timestamps.length === 0) {
        return <div className="Chart-empty">No throughput data available</div>;
    }

    // Transform data for Recharts
    const chartData = data.timestamps.map((timestamp, index) => ({
        time: new Date(timestamp * 1000).toLocaleTimeString(),
        total: data.total[index] || 0,
        reads: data.reads[index] || 0,
        writes: data.writes[index] || 0,
    }));

    return (
        <div className="RequestThroughputChart" style={{ width: "100%", height: 200 }}>
            <ResponsiveContainer>
                <LineChart data={chartData} margin={{ top: 5, right: 20, left: 0, bottom: 5 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#e0e0e0" />
                    <XAxis
                        dataKey="time"
                        tick={{ fontSize: 10 }}
                        interval="preserveStartEnd"
                    />
                    <YAxis tick={{ fontSize: 10 }} />
                    <Tooltip
                        contentStyle={{ fontSize: 12 }}
                        formatter={(value) => [`${value.toFixed(1)} req/s`, ""]}
                    />
                    <Legend wrapperStyle={{ fontSize: 12 }} />
                    <Line
                        type="monotone"
                        dataKey="total"
                        stroke="#2196f3"
                        strokeWidth={2}
                        dot={false}
                        isAnimationActive={false}
                        name="Total"
                    />
                    <Line
                        type="monotone"
                        dataKey="reads"
                        stroke="#4caf50"
                        strokeWidth={2}
                        dot={false}
                        isAnimationActive={false}
                        name="Reads"
                    />
                    <Line
                        type="monotone"
                        dataKey="writes"
                        stroke="#ff9800"
                        strokeWidth={2}
                        dot={false}
                        isAnimationActive={false}
                        name="Writes"
                    />
                </LineChart>
            </ResponsiveContainer>
        </div>
    );
}

export default RequestThroughputChart;
