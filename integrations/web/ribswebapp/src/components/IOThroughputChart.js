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
import { formatBytesBinary } from "../helpers/fmt";

function IOThroughputChart({ data }) {
    if (!data || !data.timestamps || data.timestamps.length === 0) {
        return <div className="Chart-empty">No I/O data available</div>;
    }

    // Transform data for Recharts
    const chartData = data.timestamps.map((timestamp, index) => ({
        time: new Date(timestamp * 1000).toLocaleTimeString(),
        total: data.totalBytes[index] || 0,
        reads: data.readBytes[index] || 0,
        writes: data.writeBytes[index] || 0,
    }));

    const formatBytes = (value) => {
        if (value === 0) return "0 B/s";
        return `${formatBytesBinary(value)}/s`;
    };

    return (
        <div className="IOThroughputChart" style={{ width: "100%", height: 200 }}>
            <ResponsiveContainer>
                <LineChart data={chartData} margin={{ top: 5, right: 20, left: 10, bottom: 5 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#e0e0e0" />
                    <XAxis
                        dataKey="time"
                        tick={{ fontSize: 10 }}
                        interval="preserveStartEnd"
                    />
                    <YAxis 
                        tick={{ fontSize: 10 }} 
                        tickFormatter={formatBytes}
                        width={80}
                    />
                    <Tooltip
                        contentStyle={{ fontSize: 12 }}
                        formatter={(value) => [formatBytes(value), ""]}
                    />
                    <Legend wrapperStyle={{ fontSize: 12 }} />
                    <Line
                        type="monotone"
                        dataKey="total"
                        stroke="#9c27b0"
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

export default IOThroughputChart;
