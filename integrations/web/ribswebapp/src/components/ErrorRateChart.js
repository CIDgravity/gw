import {
    BarChart,
    Bar,
    XAxis,
    YAxis,
    Tooltip,
    ResponsiveContainer,
    CartesianGrid,
    Cell,
} from "recharts";

function ErrorRateChart({ data }) {
    if (!data || !data.nodes || Object.keys(data.nodes).length === 0) {
        return <div className="Chart-empty">No error rate data available</div>;
    }

    // Transform data for Recharts
    const chartData = Object.entries(data.nodes).map(([nodeId, stats]) => ({
        node: nodeId,
        errorRate: stats.errorRate || 0,
        trend: stats.trend || "stable",
    }));

    const getBarColor = (errorRate) => {
        if (errorRate < 1) return "#4caf50"; // Green
        if (errorRate < 5) return "#ff9800"; // Orange
        return "#f44336"; // Red
    };

    return (
        <div className="ErrorRateChart" style={{ width: "100%", height: 200 }}>
            <ResponsiveContainer>
                <BarChart
                    data={chartData}
                    layout="vertical"
                    margin={{ top: 5, right: 20, left: 60, bottom: 5 }}
                >
                    <CartesianGrid strokeDasharray="3 3" stroke="#e0e0e0" />
                    <XAxis
                        type="number"
                        tick={{ fontSize: 10 }}
                        unit="%"
                    />
                    <YAxis
                        type="category"
                        dataKey="node"
                        tick={{ fontSize: 10 }}
                        width={50}
                    />
                    <Tooltip
                        contentStyle={{ fontSize: 12 }}
                        formatter={(value, name, props) => {
                            const trend = props.payload.trend;
                            return [`${value.toFixed(2)}% (trend: ${trend})`, "Error Rate"];
                        }}
                    />
                    <Bar dataKey="errorRate" name="Error Rate %">
                        {chartData.map((entry, index) => (
                            <Cell key={`cell-${index}`} fill={getBarColor(entry.errorRate)} />
                        ))}
                    </Bar>
                </BarChart>
            </ResponsiveContainer>
        </div>
    );
}

export default ErrorRateChart;
