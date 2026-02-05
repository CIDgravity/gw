import "./RecentEventsTimeline.css";

function RecentEventsTimeline({ events }) {
    if (!events || events.length === 0) {
        return <div className="RecentEventsTimeline-empty">No recent events</div>;
    }

    const getSeverityIcon = (severity) => {
        switch (severity) {
            case "error": return "🔴";
            case "warning": return "⚠️";
            default: return "ℹ️";
        }
    };

    const getEventTypeIcon = (type) => {
        switch (type) {
            case "health_change": return "🏥";
            case "latency_alert": return "⏱️";
            case "node_join": return "🟢";
            case "node_leave": return "🔴";
            case "multipart_complete": return "✅";
            default: return "📌";
        }
    };

    const formatTime = (timestamp) => {
        const date = new Date(timestamp * 1000);
        return date.toLocaleTimeString();
    };

    return (
        <div className="RecentEventsTimeline">
            {events.map((event, index) => (
                <div
                    key={index}
                    className={`RecentEventsTimeline-item RecentEventsTimeline-${event.severity || "info"}`}
                >
                    <div className="RecentEventsTimeline-time">
                        {formatTime(event.timestamp)}
                    </div>
                    <div className="RecentEventsTimeline-icon">
                        {getSeverityIcon(event.severity)}
                    </div>
                    <div className="RecentEventsTimeline-type">
                        {getEventTypeIcon(event.type)}
                    </div>
                    <div className="RecentEventsTimeline-content">
                        <span className="RecentEventsTimeline-node">{event.nodeId}</span>
                        <span className="RecentEventsTimeline-message">{event.message}</span>
                    </div>
                </div>
            ))}
        </div>
    );
}

export default RecentEventsTimeline;
