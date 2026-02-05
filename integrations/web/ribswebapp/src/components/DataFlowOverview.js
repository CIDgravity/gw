import "./DataFlowOverview.css";

function DataFlowOverview({ data }) {
    if (!data) {
        return <div className="DataFlowOverview-loading">Loading...</div>;
    }

    const { total = 0, reads = 0, writes = 0, multipart = 0 } = data;

    return (
        <div className="DataFlowOverview">
            <div className="DataFlowOverview-header">
                <span className="DataFlowOverview-label">Active Requests:</span>
                <span className="DataFlowOverview-total">{total}</span>
            </div>

            <div className="DataFlowOverview-breakdown">
                <div className="DataFlowOverview-item">
                    <span className="DataFlowOverview-icon">📖</span>
                    <span className="DataFlowOverview-type">Reads:</span>
                    <span className="DataFlowOverview-count">{reads}</span>
                    <span className="DataFlowOverview-hint">(YCQL routing)</span>
                </div>

                <div className="DataFlowOverview-item">
                    <span className="DataFlowOverview-icon">✏️</span>
                    <span className="DataFlowOverview-type">Writes:</span>
                    <span className="DataFlowOverview-count">{writes}</span>
                    <span className="DataFlowOverview-hint">(Round-robin)</span>
                </div>

                <div className="DataFlowOverview-item">
                    <span className="DataFlowOverview-icon">📦</span>
                    <span className="DataFlowOverview-type">Multipart:</span>
                    <span className="DataFlowOverview-count">{multipart}</span>
                    <span className="DataFlowOverview-hint">(Coordinating)</span>
                </div>
            </div>

            <div className="DataFlowOverview-efficiency">
                <span>Routing Efficiency: </span>
                <span className="DataFlowOverview-efficiency-value">100%</span>
            </div>
        </div>
    );
}

export default DataFlowOverview;
