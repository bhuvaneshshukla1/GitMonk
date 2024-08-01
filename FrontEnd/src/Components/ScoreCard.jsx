import 'bootstrap/dist/css/bootstrap.min.css';
import './ScoreCard.css';
import React from 'react';
function ScoreCard({data, title}) {
    return (
        <div className="score-card-container card shadow-sm p-3 mb-5 bg-white rounded text-center">
            <h2 className="score-card-heading">{title}</h2>
            <div className="score-card-value">{data}</div>
        </div>
    );
}

export default ScoreCard;