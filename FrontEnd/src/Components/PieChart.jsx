import { Pie } from 'react-chartjs-2';
import React from 'react';
import '../configs/ChartImports.js';
import './PieChart.css';
function PieChart({mappings, title = ""}){
    const labels = Array.from(mappings.keys());
    const values = Array.from(mappings.values());

    const buildColor = () => {
        return `#${Math.floor(Math.random()*16777215).toString(16)}`;

    }

    const colors = labels.map(() => buildColor());

    const pieChartData = {
        labels: labels,
        datasets: [
            {
                data: values,
                backgroundColor: colors,
                hoverBackgroundColor: colors
            }
        ]
    };

    return (
        <div className= "pie-chart-container" >
            <h2 className= " pie-chart-heading-container heading text-center">
                {title}
            </h2>
            <Pie data={pieChartData} />
        </div>
    );

}

export default PieChart;