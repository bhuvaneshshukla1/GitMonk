import {useEffect, useState} from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import 'bootstrap/dist/css/bootstrap.min.css';
import PieChart from "./Components/PieChart.jsx";
import {execute} from "./Components/GitRestClient.js";
import Filters from "./Components/Filters.jsx";
import ScoreCard from "./Components/ScoreCard.jsx";
import {formatTime} from "./utils.js";
import Header from "./Components/Header.jsx";

function App() {
    const location = useLocation();
    const navigate = useNavigate();
    const username = location.state?.username;
    const [filterData, setFilterData] = useState(null);
    const [gitResponse, setGitResponse] = useState(null);
    const [error, setError] = useState(null);


    const buildRequestPayload = (repository, project, fromDate,author) => {
        const request =  {
            repository: repository,
            project: project,
            from_date: fromDate
        };
        if(author && username){
            request.author= username ;
        }
        return request;
    }

    useEffect(() => {
        if (filterData) {
            const getGitResponse = async () => {
                try {
                    const request = buildRequestPayload(filterData.repository, filterData.project, filterData.fromDate,filterData.author);
                    const response = await execute(request);
                    setGitResponse(response);
                    setError(null);
                } catch (error) {
                    setError(error.message);
                }
            };
            getGitResponse()
        }
    }, [filterData]);

    const handleFilterSubmit = (data) => {
        setFilterData(data);
    };

    return (
        <div>
            <Header/>


            <div className="container">
                <h1 className="text-center my-4">Welcome to GitMonk</h1>
                <Filters onSubmit={handleFilterSubmit}/>
                {error && <div className="alert alert-danger"> Couldn't load statistics. Please try again later</div>}
                {!gitResponse && !error && <div> Loading ... </div>}
                {
                    gitResponse && (
                        <>
                            {gitResponse.pull_request_count >0 && <div className="row mb-4 mt-4">
                                <div className="col-md-6">
                                    <PieChart mappings={new Map(Object.entries(gitResponse.pull_request_status))}
                                              title="Pull Request Status"/>
                                </div>
                                <div className="col-md-6">
                                    <PieChart mappings={new Map(Object.entries(gitResponse.pull_request_merge_status))}
                                              title="Pull Request Merge Status"/>
                                </div>
                            </div>}
                            <div className="row mb-4 mt-4">
                                <div className="col-md-3">
                                    <ScoreCard title="Total Comments" data={gitResponse.total_comments}/>
                                </div>
                                <div className="col-md-3">
                                    <ScoreCard title="Pull Request Count" data={gitResponse.pull_request_count}/>
                                </div>
                                <div className="col-md-3">
                                    <ScoreCard title="Average time to merge pull-requests"
                                               data={gitResponse.avg_pull_request_closure_time !== null ? formatTime(gitResponse.avg_pull_request_closure_time) : "N/A"}/>
                                </div>
                                <div className="col-md-3">
                                    <ScoreCard title="Average turnaround time per comment"
                                               data={formatTime(gitResponse.avg_comment_turnaround_time)}/>
                                </div>
                            </div>


                        </>
                    )
                }
            </div>
        </div>
    );
}


export default App;
